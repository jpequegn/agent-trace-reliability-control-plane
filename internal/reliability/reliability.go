package reliability

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

type Metrics struct {
	TruePositive            int     `json:"true_positive"`
	FalsePositive           int     `json:"false_positive"`
	FalseNegative           int     `json:"false_negative"`
	Precision               float64 `json:"precision"`
	Recall                  float64 `json:"recall"`
	InstrumentationCoverage float64 `json:"instrumentation_coverage"`
}

func CalculateImpact(spans []domain.Span, signals []domain.Signal, asOf time.Time) (domain.Impact, error) {
	active := []domain.Signal{}
	for _, signal := range signals {
		if signal.State == domain.SignalDetected && !signal.LastSeen.After(asOf) {
			active = append(active, signal)
		}
	}
	if len(active) == 0 {
		return domain.Impact{}, errors.New("no classified signals available at evaluation time")
	}
	tenant, project := active[0].TenantID, active[0].ProjectID
	allRuns := map[string]bool{}
	allUsers := map[string]bool{}
	userByRun := map[string]string{}
	cohortByRun := map[string]string{}
	cohortRuns := map[string]map[string]bool{}
	for _, span := range spans {
		if span.TenantID != tenant || span.ProjectID != project || span.StartedAt.After(asOf) {
			continue
		}
		allRuns[span.RunID] = true
		allUsers[span.UserID] = true
		userByRun[span.RunID] = span.UserID
		cohortByRun[span.RunID] = span.CohortID
		if cohortRuns[span.CohortID] == nil {
			cohortRuns[span.CohortID] = map[string]bool{}
		}
		cohortRuns[span.CohortID][span.RunID] = true
	}
	affectedRuns := map[string]bool{}
	affectedUsers := map[string]bool{}
	affectedByCohort := map[string]map[string]bool{}
	first, last := active[0].FirstSeen, active[0].LastSeen
	severityTotal := 0
	for _, signal := range active {
		if signal.TenantID != tenant || signal.ProjectID != project {
			return domain.Impact{}, errors.New("signals cross tenant or project boundary")
		}
		affectedRuns[signal.RunID] = true
		affectedUsers[userByRun[signal.RunID]] = true
		cohort := cohortByRun[signal.RunID]
		if affectedByCohort[cohort] == nil {
			affectedByCohort[cohort] = map[string]bool{}
		}
		affectedByCohort[cohort][signal.RunID] = true
		if signal.FirstSeen.Before(first) {
			first = signal.FirstSeen
		}
		if signal.LastSeen.After(last) {
			last = signal.LastSeen
		}
		severityTotal += signal.Severity
	}
	byCohort := map[string]float64{}
	for cohort, runs := range cohortRuns {
		byCohort[cohort] = ratio(len(affectedByCohort[cohort]), len(runs))
	}
	runRate := ratio(len(affectedRuns), len(allRuns))
	averageSeverity := float64(severityTotal) / float64(len(active))
	return domain.Impact{FirstSeen: first, LastSeen: last, Recurrences: len(affectedRuns), AffectedRuns: len(affectedRuns), TotalRuns: len(allRuns), AffectedUsers: nonEmptyCount(affectedUsers), TotalUsers: nonEmptyCount(allUsers), AffectedRunRate: runRate, AffectedUserRate: ratio(nonEmptyCount(affectedUsers), nonEmptyCount(allUsers)), ByCohort: byCohort, SeverityWeighted: runRate * averageSeverity}, nil
}

func ProposeIssue(classifier domain.Classifier, signals []domain.Signal, impact domain.Impact, owner string, at time.Time) (domain.ReliabilityIssue, error) {
	if owner == "" || at.IsZero() || len(signals) == 0 {
		return domain.ReliabilityIssue{}, errors.New("owner, timestamp, and signals are required")
	}
	ids := make([]string, 0, len(signals))
	severity := 0
	for _, signal := range signals {
		if err := detect.ValidateClassifierSignal(signal); err != nil {
			return domain.ReliabilityIssue{}, err
		}
		if signal.ReasonCode != classifier.ReasonCode || signal.ClassifierVersion != classifier.Version {
			return domain.ReliabilityIssue{}, errors.New("signal does not match classifier")
		}
		ids = append(ids, signal.ID)
		if signal.Severity > severity {
			severity = signal.Severity
		}
	}
	sort.Strings(ids)
	issue := domain.ReliabilityIssue{ClassifierID: classifier.ID, ClassifierVersion: classifier.Version, State: domain.IssueProposed, Owner: owner, Severity: severity, Hypothesis: classifier.Description, Impact: impact, Signals: ids, UpdatedAt: at}
	issue.ID, _ = domain.StableID("issue", struct {
		Classifier string
		Signals    []string
	}{classifier.ID, ids})
	return issue, nil
}

func Transition(issue domain.ReliabilityIssue, target domain.IssueState, actor, reason string, at time.Time) (domain.ReliabilityIssue, error) {
	if actor == "" || reason == "" || at.IsZero() || at.Before(issue.UpdatedAt) {
		return issue, errors.New("transition requires actor, reason, and non-regressing timestamp")
	}
	allowed := map[domain.IssueState]map[domain.IssueState]bool{
		domain.IssueProposed:  {domain.IssueConfirmed: true, domain.IssueRejected: true, domain.IssueSuppressed: true},
		domain.IssueConfirmed: {domain.IssueMitigated: true, domain.IssueRejected: true, domain.IssueSuppressed: true},
		domain.IssueMitigated: {domain.IssueResolved: true, domain.IssueConfirmed: true},
		domain.IssueResolved:  {domain.IssueConfirmed: true},
	}
	if !allowed[issue.State][target] {
		return issue, fmt.Errorf("invalid issue transition %s -> %s", issue.State, target)
	}
	if target == domain.IssueConfirmed && (issue.ClassifierID == "" || issue.ClassifierVersion != detect.ClassifierVersion || len(issue.Signals) == 0) {
		return issue, errors.New("confirmation requires current classifier and signals")
	}
	issue.State = target
	issue.ReviewActor = actor
	issue.ReviewReason = reason
	issue.UpdatedAt = at
	if target == domain.IssueRejected {
		issue.KnownFalsePositives = append(issue.KnownFalsePositives, reason)
	}
	return issue, nil
}

func Evaluate(signals []domain.Signal, truth map[string]string) Metrics {
	predicted := map[string]map[string]bool{}
	driftRuns := map[string]bool{}
	for _, signal := range signals {
		if signal.State == domain.SignalInstrumentationDrift {
			driftRuns[signal.RunID] = true
			continue
		}
		if signal.State != domain.SignalDetected {
			continue
		}
		if predicted[signal.RunID] == nil {
			predicted[signal.RunID] = map[string]bool{}
		}
		predicted[signal.RunID][signal.ReasonCode] = true
	}
	metrics := Metrics{}
	for runID, expected := range truth {
		if expected == detect.ReasonDrift {
			continue
		}
		if expected == "none" {
			metrics.FalsePositive += len(predicted[runID])
			continue
		}
		if predicted[runID][expected] {
			metrics.TruePositive++
		} else {
			metrics.FalseNegative++
		}
		for reason := range predicted[runID] {
			if reason != expected {
				metrics.FalsePositive++
			}
		}
	}
	metrics.Precision = ratio(metrics.TruePositive, metrics.TruePositive+metrics.FalsePositive)
	metrics.Recall = ratio(metrics.TruePositive, metrics.TruePositive+metrics.FalseNegative)
	metrics.InstrumentationCoverage = ratio(len(truth)-len(driftRuns), len(truth))
	return metrics
}

type Report struct {
	Signals           int `json:"signals"`
	Clusters          int `json:"clusters"`
	ClassifierMatches int `json:"classifier_matches"`
	ReviewedIssues    int `json:"reviewed_issues"`
	ConfirmedIssues   int `json:"confirmed_issues"`
}

func Summarize(signals []domain.Signal, issues []domain.ReliabilityIssue) Report {
	clusters := map[string]bool{}
	matches := 0
	for _, signal := range signals {
		clusters[signal.ReasonCode+"/"+signal.CohortID] = true
		if detect.ValidateClassifierSignal(signal) == nil {
			matches++
		}
	}
	report := Report{Signals: len(signals), Clusters: len(clusters), ClassifierMatches: matches, ReviewedIssues: len(issues)}
	for _, issue := range issues {
		if issue.State == domain.IssueConfirmed || issue.State == domain.IssueMitigated || issue.State == domain.IssueResolved {
			report.ConfirmedIssues++
		}
	}
	return report
}

func Markdown(report Report) string {
	var output strings.Builder
	output.WriteString("# Reliability control-plane status\n\n")
	fmt.Fprintf(&output, "- Deterministic signals: %d\n", report.Signals)
	fmt.Fprintf(&output, "- Signal clusters: %d\n", report.Clusters)
	fmt.Fprintf(&output, "- Current classifier matches: %d\n", report.ClassifierMatches)
	fmt.Fprintf(&output, "- Human-reviewed issues: %d\n", report.ReviewedIssues)
	fmt.Fprintf(&output, "- Confirmed issues: %d\n", report.ConfirmedIssues)
	return output.String()
}

func ratio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func nonEmptyCount(values map[string]bool) int {
	count := 0
	for value := range values {
		if value != "" {
			count++
		}
	}
	return count
}
