package reliability

import (
	"strings"
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

func signalsFor(reason string, signals []domain.Signal) []domain.Signal {
	result := []domain.Signal{}
	for _, signal := range signals {
		if signal.ReasonCode == reason {
			result = append(result, signal)
		}
	}
	return result
}

func truthMap(data corpus.Corpus) map[string]string {
	result := map[string]string{}
	for runID, truth := range data.Truth {
		result[runID] = truth.ExpectedReason
	}
	return result
}

func TestImpactMatchesReviewedCorpusAndUsesNoFutureData(t *testing.T) {
	data := corpus.Generate()
	engine := detect.New(detect.DefaultConfig())
	signals := signalsFor(detect.ReasonRetry, engine.Detect(data.Spans))
	asOf := data.Spans[len(data.Spans)-1].EndedAt
	impact, err := CalculateImpact(data.Spans, signals, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if impact.AffectedRuns != 40 || impact.TotalRuns != 1000 || impact.Recurrences != 40 || impact.AffectedRunRate != 0.04 {
		t.Fatalf("impact=%#v", impact)
	}
	cutoff := data.Spans[99*5].StartedAt
	early, err := CalculateImpact(data.Spans, signals, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if early.TotalRuns >= impact.TotalRuns || early.LastSeen.After(cutoff) {
		t.Fatalf("future data leaked: early=%#v full=%#v", early, impact)
	}
}

func TestIssueLifecycleRequiresHumanReviewAndCurrentClassifier(t *testing.T) {
	data := corpus.Generate()
	signals := signalsFor(detect.ReasonRetry, detect.New(detect.DefaultConfig()).Detect(data.Spans))
	at := data.Spans[len(data.Spans)-1].EndedAt
	impact, _ := CalculateImpact(data.Spans, signals, at)
	classifier := detect.Registry()[detect.ReasonRetry]
	issue, err := ProposeIssue(classifier, signals, impact, "team-signal", at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Transition(issue, domain.IssueConfirmed, "", "", at.Add(time.Minute)); err == nil {
		t.Fatal("confirmation without review should fail")
	}
	confirmed, err := Transition(issue, domain.IssueConfirmed, "reviewer", "reproduced against fixtures", at.Add(time.Minute))
	if err != nil || confirmed.State != domain.IssueConfirmed {
		t.Fatalf("issue=%#v err=%v", confirmed, err)
	}
	mitigated, err := Transition(confirmed, domain.IssueMitigated, "reviewer", "candidate fix in replay", at.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Transition(mitigated, domain.IssueResolved, "reviewer", "frozen eval passed", at.Add(3*time.Minute))
	if err != nil || resolved.State != domain.IssueResolved {
		t.Fatalf("resolved=%#v err=%v", resolved, err)
	}
	if _, err := Transition(resolved, domain.IssueRejected, "reviewer", "invalid", at.Add(4*time.Minute)); err == nil {
		t.Fatal("invalid terminal transition should fail")
	}
}

func TestClassifierMetricsAndInstrumentationCoverage(t *testing.T) {
	data := corpus.Generate()
	signals := detect.New(detect.DefaultConfig()).Detect(data.Spans)
	metrics := Evaluate(signals, truthMap(data))
	if metrics.Precision != 1 || metrics.Recall != 1 || metrics.InstrumentationCoverage != 0.96 {
		t.Fatalf("metrics=%#v", metrics)
	}
	falseSignal := signalsFor(detect.ReasonRetry, signals)[0]
	falseSignal.RunID = "run-1000"
	signals = append(signals, falseSignal)
	if degraded := Evaluate(signals, truthMap(data)); degraded.Precision >= 1 {
		t.Fatalf("false positive did not degrade precision: %#v", degraded)
	}
}

func TestReportSeparatesSignalsClustersAndReviewedIssues(t *testing.T) {
	data := corpus.Generate()
	signals := detect.New(detect.DefaultConfig()).Detect(data.Spans[:100])
	report := Summarize(signals, nil)
	markdown := Markdown(report)
	if report.Signals == 0 || report.ReviewedIssues != 0 || strings.Contains(markdown, "Human-reviewed issues: 1") {
		t.Fatalf("report=%#v markdown=%s", report, markdown)
	}
}
