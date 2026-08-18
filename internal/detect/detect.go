package detect

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

const (
	RuleVersion       = "detectors-v1"
	ClassifierVersion = "classifiers-v1"

	ReasonRetry           = "retry_limit_exceeded"
	ReasonPermission      = "tool_permission_denied"
	ReasonToolError       = "tool_error_rate"
	ReasonLatency         = "run_latency"
	ReasonAbandonment     = "user_abandonment"
	ReasonMalformed       = "malformed_output"
	ReasonLoop            = "agent_loop"
	ReasonDrift           = "instrumentation_drift"
	ReasonFrequencyChange = "frequency_change"
)

type Config struct {
	MaxRunLatency       time.Duration
	MaxRetries          int
	MaxLoops            int
	MinimumVolume       int
	FrequencyRateChange float64
}

func DefaultConfig() Config {
	return Config{MaxRunLatency: 10 * time.Second, MaxRetries: 3, MaxLoops: 8, MinimumVolume: 20, FrequencyRateChange: 0.20}
}

type Engine struct {
	Config Config
}

func New(config Config) Engine { return Engine{Config: config} }

func Registry() map[string]domain.Classifier {
	definitions := []struct {
		reason, description string
		threshold           float64
	}{
		{ReasonRetry, "run retry count exceeds the deterministic limit", 3},
		{ReasonPermission, "a read-only tool call was denied", 1},
		{ReasonToolError, "a tool call returned an error", 1},
		{ReasonLatency, "root run latency exceeds the service limit", 10},
		{ReasonAbandonment, "the user cancelled before a successful outcome", 1},
		{ReasonMalformed, "the final output violates the declared format", 1},
		{ReasonLoop, "agent loop count exceeds the deterministic limit", 8},
		{ReasonFrequencyChange, "a cohort/version reason rate exceeds its baseline", 0.20},
	}
	result := map[string]domain.Classifier{}
	for _, definition := range definitions {
		id := "classifier." + definition.reason
		result[definition.reason] = domain.Classifier{ID: id, Version: ClassifierVersion, ReasonCode: definition.reason, Description: definition.description, MinimumVolume: 1, Threshold: definition.threshold, Owner: "team-signal"}
	}
	return result
}

func (e Engine) Detect(spans []domain.Span) []domain.Signal {
	runs := groupRuns(spans)
	keys := make([]string, 0, len(runs))
	for key := range runs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := []domain.Signal{}
	for _, key := range keys {
		result = append(result, e.detectRun(runs[key])...)
	}
	return result
}

func (e Engine) detectRun(spans []domain.Span) []domain.Signal {
	if len(spans) == 0 {
		return nil
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].StartedAt.Before(spans[j].StartedAt) })
	first, last := spans[0], spans[len(spans)-1]
	for _, span := range spans {
		if span.Attributes["agent.workflow"] == "" {
			return []domain.Signal{newSignal(ReasonDrift, domain.SignalInstrumentationDrift, 2, span, first.StartedAt, last.EndedAt)}
		}
	}
	reasons := map[string]domain.Span{}
	for _, span := range spans {
		if parseInt(span.Attributes["retry.count"]) > e.Config.MaxRetries {
			reasons[ReasonRetry] = span
		}
		if parseInt(span.Attributes["loop.count"]) > e.Config.MaxLoops {
			reasons[ReasonLoop] = span
		}
		if span.Attributes["user.cancelled"] == "true" {
			reasons[ReasonAbandonment] = span
		}
		if span.Attributes["output.format"] == "invalid" {
			reasons[ReasonMalformed] = span
		}
		if span.Tool != nil && span.Status == domain.StatusError {
			if strings.Contains(strings.ToLower(span.StatusMessage), "permission") {
				reasons[ReasonPermission] = span
			} else {
				reasons[ReasonToolError] = span
			}
		}
		if span.ParentSpanID == "" && span.EndedAt.Sub(span.StartedAt) > e.Config.MaxRunLatency {
			reasons[ReasonLatency] = span
		}
	}
	ordered := []string{ReasonRetry, ReasonPermission, ReasonToolError, ReasonLatency, ReasonAbandonment, ReasonMalformed, ReasonLoop}
	result := []domain.Signal{}
	for _, reason := range ordered {
		if evidence, ok := reasons[reason]; ok {
			result = append(result, newSignal(reason, domain.SignalDetected, severity(reason), evidence, first.StartedAt, last.EndedAt))
		}
	}
	return result
}

func newSignal(reason string, state domain.SignalState, severity int, span domain.Span, first, last time.Time) domain.Signal {
	digest, _ := domain.Digest(span)
	evidenceID, _ := domain.StableID("evidence", struct{ SpanID, Digest string }{span.SpanID, digest})
	classifierVersion := ClassifierVersion
	if state == domain.SignalInstrumentationDrift {
		classifierVersion = ""
	}
	signal := domain.Signal{RuleID: "rule." + reason, RuleVersion: RuleVersion, ReasonCode: reason, State: state, TenantID: span.TenantID, ProjectID: span.ProjectID, RunID: span.RunID, CohortID: span.CohortID, FirstSeen: first, LastSeen: last, Severity: severity, Evidence: []domain.EvidenceRef{{ID: evidenceID, Kind: "sanitized_span", Digest: digest, RecordedAt: last}}, ClassifierVersion: classifierVersion}
	signal.ID, _ = domain.StableID("signal", struct{ Rule, Tenant, Run string }{signal.RuleID, signal.TenantID, signal.RunID})
	return signal
}

type Baseline struct {
	GeneratedAt time.Time                `json:"generated_at"`
	Groups      map[string]BaselineGroup `json:"groups"`
}

type BaselineGroup struct {
	Runs        int                `json:"runs"`
	ReasonRates map[string]float64 `json:"reason_rates"`
}

func (e Engine) BuildBaseline(spans []domain.Span, cutoff time.Time) Baseline {
	filtered := make([]domain.Span, 0, len(spans))
	for _, span := range spans {
		if !span.StartedAt.After(cutoff) {
			filtered = append(filtered, span)
		}
	}
	return e.baseline(filtered, cutoff)
}

func (e Engine) baseline(spans []domain.Span, generatedAt time.Time) Baseline {
	runs := groupRuns(spans)
	groups := map[string][]string{}
	for _, run := range runs {
		if len(run) == 0 {
			continue
		}
		group := run[0].CohortID + "/" + run[0].Versions.Harness
		groups[group] = append(groups[group], run[0].TenantID+"/"+run[0].RunID)
	}
	signals := e.Detect(spans)
	reasonsByRun := map[string]map[string]bool{}
	for _, signal := range signals {
		if signal.State != domain.SignalDetected {
			continue
		}
		key := signal.TenantID + "/" + signal.RunID
		if reasonsByRun[key] == nil {
			reasonsByRun[key] = map[string]bool{}
		}
		reasonsByRun[key][signal.ReasonCode] = true
	}
	baseline := Baseline{GeneratedAt: generatedAt, Groups: map[string]BaselineGroup{}}
	for group, runKeys := range groups {
		counts := map[string]int{}
		for _, runKey := range runKeys {
			for reason := range reasonsByRun[runKey] {
				counts[reason]++
			}
		}
		rates := map[string]float64{}
		for reason, count := range counts {
			rates[reason] = float64(count) / float64(len(runKeys))
		}
		baseline.Groups[group] = BaselineGroup{Runs: len(runKeys), ReasonRates: rates}
	}
	return baseline
}

func (e Engine) FrequencyChanges(current []domain.Span, baseline Baseline, at time.Time) []domain.Signal {
	currentBaseline := e.baseline(current, at)
	result := []domain.Signal{}
	for group, currentGroup := range currentBaseline.Groups {
		if currentGroup.Runs < e.Config.MinimumVolume {
			continue
		}
		prior, ok := baseline.Groups[group]
		if !ok || prior.Runs < e.Config.MinimumVolume {
			continue
		}
		for reason, rate := range currentGroup.ReasonRates {
			if rate-prior.ReasonRates[reason] < e.Config.FrequencyRateChange {
				continue
			}
			digest, _ := domain.Digest(struct {
				Group, Reason string
				Rate, Prior   float64
			}{group, reason, rate, prior.ReasonRates[reason]})
			signal := domain.Signal{RuleID: "rule." + ReasonFrequencyChange, RuleVersion: RuleVersion, ReasonCode: ReasonFrequencyChange, State: domain.SignalDetected, CohortID: group, FirstSeen: baseline.GeneratedAt, LastSeen: at, Severity: 3, Evidence: []domain.EvidenceRef{{ID: "baseline_" + digest[:16], Kind: "cohort_baseline", Digest: digest, RecordedAt: at}}, ClassifierVersion: ClassifierVersion}
			signal.ID, _ = domain.StableID("signal", struct {
				Group, Reason string
				At            time.Time
			}{group, reason, at})
			result = append(result, signal)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func groupRuns(spans []domain.Span) map[string][]domain.Span {
	result := map[string][]domain.Span{}
	for _, span := range spans {
		key := span.TenantID + "/" + span.RunID
		result[key] = append(result[key], span)
	}
	return result
}

func parseInt(value string) int {
	result, _ := strconv.Atoi(value)
	return result
}

func severity(reason string) int {
	values := map[string]int{ReasonPermission: 4, ReasonToolError: 4, ReasonRetry: 3, ReasonLatency: 3, ReasonLoop: 3, ReasonAbandonment: 2, ReasonMalformed: 2}
	return values[reason]
}

func ValidateClassifierSignal(signal domain.Signal) error {
	if signal.State != domain.SignalDetected {
		return fmt.Errorf("signal state %s cannot activate an issue", signal.State)
	}
	classifier, ok := Registry()[signal.ReasonCode]
	if !ok || signal.ClassifierVersion != classifier.Version {
		return fmt.Errorf("signal %s lacks a current classifier", signal.ID)
	}
	if len(signal.Evidence) == 0 {
		return fmt.Errorf("signal %s lacks evidence", signal.ID)
	}
	return nil
}
