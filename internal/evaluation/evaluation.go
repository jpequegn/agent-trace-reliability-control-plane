package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/evalexport"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/ingest"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/investigate"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/reliability"
)

type LoadReport struct {
	Requested      int     `json:"requested_spans"`
	Processed      int     `json:"processed_spans"`
	Dropped        int     `json:"dropped_spans"`
	Batches        int     `json:"batches"`
	PeakBatch      int     `json:"peak_batch_spans"`
	SpansPerSecond float64 `json:"spans_per_second"`
	CanaryLeaks    int     `json:"canary_leaks"`
}

type Report struct {
	Schema                 string              `json:"schema_version"`
	CorpusVersion          string              `json:"corpus_version"`
	GeneratedAt            time.Time           `json:"generated_at"`
	Runs                   int                 `json:"runs"`
	Spans                  int                 `json:"spans"`
	RedactedFields         int                 `json:"redacted_fields"`
	CanaryLeaks            int                 `json:"canary_leaks"`
	Classifier             reliability.Metrics `json:"classifier"`
	AverageAlertLatencyMS  float64             `json:"average_alert_latency_ms"`
	ImpactEstimationError  float64             `json:"impact_estimation_error"`
	ReleaseContextCoverage float64             `json:"release_context_coverage"`
	SafeStopRate           float64             `json:"safe_stop_rate"`
	EvalCandidatesPromoted int                 `json:"eval_candidates_promoted"`
	Load                   LoadReport          `json:"load"`
	QualityGatePassed      bool                `json:"quality_gate_passed"`
	QualityGateFailures    []string            `json:"quality_gate_failures,omitempty"`
}

type Artifacts struct {
	Report    Report
	Issue     domain.ReliabilityIssue
	Handoff   investigate.Handoff
	Candidate domain.EvalCandidate
	Bundle    evalexport.Bundle
}

func Run(ctx context.Context) (Artifacts, error) {
	data := corpus.Generate()
	if err := data.Validate(); err != nil {
		return Artifacts{}, err
	}
	sanitized := make([]domain.Span, 0, len(data.Spans))
	redacted := 0
	for _, span := range data.Spans {
		clean, count, err := ingest.Sanitize(span)
		if err != nil {
			return Artifacts{}, err
		}
		redacted += count
		sanitized = append(sanitized, clean)
	}
	encoded, _ := json.Marshal(sanitized)
	leaks := 0
	if strings.Contains(string(encoded), corpus.SecretCanary) || strings.Contains(string(encoded), "fictional@example.test") {
		leaks = 1
	}
	engine := detect.New(detect.DefaultConfig())
	signals := engine.Detect(sanitized)
	truth := map[string]string{}
	for runID, item := range data.Truth {
		truth[runID] = item.ExpectedReason
	}
	metrics := reliability.Evaluate(signals, truth)
	latencyTotal, detectedCount := 0.0, 0
	runStart := map[string]time.Time{}
	for _, span := range sanitized {
		if current, ok := runStart[span.RunID]; !ok || span.StartedAt.Before(current) {
			runStart[span.RunID] = span.StartedAt
		}
	}
	for _, signal := range signals {
		if signal.State == domain.SignalDetected {
			latencyTotal += float64(signal.LastSeen.Sub(runStart[signal.RunID]).Milliseconds())
			detectedCount++
		}
	}
	retrySignals := filter(signals, detect.ReasonRetry)
	asOf := sanitized[len(sanitized)-1].EndedAt
	impact, err := reliability.CalculateImpact(sanitized, retrySignals, asOf)
	if err != nil {
		return Artifacts{}, err
	}
	issue, err := reliability.ProposeIssue(detect.Registry()[detect.ReasonRetry], retrySignals, impact, "team-signal", asOf)
	if err != nil {
		return Artifacts{}, err
	}
	issue, err = reliability.Transition(issue, domain.IssueConfirmed, "fixture-reviewer", "retry regression reproduced", asOf.Add(time.Minute))
	if err != nil {
		return Artifacts{}, err
	}
	_, handoff, err := (investigate.Service{Router: investigate.DefaultRouter()}).Execute(ctx, investigate.RouteRequest{Issue: issue, Signals: retrySignals, At: asOf.Add(time.Minute)})
	if err != nil {
		return Artifacts{}, err
	}
	candidate, err := evalexport.Build(issue, retrySignals, sanitized, handoff, asOf.Add(2*time.Minute))
	if err != nil {
		return Artifacts{}, err
	}
	candidate, err = evalexport.Promote(candidate, issue, "eval-reviewer", "fixture reproduces current classifier")
	if err != nil {
		return Artifacts{}, err
	}
	outcomes := []evalexport.Outcome{{Kind: "shadow_replay", State: "passed", Evidence: "fixture://replay/retry", RecordedAt: asOf.Add(3 * time.Minute)}, {Kind: "release_decision", State: "advisory_only", Evidence: "fixture://release/policy", RecordedAt: asOf.Add(4 * time.Minute)}}
	bundle, err := evalexport.NewBundle(candidate, handoff, outcomes)
	if err != nil {
		return Artifacts{}, err
	}
	coverage, safeStop, err := releaseRouting(data, sanitized, signals)
	if err != nil {
		return Artifacts{}, err
	}
	load, err := RunLoad(ctx, 100_000)
	if err != nil {
		return Artifacts{}, err
	}
	report := Report{Schema: "trace-reliability-evaluation-v1", CorpusVersion: data.Version, GeneratedAt: asOf.Add(5 * time.Minute), Runs: len(data.Truth), Spans: len(sanitized), RedactedFields: redacted, CanaryLeaks: leaks, Classifier: metrics, AverageAlertLatencyMS: latencyTotal / float64(detectedCount), ImpactEstimationError: abs(impact.AffectedRuns-40) / 40, ReleaseContextCoverage: coverage, SafeStopRate: safeStop, EvalCandidatesPromoted: 1, Load: load}
	report.QualityGateFailures = gate(report)
	report.QualityGatePassed = len(report.QualityGateFailures) == 0
	return Artifacts{Report: report, Issue: issue, Handoff: handoff, Candidate: candidate, Bundle: bundle}, nil
}

func RunLoad(ctx context.Context, requested int) (LoadReport, error) {
	started := time.Now()
	generated, batches, peak := 0, 0, 0
	sink := &countingSink{}
	ingestor := ingest.New(sink, 5000, 4)
	for generated < requested {
		if err := ctx.Err(); err != nil {
			return LoadReport{}, err
		}
		batch := corpus.Generate().Spans
		remaining := requested - generated
		if len(batch) > remaining {
			batch = batch[:remaining]
		}
		for index := range batch {
			span := batch[index]
			span.RunID = fmt.Sprintf("load-%03d-%s", batches, span.RunID)
			span.SpanID = fmt.Sprintf("load-%03d-%s", batches, span.SpanID)
			span.TraceID = fmt.Sprintf("load-%03d-%s", batches, span.TraceID)
			batch[index] = span
		}
		result, err := ingestor.Ingest(ctx, batch)
		if err != nil {
			return LoadReport{}, fmt.Errorf("load ingestion failed after accepting %d of %d: %w", result.Accepted, len(batch), err)
		}
		if result.Accepted != len(batch) {
			return LoadReport{}, fmt.Errorf("load ingestion accepted %d of %d spans", result.Accepted, len(batch))
		}
		generated += len(batch)
		batches++
		if len(batch) > peak {
			peak = len(batch)
		}
	}
	duration := time.Since(started).Seconds()
	if duration <= 0 {
		duration = 1
	}
	return LoadReport{Requested: requested, Processed: sink.count, Dropped: requested - sink.count, Batches: batches, PeakBatch: peak, SpansPerSecond: float64(sink.count) / duration, CanaryLeaks: sink.canaryLeaks}, nil
}

type countingSink struct {
	count       int
	canaryLeaks int
}

func (s *countingSink) Append(span domain.Span) (bool, error) {
	encoded, err := json.Marshal(span)
	if err != nil {
		return false, err
	}
	if strings.Contains(string(encoded), corpus.SecretCanary) || strings.Contains(string(encoded), "fictional@example.test") {
		s.canaryLeaks++
	}
	s.count++
	return true, nil
}

func releaseRouting(data corpus.Corpus, spans []domain.Span, signals []domain.Signal) (float64, float64, error) {
	latency := filter(signals, detect.ReasonLatency)
	at := data.ReleasePackets[0].DeployedAt.Add(5*time.Hour + 30*time.Minute)
	impact, err := reliability.CalculateImpact(spans, latency, at)
	if err != nil {
		return 0, 0, err
	}
	issue, err := reliability.ProposeIssue(detect.Registry()[detect.ReasonLatency], latency, impact, "team-signal", at)
	if err != nil {
		return 0, 0, err
	}
	issue, err = reliability.Transition(issue, domain.IssueConfirmed, "fixture-reviewer", "latency reproduced", at.Add(time.Minute))
	if err != nil {
		return 0, 0, err
	}
	router := investigate.DefaultRouter()
	approved := 0
	safeStops := 0
	for index := range data.ReleasePackets {
		decision, err := router.Route(investigate.RouteRequest{Issue: issue, Signals: latency, Release: &data.ReleasePackets[index], RequireReleaseContext: true, At: at.Add(time.Minute)})
		if err != nil {
			return 0, 0, err
		}
		if decision.State == domain.WatchApproved {
			approved++
		} else if decision.State == domain.WatchMappingUncertain {
			safeStops++
		}
	}
	return float64(approved) / float64(len(data.ReleasePackets)), float64(safeStops) / float64(len(data.ReleasePackets)-1), nil
}

func filter(signals []domain.Signal, reason string) []domain.Signal {
	result := []domain.Signal{}
	for _, signal := range signals {
		if signal.ReasonCode == reason {
			result = append(result, signal)
		}
	}
	return result
}

func gate(report Report) []string {
	failures := []string{}
	if report.Spans != 5000 || report.Runs != 1000 {
		failures = append(failures, "corpus count mismatch")
	}
	if report.CanaryLeaks != 0 {
		failures = append(failures, "secret canary leakage")
	}
	if report.Classifier.Precision < 0.99 || report.Classifier.Recall < 0.99 {
		failures = append(failures, "classifier quality below 0.99")
	}
	if report.ImpactEstimationError > 0.01 {
		failures = append(failures, "impact estimation error above 0.01")
	}
	if report.SafeStopRate < 1 || report.EvalCandidatesPromoted != 1 {
		failures = append(failures, "routing or eval lineage gate failed")
	}
	if report.Load.Processed != 100_000 || report.Load.Dropped != 0 || report.Load.CanaryLeaks != 0 {
		failures = append(failures, "100000-span load gate failed")
	}
	return failures
}

func Markdown(report Report) string {
	status := "PASSED"
	if !report.QualityGatePassed {
		status = "FAILED"
	}
	return fmt.Sprintf("# Agent trace reliability evaluation\n\n- Quality gate: **%s**\n- Corpus: %d runs / %d spans\n- Classifier precision: %.3f\n- Classifier recall: %.3f\n- Instrumentation coverage: %.3f\n- Canary leaks: %d\n- Average alert latency: %.1f ms\n- Impact estimation error: %.3f\n- Release-context coverage: %.3f\n- Unsafe-context safe-stop rate: %.3f\n- Promoted eval candidates: %d\n- Load: %d processed, %d dropped, %d canary leaks, %.0f spans/s\n", status, report.Runs, report.Spans, report.Classifier.Precision, report.Classifier.Recall, report.Classifier.InstrumentationCoverage, report.CanaryLeaks, report.AverageAlertLatencyMS, report.ImpactEstimationError, report.ReleaseContextCoverage, report.SafeStopRate, report.EvalCandidatesPromoted, report.Load.Processed, report.Load.Dropped, report.Load.CanaryLeaks, report.Load.SpansPerSecond)
}

func abs(value int) float64 {
	if value < 0 {
		value = -value
	}
	return float64(value)
}
