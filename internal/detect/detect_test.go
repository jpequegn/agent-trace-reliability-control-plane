package detect

import (
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

func TestDetectorsMatchEverySeededClass(t *testing.T) {
	data := corpus.Generate()
	engine := New(DefaultConfig())
	signals := engine.Detect(data.Spans)
	reasonsByRun := map[string]map[string]domain.Signal{}
	for _, signal := range signals {
		if reasonsByRun[signal.RunID] == nil {
			reasonsByRun[signal.RunID] = map[string]domain.Signal{}
		}
		reasonsByRun[signal.RunID][signal.ReasonCode] = signal
	}
	for runID, truth := range data.Truth {
		if truth.Class == "healthy" {
			if len(reasonsByRun[runID]) != 0 {
				t.Fatalf("healthy run %s has signals %#v", runID, reasonsByRun[runID])
			}
			continue
		}
		signal, ok := reasonsByRun[runID][truth.ExpectedReason]
		if !ok {
			t.Fatalf("run=%s class=%s expected=%s got=%#v", runID, truth.Class, truth.ExpectedReason, reasonsByRun[runID])
		}
		if truth.Class == "instrumentation_drift" {
			if signal.State != domain.SignalInstrumentationDrift || signal.ClassifierVersion != "" {
				t.Fatalf("drift signal=%#v", signal)
			}
		} else if err := ValidateClassifierSignal(signal); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSignalsAreDeterministicAndTenantIsolated(t *testing.T) {
	data := corpus.Generate()
	engine := New(DefaultConfig())
	first := engine.Detect(data.Spans[:50])
	second := engine.Detect(data.Spans[:50])
	if len(first) != len(second) {
		t.Fatalf("first=%d second=%d", len(first), len(second))
	}
	for index := range first {
		if first[index].ID != second[index].ID || first[index].TenantID != "tenant-fictional" {
			t.Fatalf("first=%#v second=%#v", first[index], second[index])
		}
	}
}

func TestFrequencyChangeRequiresVolumeAndUsesPastBaseline(t *testing.T) {
	data := corpus.Generate()
	baselineSpans := cloneRuns(data.Spans[400*5:430*5], false)
	currentSpans := cloneRuns(data.Spans[430*5:460*5], true)
	engine := New(DefaultConfig())
	cutoff := baselineSpans[len(baselineSpans)-1].EndedAt
	baseline := engine.BuildBaseline(baselineSpans, cutoff)
	changes := engine.FrequencyChanges(currentSpans, baseline, currentSpans[len(currentSpans)-1].EndedAt)
	if len(changes) != 1 || changes[0].ReasonCode != ReasonFrequencyChange {
		t.Fatalf("changes=%#v", changes)
	}
	if got := engine.FrequencyChanges(currentSpans[:10*5], baseline, time.Now().UTC()); len(got) != 0 {
		t.Fatalf("minimum-volume guard failed: %#v", got)
	}
	for _, span := range data.Spans {
		if span.StartedAt.After(cutoff) {
			continue
		}
	}
	if !baseline.GeneratedAt.Equal(cutoff) {
		t.Fatalf("baseline time=%s cutoff=%s", baseline.GeneratedAt, cutoff)
	}
}

func TestUnknownOrDriftCannotActivateIssue(t *testing.T) {
	signal := domain.Signal{ID: "drift", State: domain.SignalInstrumentationDrift, ReasonCode: ReasonDrift}
	if err := ValidateClassifierSignal(signal); err == nil {
		t.Fatal("expected drift signal to be rejected")
	}
}

func cloneRuns(spans []domain.Span, retry bool) []domain.Span {
	result := make([]domain.Span, len(spans))
	for index, span := range spans {
		span.CohortID = "cohort-frequency"
		span.Versions.Harness = "harness-frequency"
		span.Attributes = map[string]string{}
		for key, value := range spans[index].Attributes {
			span.Attributes[key] = value
		}
		if retry {
			span.Attributes["retry.count"] = "9"
		}
		result[index] = span
	}
	return result
}
