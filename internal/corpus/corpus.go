package corpus

import (
	"fmt"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

const (
	SpanCount    = 5000
	RunCount     = 1000
	SecretCanary = "sk-live-CANARY-DO-NOT-STORE"
)

type Truth struct {
	RunID          string `json:"run_id"`
	Class          string `json:"class"`
	ExpectedReason string `json:"expected_reason"`
	Affected       bool   `json:"affected"`
}

type Corpus struct {
	Version        string                 `json:"version"`
	Fictional      bool                   `json:"fictional"`
	Spans          []domain.Span          `json:"spans"`
	Truth          map[string]Truth       `json:"truth"`
	ReleasePackets []domain.ReleasePacket `json:"release_packets"`
}

var classes = []string{"healthy", "retry_loop", "permission_denial", "tool_error", "latency", "abandonment", "malformed_output", "loop", "instrumentation_drift"}

func Generate() Corpus {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	result := Corpus{Version: "2026-08-18.1", Fictional: true, Truth: map[string]Truth{}}
	for index := 0; index < RunCount; index++ {
		runID := fmt.Sprintf("run-%04d", index+1)
		class := "healthy"
		if index < 320 {
			class = classes[1+(index%8)]
		}
		started := base.Add(time.Duration(index) * time.Minute)
		spans := runSpans(index, runID, class, started)
		result.Spans = append(result.Spans, spans...)
		result.Truth[runID] = Truth{RunID: runID, Class: class, ExpectedReason: reasonFor(class), Affected: class != "healthy" && class != "instrumentation_drift"}
	}
	result.ReleasePackets = releasePackets(base)
	return result
}

func runSpans(index int, runID, class string, started time.Time) []domain.Span {
	traceID := fmt.Sprintf("%032x", index+1)
	versions := domain.VersionSet{Model: "model-v1", Prompt: "prompt-v1", Tools: "tools-v1", Policy: "policy-v1", Harness: "harness-v1", Memory: "memory-v1"}
	if index >= 500 {
		versions.Harness = "harness-v2"
	}
	attrs := map[string]string{"agent.workflow": "research", "output.format": "json", "retry.count": "0", "loop.count": "1", "user.cancelled": "false"}
	if index%97 == 0 {
		attrs["authorization"] = SecretCanary
		attrs["user.email"] = "fictional@example.test"
	}
	durations := []time.Duration{2 * time.Second, 300 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 100 * time.Millisecond}
	statuses := []domain.SpanStatus{domain.StatusOK, domain.StatusOK, domain.StatusOK, domain.StatusOK, domain.StatusOK}
	messages := make([]string, 5)
	switch class {
	case "retry_loop":
		attrs["retry.count"] = "7"
		durations[1] = 8 * time.Second
	case "permission_denial":
		statuses[2] = domain.StatusError
		messages[2] = "permission denied"
	case "tool_error":
		statuses[3] = domain.StatusError
		messages[3] = "tool execution failed"
	case "latency":
		durations[0] = 15 * time.Second
	case "abandonment":
		attrs["user.cancelled"] = "true"
		statuses[4] = domain.StatusUnset
	case "malformed_output":
		attrs["output.format"] = "invalid"
	case "loop":
		attrs["loop.count"] = "12"
	case "instrumentation_drift":
		delete(attrs, "agent.workflow")
	}
	names := []string{"agent.run", "tool.search", "tool.calculate", "tool.notify", "agent.finalize"}
	spans := make([]domain.Span, 0, len(names))
	cursor := started
	for spanIndex, name := range names {
		spanID := fmt.Sprintf("%016x", index*10+spanIndex+1)
		parent := ""
		if spanIndex > 0 {
			parent = fmt.Sprintf("%016x", index*10+1)
		}
		spanAttrs := clone(attrs)
		var tool *domain.ToolCall
		if spanIndex >= 1 && spanIndex <= 3 {
			result := "ok"
			if statuses[spanIndex] == domain.StatusError {
				result = "error"
			}
			tool = &domain.ToolCall{Name: name[5:], Intent: "serve fictional user request", Authority: "read_only", Result: result}
		}
		span := domain.Span{Schema: domain.SchemaVersion, TenantID: "tenant-fictional", ProjectID: "agent-lab", RunID: runID, SessionID: fmt.Sprintf("session-%03d", index/5), UserID: fmt.Sprintf("user-%03d", index%100), CohortID: fmt.Sprintf("cohort-%d", index%4), TraceID: traceID, SpanID: spanID, ParentSpanID: parent, Name: name, Kind: "internal", StartedAt: cursor, EndedAt: cursor.Add(durations[spanIndex]), Status: statuses[spanIndex], StatusMessage: messages[spanIndex], Attributes: spanAttrs, Tool: tool, Versions: versions, CostMicros: int64(100 + spanIndex*25), Privacy: domain.PrivacyInternal, RetentionDays: 30}
		spans = append(spans, span)
		cursor = span.EndedAt
	}
	return spans
}

func releasePackets(base time.Time) []domain.ReleasePacket {
	legitimate := domain.ReleasePacket{ID: "release-latency", Service: "agent-lab", Version: "harness-v2", Owner: "team-signal", DeployedAt: base, ExpiresAt: base.Add(6 * time.Hour), ExpectedSignals: []string{"run_latency"}, AllowedReadTools: []string{"trace.read"}, BaselineCohort: "harness-v1", RollbackRef: "rollback-harness-v2"}
	legitimate.EvidenceDigest, _ = domain.Digest(legitimate.ID)
	ambiguous := domain.ReleasePacket{ID: "release-ambiguous", Service: "unknown", Version: "prompt-v2", Owner: "", DeployedAt: base.Add(14 * time.Hour), ExpiresAt: base.Add(16 * time.Hour), ExpectedSignals: []string{"tool_error"}, AllowedReadTools: []string{"trace.read"}, BaselineCohort: "prompt-v1", RollbackRef: "rollback-prompt-v2"}
	ambiguous.EvidenceDigest, _ = domain.Digest(ambiguous.ID)
	stale := legitimate
	stale.ID = "release-stale"
	stale.DeployedAt = base.Add(-48 * time.Hour)
	stale.ExpiresAt = base.Add(-47 * time.Hour)
	stale.EvidenceDigest, _ = domain.Digest(stale.ID)
	return []domain.ReleasePacket{legitimate, ambiguous, stale}
}

func (c Corpus) Validate() error {
	if !c.Fictional || len(c.Spans) != SpanCount || len(c.Truth) != RunCount {
		return fmt.Errorf("corpus requires fictional marker, %d spans, and %d runs", SpanCount, RunCount)
	}
	coverage := map[string]bool{}
	spansByRun := map[string]int{}
	for _, span := range c.Spans {
		if err := span.Validate(); err != nil {
			return fmt.Errorf("%s: %w", span.SpanID, err)
		}
		spansByRun[span.RunID]++
	}
	for runID, truth := range c.Truth {
		coverage[truth.Class] = true
		if spansByRun[runID] != 5 {
			return fmt.Errorf("run %s has %d spans", runID, spansByRun[runID])
		}
	}
	for _, class := range classes {
		if !coverage[class] {
			return fmt.Errorf("missing scenario class %s", class)
		}
	}
	return nil
}

func reasonFor(class string) string {
	return map[string]string{"healthy": "none", "retry_loop": "retry_limit_exceeded", "permission_denial": "tool_permission_denied", "tool_error": "tool_error_rate", "latency": "run_latency", "abandonment": "user_abandonment", "malformed_output": "malformed_output", "loop": "agent_loop", "instrumentation_drift": "instrumentation_drift"}[class]
}

func clone(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
