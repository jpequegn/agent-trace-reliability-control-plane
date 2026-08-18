package investigate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/reliability"
)

type fakeInvestigator struct {
	calls             int
	result            Result
	err               error
	usePacketEvidence bool
}

func (f *fakeInvestigator) Investigate(_ context.Context, packet domain.InvestigationPacket) (Result, error) {
	f.calls++
	if f.usePacketEvidence && len(packet.Evidence) > 0 {
		f.result.Hypotheses[0].EvidenceFor = []string{packet.Evidence[0].ID}
	}
	return f.result, f.err
}

func confirmedLatency(t *testing.T) (corpus.Corpus, domain.ReliabilityIssue, []domain.Signal, time.Time) {
	t.Helper()
	data := corpus.Generate()
	allSignals := detect.New(detect.DefaultConfig()).Detect(data.Spans)
	signals := []domain.Signal{}
	for _, signal := range allSignals {
		if signal.ReasonCode == detect.ReasonLatency {
			signals = append(signals, signal)
		}
	}
	at := data.ReleasePackets[0].DeployedAt.Add(5*time.Hour + 30*time.Minute)
	impact, err := reliability.CalculateImpact(data.Spans, signals, at)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := reliability.ProposeIssue(detect.Registry()[detect.ReasonLatency], signals, impact, "team-signal", at)
	if err != nil {
		t.Fatal(err)
	}
	issue, err = reliability.Transition(issue, domain.IssueConfirmed, "reviewer", "reproduced latency regression", at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return data, issue, signals, at.Add(time.Minute)
}

func TestCurrentMatchingReleaseRoutesBoundedPacket(t *testing.T) {
	data, issue, signals, at := confirmedLatency(t)
	result := Result{Hypotheses: []Finding{{Statement: "harness regression"}}, LikelyFixLayer: "harness"}
	investigator := &fakeInvestigator{result: result, usePacketEvidence: true}
	service := Service{Router: DefaultRouter(), Investigator: investigator}
	decision, handoff, err := service.Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, Release: &data.ReleasePackets[0], RequireReleaseContext: true, At: at})
	if err != nil {
		t.Fatal(err)
	}
	if decision.State != domain.WatchApproved || decision.Packet == nil || len(decision.Packet.Evidence) > 12 || investigator.calls != 1 {
		t.Fatalf("decision=%#v calls=%d", decision, investigator.calls)
	}
	if !handoff.DraftOnly || handoff.State != "investigated_draft" || handoff.Investigation == nil {
		t.Fatalf("handoff=%#v", handoff)
	}
	for _, tool := range decision.Plan.AllowedReads {
		if strings.Contains(tool, "write") {
			t.Fatalf("write tool allowed: %s", tool)
		}
	}
}

func TestStaleMissingAndConflictingContextSafelyStop(t *testing.T) {
	data, issue, signals, at := confirmedLatency(t)
	investigator := &fakeInvestigator{}
	service := Service{Router: DefaultRouter(), Investigator: investigator}
	tests := []struct {
		name    string
		release *domain.ReleasePacket
	}{
		{"missing", nil},
		{"stale", &data.ReleasePackets[2]},
		{"conflicting", &data.ReleasePackets[1]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, handoff, err := service.Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, Release: test.release, RequireReleaseContext: true, At: at})
			if err != nil || decision.State != domain.WatchMappingUncertain || handoff.ID != "" {
				t.Fatalf("decision=%#v handoff=%#v err=%v", decision, handoff, err)
			}
		})
	}
	if investigator.calls != 0 {
		t.Fatalf("investigator calls=%d", investigator.calls)
	}
}

func TestInvestigatorUnavailableDoesNotSuppressDeterministicHandoff(t *testing.T) {
	_, issue, signals, at := confirmedLatency(t)
	decision, handoff, err := (Service{Router: DefaultRouter()}).Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, At: at})
	if err != nil || decision.State != domain.WatchApproved || !handoff.InvestigatorUnavailable || len(handoff.DeterministicReasonCodes) == 0 {
		t.Fatalf("decision=%#v handoff=%#v err=%v", decision, handoff, err)
	}
}

func TestAdversarialInvestigatorCannotEscapeEvidenceOrOutputBudget(t *testing.T) {
	_, issue, signals, at := confirmedLatency(t)
	unknown := &fakeInvestigator{result: Result{Hypotheses: []Finding{{Statement: "unsupported", EvidenceFor: []string{"raw-secret-span"}}}}}
	_, _, err := (Service{Router: DefaultRouter(), Investigator: unknown}).Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, At: at})
	if err == nil || !strings.Contains(err.Error(), "outside packet") {
		t.Fatalf("error=%v", err)
	}
	oversized := &fakeInvestigator{result: Result{Hypotheses: []Finding{{Statement: strings.Repeat("x", 5000)}}}}
	_, _, err = (Service{Router: DefaultRouter(), Investigator: oversized}).Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, At: at})
	if err == nil || !strings.Contains(err.Error(), "exceeds budget") {
		t.Fatalf("error=%v", err)
	}
}

func TestAbstentionRequiresReasonAndRemainsDraft(t *testing.T) {
	_, issue, signals, at := confirmedLatency(t)
	investigator := &fakeInvestigator{result: Result{Abstained: true, AbstainReason: "evidence is insufficient"}}
	_, handoff, err := (Service{Router: DefaultRouter(), Investigator: investigator}).Execute(context.Background(), RouteRequest{Issue: issue, Signals: signals, At: at})
	if err != nil || !handoff.Investigation.Abstained || !handoff.DraftOnly {
		t.Fatalf("handoff=%#v err=%v", handoff, err)
	}
}
