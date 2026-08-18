package evalexport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/investigate"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/reliability"
)

func confirmedRetry(t *testing.T) (corpus.Corpus, domain.ReliabilityIssue, []domain.Signal, investigate.Handoff, time.Time) {
	t.Helper()
	data := corpus.Generate()
	allSignals := detect.New(detect.DefaultConfig()).Detect(data.Spans)
	signals := []domain.Signal{}
	for _, signal := range allSignals {
		if signal.ReasonCode == detect.ReasonRetry {
			signals = append(signals, signal)
		}
	}
	at := data.Spans[len(data.Spans)-1].EndedAt
	impact, err := reliability.CalculateImpact(data.Spans, signals, at)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := reliability.ProposeIssue(detect.Registry()[detect.ReasonRetry], signals, impact, "team-signal", at)
	if err != nil {
		t.Fatal(err)
	}
	issue, err = reliability.Transition(issue, domain.IssueConfirmed, "issue-reviewer", "retry regression reproduced", at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, handoff, err := (investigate.Service{Router: investigate.DefaultRouter()}).Execute(context.Background(), investigate.RouteRequest{Issue: issue, Signals: signals, At: at.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return data, issue, signals, handoff, at.Add(2 * time.Minute)
}

func TestConfirmedFailureExportsMinimalDeterministicCandidate(t *testing.T) {
	data, issue, signals, handoff, at := confirmedRetry(t)
	first, err := Build(issue, signals, data.Spans, handoff, at)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(issue, signals, data.Spans, handoff, at.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.PromotionState != "proposed" || first.ReasonCode != detect.ReasonRetry {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	fixture := string(first.Fixture)
	for _, forbidden := range []string{corpus.SecretCanary, "authorization", "user.email", "tenant-fictional", "user-"} {
		if strings.Contains(fixture, forbidden) {
			t.Fatalf("fixture contains %q: %s", forbidden, fixture)
		}
	}
}

func TestUnreviewedAndDriftSignalsCannotExport(t *testing.T) {
	data, issue, signals, handoff, at := confirmedRetry(t)
	issue.State = domain.IssueProposed
	if _, err := Build(issue, signals, data.Spans, handoff, at); err == nil {
		t.Fatal("unreviewed issue exported")
	}
	issue.State = domain.IssueConfirmed
	drift := detect.New(detect.DefaultConfig()).Detect(data.Spans[7*5 : 8*5])
	if _, err := Build(issue, drift, data.Spans, handoff, at); err == nil {
		t.Fatal("drift signal exported")
	}
}

func TestCatalogDeduplicatesAndPromotionIsSeparateReview(t *testing.T) {
	data, issue, signals, handoff, at := confirmedRetry(t)
	candidate, _ := Build(issue, signals, data.Spans, handoff, at)
	catalog := &Catalog{Path: filepath.Join(t.TempDir(), "evals.jsonl")}
	first, err := catalog.Put(candidate)
	if err != nil || !first {
		t.Fatalf("first=%v err=%v", first, err)
	}
	second, err := catalog.Put(candidate)
	if err != nil || second {
		t.Fatalf("second=%v err=%v", second, err)
	}
	if _, err := Promote(candidate, issue, "", ""); err == nil {
		t.Fatal("promotion without review succeeded")
	}
	promoted, err := Promote(candidate, issue, "eval-reviewer", "fixture reproduces classifier")
	if err != nil || promoted.PromotionState != "promoted" || promoted.ReviewActor == issue.ReviewActor {
		t.Fatalf("promoted=%#v err=%v", promoted, err)
	}
	items, err := catalog.ReadAll()
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
}

func TestGeneratedGoEvalExecutesAndBundlePreservesLineage(t *testing.T) {
	data, issue, signals, handoff, at := confirmedRetry(t)
	candidate, _ := Build(issue, signals, data.Spans, handoff, at)
	candidate, _ = Promote(candidate, issue, "eval-reviewer", "reproduced")
	source, err := GenerateGoTest(candidate, "generatedeval")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module generatedeval\n\ngo 1.24\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "failure_test.go"), source, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOCACHE="+filepath.Join(dir, "cache"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated eval failed: %v\n%s\n%s", err, output, source)
	}
	outcomes := []Outcome{{Kind: "shadow_replay", State: "passed", Evidence: "fixture://replay/1", RecordedAt: at}, {Kind: "release_decision", State: "held", Evidence: "fixture://release/1", RecordedAt: at.Add(time.Minute)}}
	bundle, err := NewBundle(candidate, handoff, outcomes)
	if err != nil || bundle.Digest == "" || bundle.Candidate.IssueID != issue.ID {
		t.Fatalf("bundle=%#v err=%v", bundle, err)
	}
}
