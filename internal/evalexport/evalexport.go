package evalexport

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/ingest"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/investigate"
)

const FixtureSchema = "code-native-eval-v1"

type FixtureSpan struct {
	Name           string            `json:"name"`
	Status         domain.SpanStatus `json:"status"`
	DurationMillis int64             `json:"duration_millis"`
	Attributes     map[string]string `json:"attributes,omitempty"`
	ToolAuthority  string            `json:"tool_authority,omitempty"`
	ToolResult     string            `json:"tool_result,omitempty"`
}

type Fixture struct {
	Schema            string        `json:"schema_version"`
	Project           string        `json:"project"`
	ReasonCode        string        `json:"reason_code"`
	ClassifierID      string        `json:"classifier_id"`
	ClassifierVersion string        `json:"classifier_version"`
	Spans             []FixtureSpan `json:"spans"`
	ExpectedOutcome   string        `json:"expected_outcome"`
}

type Outcome struct {
	Kind       string    `json:"kind"`
	State      string    `json:"state"`
	Evidence   string    `json:"evidence"`
	RecordedAt time.Time `json:"recorded_at"`
}

type Bundle struct {
	Schema    string               `json:"schema_version"`
	Candidate domain.EvalCandidate `json:"candidate"`
	Handoff   investigate.Handoff  `json:"handoff"`
	Outcomes  []Outcome            `json:"outcomes,omitempty"`
	Digest    string               `json:"digest"`
}

func Build(issue domain.ReliabilityIssue, signals []domain.Signal, spans []domain.Span, handoff investigate.Handoff, at time.Time) (domain.EvalCandidate, error) {
	if issue.State != domain.IssueConfirmed || issue.ReviewActor == "" {
		return domain.EvalCandidate{}, errors.New("eval export requires a human-confirmed issue")
	}
	if handoff.IssueID != issue.ID || !handoff.DraftOnly {
		return domain.EvalCandidate{}, errors.New("eval export requires a draft handoff for the issue")
	}
	matching := []domain.Signal{}
	for _, signal := range signals {
		if err := detect.ValidateClassifierSignal(signal); err != nil {
			return domain.EvalCandidate{}, err
		}
		classifier, ok := detect.Registry()[signal.ReasonCode]
		if !ok || classifier.ID != issue.ClassifierID || signal.ClassifierVersion != issue.ClassifierVersion {
			return domain.EvalCandidate{}, errors.New("signal lineage does not match issue")
		}
		matching = append(matching, signal)
	}
	if len(matching) == 0 {
		return domain.EvalCandidate{}, errors.New("eval export requires classified signals")
	}
	sort.Slice(matching, func(i, j int) bool {
		if matching[i].RunID != matching[j].RunID {
			return matching[i].RunID < matching[j].RunID
		}
		return matching[i].ID < matching[j].ID
	})
	selectedRun := matching[0].RunID
	reason := matching[0].ReasonCode
	fixture := Fixture{Schema: FixtureSchema, Project: matching[0].ProjectID, ReasonCode: reason, ClassifierID: issue.ClassifierID, ClassifierVersion: issue.ClassifierVersion, ExpectedOutcome: reason}
	for _, span := range spans {
		if span.RunID != selectedRun || span.TenantID != matching[0].TenantID {
			continue
		}
		sanitized, _, err := ingest.Sanitize(span)
		if err != nil {
			return domain.EvalCandidate{}, errors.New("selected run failed sanitization")
		}
		fixtureSpan := FixtureSpan{Name: sanitized.Name, Status: sanitized.Status, DurationMillis: sanitized.EndedAt.Sub(sanitized.StartedAt).Milliseconds(), Attributes: relevantAttributes(reason, sanitized.Attributes)}
		if sanitized.Tool != nil {
			fixtureSpan.ToolAuthority = sanitized.Tool.Authority
			fixtureSpan.ToolResult = sanitized.Tool.Result
		}
		fixture.Spans = append(fixture.Spans, fixtureSpan)
	}
	if len(fixture.Spans) == 0 {
		return domain.EvalCandidate{}, errors.New("selected signal run has no spans")
	}
	encoded, err := json.Marshal(fixture)
	if err != nil {
		return domain.EvalCandidate{}, err
	}
	if containsSensitive(encoded) {
		return domain.EvalCandidate{}, errors.New("minimized fixture contains sensitive material")
	}
	evidence := []domain.EvidenceRef{}
	for _, signal := range matching {
		if signal.RunID == selectedRun {
			evidence = append(evidence, signal.Evidence...)
		}
	}
	evidence = deduplicateEvidence(evidence)
	candidate := domain.EvalCandidate{IssueID: issue.ID, ClassifierID: issue.ClassifierID, ClassifierVersion: issue.ClassifierVersion, ReasonCode: reason, Fixture: encoded, ExpectedOutcome: reason, Evidence: evidence, PromotionState: "proposed", CreatedAt: at}
	candidate.ID, _ = domain.StableID("eval", struct {
		Classifier string
		Fixture    json.RawMessage
	}{candidate.ClassifierID, candidate.Fixture})
	return candidate, nil
}

func Promote(candidate domain.EvalCandidate, issue domain.ReliabilityIssue, actor, reason string) (domain.EvalCandidate, error) {
	if candidate.PromotionState != "proposed" || candidate.IssueID != issue.ID || issue.State != domain.IssueConfirmed {
		return candidate, errors.New("only a proposed candidate from a confirmed issue can be promoted")
	}
	if actor == "" || reason == "" {
		return candidate, errors.New("promotion requires actor and reason")
	}
	if candidate.ClassifierVersion != issue.ClassifierVersion || len(candidate.Evidence) == 0 {
		return candidate, errors.New("candidate lineage is incomplete")
	}
	candidate.PromotionState = "promoted"
	candidate.ReviewActor = actor
	candidate.ReviewReason = reason
	return candidate, nil
}

func NewBundle(candidate domain.EvalCandidate, handoff investigate.Handoff, outcomes []Outcome) (Bundle, error) {
	if candidate.PromotionState != "promoted" {
		return Bundle{}, errors.New("bundle requires a promoted candidate")
	}
	for _, outcome := range outcomes {
		if outcome.Kind != "shadow_replay" && outcome.Kind != "release_decision" {
			return Bundle{}, fmt.Errorf("unsupported lineage outcome %s", outcome.Kind)
		}
		if outcome.State == "" || outcome.Evidence == "" || outcome.RecordedAt.IsZero() {
			return Bundle{}, errors.New("lineage outcome is incomplete")
		}
	}
	bundle := Bundle{Schema: "trace-to-eval-bundle-v1", Candidate: candidate, Handoff: handoff, Outcomes: append([]Outcome(nil), outcomes...)}
	bundle.Digest, _ = domain.Digest(struct {
		Candidate domain.EvalCandidate
		Handoff   investigate.Handoff
		Outcomes  []Outcome
	}{candidate, handoff, outcomes})
	return bundle, nil
}

type Catalog struct {
	Path string
	mu   sync.Mutex
}

func (c *Catalog) Put(candidate domain.EvalCandidate) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, err := c.readUnlocked()
	if err != nil {
		return false, err
	}
	for _, item := range existing {
		if item.ID == candidate.ID {
			return false, nil
		}
	}
	file, err := os.OpenFile(c.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return false, err
	}
	encoded, err := json.Marshal(candidate)
	if err == nil {
		_, err = file.Write(append(encoded, '\n'))
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Catalog) ReadAll() ([]domain.EvalCandidate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readUnlocked()
}

func (c *Catalog) readUnlocked() ([]domain.EvalCandidate, error) {
	file, err := os.Open(c.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	result := []domain.EvalCandidate{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var candidate domain.EvalCandidate
		if err := json.Unmarshal(scanner.Bytes(), &candidate); err != nil {
			return nil, errors.New("invalid eval catalog")
		}
		result = append(result, candidate)
	}
	return result, scanner.Err()
}

var packagePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func GenerateGoTest(candidate domain.EvalCandidate, packageName string) ([]byte, error) {
	if !packagePattern.MatchString(packageName) {
		return nil, errors.New("invalid Go package name")
	}
	if candidate.PromotionState != "promoted" {
		return nil, errors.New("generated tests require a promoted candidate")
	}
	source := fmt.Sprintf(`package %s

import (
    "encoding/json"
    "testing"
)

func TestPromotedTraceFailure_%s(t *testing.T) {
    var fixture struct {
        ReasonCode string            `+"`json:\"reason_code\"`"+`
        Spans      []json.RawMessage `+"`json:\"spans\"`"+`
    }
    if err := json.Unmarshal([]byte(%s), &fixture); err != nil {
        t.Fatal(err)
    }
    if fixture.ReasonCode != %s || len(fixture.Spans) == 0 {
        t.Fatalf("reason=%%s spans=%%d", fixture.ReasonCode, len(fixture.Spans))
    }
}
`, packageName, strings.ReplaceAll(candidate.ID, "-", "_"), strconv.Quote(string(candidate.Fixture)), strconv.Quote(candidate.ExpectedOutcome))
	return format.Source([]byte(source))
}

func relevantAttributes(reason string, input map[string]string) map[string]string {
	keys := map[string][]string{detect.ReasonRetry: {"retry.count"}, detect.ReasonLoop: {"loop.count"}, detect.ReasonAbandonment: {"user.cancelled"}, detect.ReasonMalformed: {"output.format"}}
	result := map[string]string{}
	for _, key := range keys[reason] {
		if value, ok := input[key]; ok {
			result[key] = value
		}
	}
	return result
}

func deduplicateEvidence(values []domain.EvidenceRef) []domain.EvidenceRef {
	byID := map[string]domain.EvidenceRef{}
	for _, value := range values {
		byID[value.ID] = value
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]domain.EvidenceRef, 0, len(ids))
	for _, id := range ids {
		result = append(result, byID[id])
	}
	return result
}

func containsSensitive(data []byte) bool {
	lower := strings.ToLower(string(data))
	return strings.Contains(lower, strings.ToLower(corpus.SecretCanary)) || strings.Contains(lower, "authorization") || strings.Contains(lower, "user.email") || strings.Contains(lower, "@")
}
