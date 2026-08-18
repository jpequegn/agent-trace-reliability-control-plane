package investigate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/detect"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

const PacketTTL = 15 * time.Minute

type RouteRequest struct {
	Issue                 domain.ReliabilityIssue
	Signals               []domain.Signal
	Release               *domain.ReleasePacket
	RequireReleaseContext bool
	At                    time.Time
}

type Decision struct {
	State  domain.WatchState           `json:"state"`
	Reason string                      `json:"reason"`
	Plan   *domain.TemporaryWatchPlan  `json:"watch_plan,omitempty"`
	Packet *domain.InvestigationPacket `json:"investigation_packet,omitempty"`
}

type Finding struct {
	Statement       string   `json:"statement"`
	EvidenceFor     []string `json:"evidence_for"`
	EvidenceAgainst []string `json:"evidence_against,omitempty"`
}

type Result struct {
	Hypotheses     []Finding `json:"hypotheses"`
	LikelyFixLayer string    `json:"likely_fix_layer,omitempty"`
	Abstained      bool      `json:"abstained"`
	AbstainReason  string    `json:"abstain_reason,omitempty"`
}

type Handoff struct {
	ID                       string               `json:"id"`
	IssueID                  string               `json:"issue_id"`
	Owner                    string               `json:"owner"`
	State                    string               `json:"state"`
	DeterministicReasonCodes []string             `json:"deterministic_reason_codes"`
	Evidence                 []domain.EvidenceRef `json:"evidence"`
	Investigation            *Result              `json:"investigation,omitempty"`
	InvestigatorUnavailable  bool                 `json:"investigator_unavailable"`
	DraftOnly                bool                 `json:"draft_only"`
	CreatedAt                time.Time            `json:"created_at"`
}

type Investigator interface {
	Investigate(context.Context, domain.InvestigationPacket) (Result, error)
}

type Router struct {
	MaxEvidence    int
	MaxOutputBytes int
}

func DefaultRouter() Router {
	return Router{MaxEvidence: 12, MaxOutputBytes: 4096}
}

func (r Router) Route(request RouteRequest) (Decision, error) {
	if request.At.IsZero() || request.Issue.State != domain.IssueConfirmed || request.Issue.ReviewActor == "" {
		return Decision{}, errors.New("investigation requires a human-confirmed issue and timestamp")
	}
	if len(request.Signals) == 0 {
		return Decision{}, errors.New("investigation requires deterministic signals")
	}
	evidence := []domain.EvidenceRef{}
	reasons := map[string]bool{}
	first, last := request.Signals[0].FirstSeen, request.Signals[0].LastSeen
	for _, signal := range request.Signals {
		if err := detect.ValidateClassifierSignal(signal); err != nil {
			return Decision{}, err
		}
		if signal.LastSeen.After(request.At) {
			return Decision{}, errors.New("signal evidence is from the future")
		}
		if signal.ClassifierVersion != request.Issue.ClassifierVersion {
			return Decision{}, errors.New("signal and issue classifier versions differ")
		}
		reasons[signal.ReasonCode] = true
		evidence = append(evidence, signal.Evidence...)
		if signal.FirstSeen.Before(first) {
			first = signal.FirstSeen
		}
		if signal.LastSeen.After(last) {
			last = signal.LastSeen
		}
	}
	if request.RequireReleaseContext {
		if request.Release == nil {
			return Decision{State: domain.WatchMappingUncertain, Reason: "release_context_missing"}, nil
		}
		release := *request.Release
		if !release.Current(request.At) {
			return Decision{State: domain.WatchMappingUncertain, Reason: "release_context_expired"}, nil
		}
		if release.Service != request.Signals[0].ProjectID || release.Owner == "" || !containsAny(release.ExpectedSignals, reasons) {
			return Decision{State: domain.WatchMappingUncertain, Reason: "release_mapping_conflicting"}, nil
		}
		if first.Before(release.DeployedAt) || last.After(release.ExpiresAt) {
			return Decision{State: domain.WatchMappingUncertain, Reason: "signal_outside_release_window"}, nil
		}
	}
	if r.MaxEvidence <= 0 {
		r.MaxEvidence = 12
	}
	if r.MaxOutputBytes <= 0 {
		r.MaxOutputBytes = 4096
	}
	evidence = deduplicateEvidence(evidence)
	if len(evidence) > r.MaxEvidence {
		evidence = evidence[:r.MaxEvidence]
	}
	ruleIDs := make([]string, 0, len(reasons))
	for reason := range reasons {
		ruleIDs = append(ruleIDs, "rule."+reason)
	}
	sort.Strings(ruleIDs)
	allowedReads := []string{"trace.read"}
	releaseID := ""
	if request.Release != nil {
		releaseID = request.Release.ID
		allowedReads = intersectReadTools(request.Release.AllowedReadTools)
	}
	plan := domain.TemporaryWatchPlan{ReleaseID: releaseID, State: domain.WatchApproved, Service: request.Signals[0].ProjectID, RuleIDs: ruleIDs, AllowedReads: allowedReads, StartsAt: first, ExpiresAt: request.At.Add(PacketTTL), MaxEvidence: r.MaxEvidence, MaxDuration: PacketTTL, ApprovalActor: request.Issue.ReviewActor, ApprovalReason: request.Issue.ReviewReason}
	plan.ID, _ = domain.StableID("watch", struct {
		Issue, Release string
		Rules          []string
	}{request.Issue.ID, releaseID, ruleIDs})
	packet := domain.InvestigationPacket{IssueID: request.Issue.ID, Hypothesis: request.Issue.Hypothesis, Evidence: evidence, AllowedTools: allowedReads, MaxEvidence: r.MaxEvidence, MaxOutputBytes: r.MaxOutputBytes, ExpiresAt: plan.ExpiresAt}
	packet.ID, _ = domain.StableID("investigation", struct {
		Issue    string
		Evidence []domain.EvidenceRef
	}{request.Issue.ID, evidence})
	return Decision{State: domain.WatchApproved, Reason: "deterministic_trigger_confirmed", Plan: &plan, Packet: &packet}, nil
}

type Service struct {
	Router       Router
	Investigator Investigator
}

func (s Service) Execute(ctx context.Context, request RouteRequest) (Decision, Handoff, error) {
	decision, err := s.Router.Route(request)
	if err != nil || decision.State != domain.WatchApproved {
		return decision, Handoff{}, err
	}
	reasons := []string{}
	for _, signal := range request.Signals {
		reasons = append(reasons, signal.ReasonCode)
	}
	sort.Strings(reasons)
	handoff := Handoff{IssueID: request.Issue.ID, Owner: request.Issue.Owner, State: "deterministic_only", DeterministicReasonCodes: unique(reasons), Evidence: append([]domain.EvidenceRef(nil), decision.Packet.Evidence...), DraftOnly: true, CreatedAt: request.At}
	if s.Investigator == nil {
		handoff.InvestigatorUnavailable = true
		handoff.ID, _ = domain.StableID("handoff", struct{ Issue, State string }{handoff.IssueID, handoff.State})
		return decision, handoff, nil
	}
	result, err := s.Investigator.Investigate(ctx, *decision.Packet)
	if err != nil {
		handoff.InvestigatorUnavailable = true
		handoff.ID, _ = domain.StableID("handoff", struct{ Issue, State string }{handoff.IssueID, handoff.State})
		return decision, handoff, nil
	}
	if err := validateResult(result, *decision.Packet); err != nil {
		return decision, Handoff{}, err
	}
	handoff.State = "investigated_draft"
	handoff.Investigation = &result
	handoff.ID, _ = domain.StableID("handoff", struct {
		Issue  string
		Result Result
	}{handoff.IssueID, result})
	return decision, handoff, nil
}

func validateResult(result Result, packet domain.InvestigationPacket) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if len(encoded) > packet.MaxOutputBytes {
		return errors.New("investigator output exceeds budget")
	}
	allowed := map[string]bool{}
	for _, evidence := range packet.Evidence {
		allowed[evidence.ID] = true
	}
	for _, hypothesis := range result.Hypotheses {
		if hypothesis.Statement == "" {
			return errors.New("investigator hypothesis is empty")
		}
		for _, evidenceID := range append(hypothesis.EvidenceFor, hypothesis.EvidenceAgainst...) {
			if !allowed[evidenceID] {
				return fmt.Errorf("investigator referenced evidence outside packet")
			}
		}
	}
	if result.Abstained && result.AbstainReason == "" {
		return errors.New("investigator abstention requires a reason")
	}
	return nil
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

func containsAny(expected []string, actual map[string]bool) bool {
	for _, value := range expected {
		if actual[value] {
			return true
		}
	}
	return false
}

func intersectReadTools(values []string) []string {
	result := []string{}
	for _, value := range values {
		if value == "trace.read" || value == "metrics.read" || value == "release.read" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return unique(result)
}

func unique(values []string) []string {
	result := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
