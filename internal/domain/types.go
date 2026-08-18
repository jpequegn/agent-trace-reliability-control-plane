package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const SchemaVersion = "trace-control-v1"

type SpanStatus string

const (
	StatusUnset SpanStatus = "unset"
	StatusOK    SpanStatus = "ok"
	StatusError SpanStatus = "error"
)

type PrivacyClass string

const (
	PrivacyPublic     PrivacyClass = "public"
	PrivacyInternal   PrivacyClass = "internal"
	PrivacyRestricted PrivacyClass = "restricted"
)

type VersionSet struct {
	Model   string `json:"model"`
	Prompt  string `json:"prompt"`
	Tools   string `json:"tools"`
	Policy  string `json:"policy"`
	Harness string `json:"harness"`
	Memory  string `json:"memory"`
}

type ToolCall struct {
	Name      string `json:"name"`
	Intent    string `json:"intent"`
	Authority string `json:"authority"`
	Result    string `json:"result"`
}

type Span struct {
	Schema        string            `json:"schema_version"`
	TenantID      string            `json:"tenant_id"`
	ProjectID     string            `json:"project_id"`
	RunID         string            `json:"run_id"`
	SessionID     string            `json:"session_id"`
	UserID        string            `json:"user_id"`
	CohortID      string            `json:"cohort_id"`
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind"`
	StartedAt     time.Time         `json:"started_at"`
	EndedAt       time.Time         `json:"ended_at"`
	Status        SpanStatus        `json:"status"`
	StatusMessage string            `json:"status_message,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Tool          *ToolCall         `json:"tool,omitempty"`
	Versions      VersionSet        `json:"versions"`
	CostMicros    int64             `json:"cost_micros"`
	Privacy       PrivacyClass      `json:"privacy_class"`
	RetentionDays int               `json:"retention_days"`
}

func (s Span) Validate() error {
	if s.Schema != SchemaVersion || s.TenantID == "" || s.ProjectID == "" || s.RunID == "" || s.TraceID == "" || s.SpanID == "" {
		return errors.New("span requires schema, tenant, project, run, trace, and span identity")
	}
	if s.Name == "" || s.StartedAt.IsZero() || s.EndedAt.Before(s.StartedAt) {
		return errors.New("span requires a name and valid timing")
	}
	if s.Status != StatusUnset && s.Status != StatusOK && s.Status != StatusError {
		return fmt.Errorf("unsupported span status %q", s.Status)
	}
	if s.Privacy == "" || s.RetentionDays <= 0 {
		return errors.New("span requires privacy and positive retention")
	}
	if s.Tool != nil && (s.Tool.Name == "" || s.Tool.Intent == "" || s.Tool.Authority == "" || s.Tool.Result == "") {
		return errors.New("tool spans require name, intent, authority, and result")
	}
	return nil
}

type ReleasePacket struct {
	ID               string    `json:"id"`
	Service          string    `json:"service"`
	Version          string    `json:"version"`
	Owner            string    `json:"owner"`
	DeployedAt       time.Time `json:"deployed_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	ExpectedSignals  []string  `json:"expected_signals"`
	AllowedReadTools []string  `json:"allowed_read_tools"`
	BaselineCohort   string    `json:"baseline_cohort"`
	RollbackRef      string    `json:"rollback_ref"`
	EvidenceDigest   string    `json:"evidence_digest"`
}

func (p ReleasePacket) Current(at time.Time) bool {
	return p.ID != "" && !at.Before(p.DeployedAt) && at.Before(p.ExpiresAt)
}

type WatchState string

const (
	WatchProposed         WatchState = "proposed"
	WatchApproved         WatchState = "approved"
	WatchMappingUncertain WatchState = "mapping_uncertain"
	WatchExpired          WatchState = "expired"
)

type TemporaryWatchPlan struct {
	ID             string        `json:"id"`
	ReleaseID      string        `json:"release_id"`
	State          WatchState    `json:"state"`
	Service        string        `json:"service"`
	RuleIDs        []string      `json:"rule_ids"`
	AllowedReads   []string      `json:"allowed_reads"`
	StartsAt       time.Time     `json:"starts_at"`
	ExpiresAt      time.Time     `json:"expires_at"`
	MaxEvidence    int           `json:"max_evidence"`
	MaxDuration    time.Duration `json:"max_duration"`
	ApprovalActor  string        `json:"approval_actor,omitempty"`
	ApprovalReason string        `json:"approval_reason,omitempty"`
}

type SignalState string

const (
	SignalDetected             SignalState = "detected"
	SignalUnknown              SignalState = "unknown"
	SignalInstrumentationDrift SignalState = "instrumentation_drift"
)

type EvidenceRef struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Digest     string    `json:"digest"`
	RecordedAt time.Time `json:"recorded_at"`
}

type Signal struct {
	ID                string        `json:"id"`
	RuleID            string        `json:"rule_id"`
	RuleVersion       string        `json:"rule_version"`
	ReasonCode        string        `json:"reason_code"`
	State             SignalState   `json:"state"`
	TenantID          string        `json:"tenant_id"`
	ProjectID         string        `json:"project_id"`
	RunID             string        `json:"run_id"`
	CohortID          string        `json:"cohort_id"`
	FirstSeen         time.Time     `json:"first_seen"`
	LastSeen          time.Time     `json:"last_seen"`
	Severity          int           `json:"severity"`
	Evidence          []EvidenceRef `json:"evidence"`
	ClassifierVersion string        `json:"classifier_version"`
}

type Classifier struct {
	ID                 string   `json:"id"`
	Version            string   `json:"version"`
	ReasonCode         string   `json:"reason_code"`
	Description        string   `json:"description"`
	MinimumVolume      int      `json:"minimum_volume"`
	Threshold          float64  `json:"threshold"`
	RequiredAttributes []string `json:"required_attributes"`
	Owner              string   `json:"owner"`
}

type Impact struct {
	FirstSeen        time.Time          `json:"first_seen"`
	LastSeen         time.Time          `json:"last_seen"`
	AffectedRuns     int                `json:"affected_runs"`
	TotalRuns        int                `json:"total_runs"`
	AffectedUsers    int                `json:"affected_users"`
	TotalUsers       int                `json:"total_users"`
	AffectedRunRate  float64            `json:"affected_run_rate"`
	AffectedUserRate float64            `json:"affected_user_rate"`
	ByCohort         map[string]float64 `json:"by_cohort"`
	SeverityWeighted float64            `json:"severity_weighted_impact"`
}

type IssueState string

const (
	IssueProposed   IssueState = "proposed"
	IssueConfirmed  IssueState = "confirmed"
	IssueMitigated  IssueState = "mitigated"
	IssueResolved   IssueState = "resolved"
	IssueRejected   IssueState = "rejected"
	IssueSuppressed IssueState = "suppressed"
)

type ReliabilityIssue struct {
	ID                  string     `json:"id"`
	ClassifierID        string     `json:"classifier_id"`
	ClassifierVersion   string     `json:"classifier_version"`
	State               IssueState `json:"state"`
	Owner               string     `json:"owner"`
	Severity            int        `json:"severity"`
	Hypothesis          string     `json:"hypothesis"`
	Impact              Impact     `json:"impact"`
	Signals             []string   `json:"signal_ids"`
	KnownFalsePositives []string   `json:"known_false_positives,omitempty"`
	ReviewActor         string     `json:"review_actor,omitempty"`
	ReviewReason        string     `json:"review_reason,omitempty"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

type InvestigationPacket struct {
	ID             string        `json:"id"`
	IssueID        string        `json:"issue_id"`
	Hypothesis     string        `json:"hypothesis"`
	Evidence       []EvidenceRef `json:"evidence"`
	AllowedTools   []string      `json:"allowed_tools"`
	MaxEvidence    int           `json:"max_evidence"`
	MaxOutputBytes int           `json:"max_output_bytes"`
	ExpiresAt      time.Time     `json:"expires_at"`
}

type EvalCandidate struct {
	ID                string          `json:"id"`
	IssueID           string          `json:"issue_id"`
	ClassifierID      string          `json:"classifier_id"`
	ClassifierVersion string          `json:"classifier_version"`
	ReasonCode        string          `json:"reason_code"`
	Fixture           json.RawMessage `json:"fixture"`
	ExpectedOutcome   string          `json:"expected_outcome"`
	Evidence          []EvidenceRef   `json:"evidence"`
	PromotionState    string          `json:"promotion_state"`
	ReviewActor       string          `json:"review_actor,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
}

func Digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func StableID(prefix string, value any) (string, error) {
	if prefix == "" {
		return "", errors.New("stable ID prefix is required")
	}
	digest, err := Digest(value)
	if err != nil {
		return "", err
	}
	return prefix + "_" + digest[:24], nil
}
