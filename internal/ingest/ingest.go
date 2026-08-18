package ingest

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const DefaultMaxBatch = 10_000

var allowedAttributes = map[string]bool{
	"agent.workflow": true,
	"output.format":  true,
	"retry.count":    true,
	"loop.count":     true,
	"user.cancelled": true,
}

var identityKeys = []string{"tenant.id", "project.id", "run.id", "session.id", "user.id", "cohort.id"}

type Rejection struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

type Result struct {
	Received   int         `json:"received"`
	Accepted   int         `json:"accepted"`
	Duplicates int         `json:"duplicates"`
	Redacted   int         `json:"redacted_fields"`
	Rejected   []Rejection `json:"rejected,omitempty"`
}

type Store struct {
	Path string
	mu   sync.Mutex
	seen map[string]bool
}

func NewStore(path string) (*Store, error) {
	store := &Store{Path: path, seen: map[string]bool{}}
	spans, err := store.readUnlocked()
	if err != nil {
		return nil, err
	}
	for _, span := range spans {
		store.seen[span.TenantID+"/"+span.SpanID] = true
	}
	return store, nil
}

func (s *Store) Append(span domain.Span) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := span.TenantID + "/" + span.SpanID
	if s.seen[key] {
		return false, nil
	}
	file, err := os.OpenFile(s.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return false, fmt.Errorf("open trace store: %w", err)
	}
	encoded, err := json.Marshal(span)
	if err == nil {
		_, err = file.Write(append(encoded, '\n'))
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, fmt.Errorf("append trace store: %w", err)
	}
	s.seen[key] = true
	return true, nil
}

func (s *Store) ReadAll() ([]domain.Span, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readUnlocked()
}

func (s *Store) readUnlocked() ([]domain.Span, error) {
	file, err := os.Open(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open trace store: %w", err)
	}
	defer file.Close()
	spans := []domain.Span{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var span domain.Span
		if err := json.Unmarshal(scanner.Bytes(), &span); err != nil {
			return nil, errors.New("decode sanitized trace store")
		}
		if err := span.Validate(); err != nil {
			return nil, errors.New("invalid sanitized trace store")
		}
		spans = append(spans, span)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan trace store: %w", err)
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].RunID != spans[j].RunID {
			return spans[i].RunID < spans[j].RunID
		}
		if !spans[i].StartedAt.Equal(spans[j].StartedAt) {
			return spans[i].StartedAt.Before(spans[j].StartedAt)
		}
		return spans[i].SpanID < spans[j].SpanID
	})
	return spans, nil
}

type Ingestor struct {
	Store    Sink
	MaxBatch int
	slots    chan struct{}
}

type Sink interface {
	Append(domain.Span) (bool, error)
}

func New(store Sink, maxBatch, concurrency int) *Ingestor {
	if maxBatch <= 0 {
		maxBatch = DefaultMaxBatch
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Ingestor{Store: store, MaxBatch: maxBatch, slots: make(chan struct{}, concurrency)}
}

func (i *Ingestor) Ingest(ctx context.Context, spans []domain.Span) (Result, error) {
	result := Result{Received: len(spans)}
	if len(spans) > i.MaxBatch {
		return result, errors.New("batch_limit_exceeded")
	}
	select {
	case i.slots <- struct{}{}:
		defer func() { <-i.slots }()
	case <-ctx.Done():
		return result, errors.New("backpressure_timeout")
	}
	for index, input := range spans {
		sanitized, redacted, err := Sanitize(input)
		result.Redacted += redacted
		if err != nil {
			result.Rejected = append(result.Rejected, Rejection{Index: index, Reason: "invalid_or_sensitive_span"})
			continue
		}
		appended, err := i.Store.Append(sanitized)
		if err != nil {
			return result, err
		}
		if appended {
			result.Accepted++
		} else {
			result.Duplicates++
		}
	}
	return result, nil
}

func Sanitize(input domain.Span) (domain.Span, int, error) {
	for _, value := range []string{input.TenantID, input.ProjectID, input.RunID, input.SessionID, input.UserID, input.CohortID, input.TraceID, input.SpanID, input.ParentSpanID, input.Name} {
		if unsafe(value) {
			return domain.Span{}, 0, errors.New("unsafe identity")
		}
	}
	for _, value := range []string{input.Versions.Model, input.Versions.Prompt, input.Versions.Tools, input.Versions.Policy, input.Versions.Harness, input.Versions.Memory} {
		if unsafe(value) {
			return domain.Span{}, 0, errors.New("unsafe version metadata")
		}
	}
	redacted := 0
	attributes := map[string]string{}
	for key, value := range input.Attributes {
		if !allowedAttributes[key] {
			redacted++
			continue
		}
		if unsafe(value) || len(value) > 256 {
			attributes[key] = "[REDACTED]"
			redacted++
			continue
		}
		attributes[key] = value
	}
	input.Attributes = attributes
	input.StatusMessage = fixedStatusMessage(input.StatusMessage)
	if input.Tool != nil {
		if unsafe(input.Tool.Name) || unsafe(input.Tool.Intent) || unsafe(input.Tool.Authority) || unsafe(input.Tool.Result) {
			return domain.Span{}, redacted, errors.New("unsafe tool metadata")
		}
	}
	if err := input.Validate(); err != nil {
		return domain.Span{}, redacted, err
	}
	return input, redacted, nil
}

func fixedStatusMessage(value string) string {
	lower := strings.ToLower(value)
	switch {
	case strings.Contains(lower, "permission") || strings.Contains(lower, "denied"):
		return "permission denied"
	case strings.Contains(lower, "timeout"):
		return "timeout"
	case strings.Contains(lower, "error") || strings.Contains(lower, "failed"):
		return "tool execution failed"
	default:
		return ""
	}
}

func unsafe(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "canary") || strings.Contains(lower, "bearer ") || strings.Contains(lower, "sk-live-") || strings.Contains(lower, "@") || len(value) > 512
}

func Normalize(request *collectortrace.ExportTraceServiceRequest) ([]domain.Span, []Rejection) {
	spans := []domain.Span{}
	rejected := []Rejection{}
	index := 0
	for _, resourceSpans := range request.ResourceSpans {
		resource := attributes(resourceSpans.GetResource().GetAttributes())
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, input := range scopeSpans.Spans {
				span, err := normalizeSpan(input, resource)
				if err != nil {
					rejected = append(rejected, Rejection{Index: index, Reason: "normalization_failed"})
				} else {
					spans = append(spans, span)
				}
				index++
			}
		}
	}
	return spans, rejected
}

func normalizeSpan(input *trace.Span, resource map[string]string) (domain.Span, error) {
	attrs := clone(resource)
	for key, value := range attributes(input.Attributes) {
		attrs[key] = value
	}
	for _, key := range identityKeys {
		if attrs[key] == "" {
			return domain.Span{}, errors.New("missing required identity")
		}
	}
	started := time.Unix(0, int64(input.StartTimeUnixNano)).UTC()
	ended := time.Unix(0, int64(input.EndTimeUnixNano)).UTC()
	status := domain.StatusUnset
	if input.GetStatus().GetCode() == trace.Status_STATUS_CODE_OK {
		status = domain.StatusOK
	}
	if input.GetStatus().GetCode() == trace.Status_STATUS_CODE_ERROR {
		status = domain.StatusError
	}
	span := domain.Span{Schema: domain.SchemaVersion, TenantID: attrs["tenant.id"], ProjectID: attrs["project.id"], RunID: attrs["run.id"], SessionID: attrs["session.id"], UserID: attrs["user.id"], CohortID: attrs["cohort.id"], TraceID: hex.EncodeToString(input.TraceId), SpanID: hex.EncodeToString(input.SpanId), ParentSpanID: hex.EncodeToString(input.ParentSpanId), Name: input.Name, Kind: input.Kind.String(), StartedAt: started, EndedAt: ended, Status: status, StatusMessage: input.GetStatus().GetMessage(), Attributes: attrs, Versions: domain.VersionSet{Model: attrs["version.model"], Prompt: attrs["version.prompt"], Tools: attrs["version.tools"], Policy: attrs["version.policy"], Harness: attrs["version.harness"], Memory: attrs["version.memory"]}, CostMicros: parseInt(attrs["cost.micros"]), Privacy: domain.PrivacyClass(defaultValue(attrs["privacy.class"], string(domain.PrivacyInternal))), RetentionDays: int(parseInt(defaultValue(attrs["retention.days"], "30")))}
	if strings.HasPrefix(input.Name, "tool.") {
		span.Tool = &domain.ToolCall{Name: strings.TrimPrefix(input.Name, "tool."), Intent: defaultValue(attrs["tool.intent"], "unspecified"), Authority: defaultValue(attrs["tool.authority"], "read_only"), Result: defaultValue(attrs["tool.result"], "unknown")}
	}
	return span, nil
}

type Handler struct {
	Ingestor *Ingestor
	MaxBytes int64
}

func (h Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != "/v1/traces" {
		http.Error(response, "route_not_found", http.StatusNotFound)
		return
	}
	maxBytes := h.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 8 << 20
	}
	body := http.MaxBytesReader(response, request.Body, maxBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(response, "request_too_large", http.StatusRequestEntityTooLarge)
		return
	}
	message := &collectortrace.ExportTraceServiceRequest{}
	contentType := strings.Split(request.Header.Get("Content-Type"), ";")[0]
	switch contentType {
	case "application/x-protobuf", "application/protobuf":
		err = proto.Unmarshal(data, message)
	case "application/json":
		err = protojson.Unmarshal(data, message)
	default:
		http.Error(response, "unsupported_content_type", http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		http.Error(response, "invalid_otlp_payload", http.StatusBadRequest)
		return
	}
	spans, normalizationRejected := Normalize(message)
	result, err := h.Ingestor.Ingest(request.Context(), spans)
	result.Received += len(normalizationRejected)
	result.Rejected = append(normalizationRejected, result.Rejected...)
	if err != nil {
		status := http.StatusServiceUnavailable
		if err.Error() == "batch_limit_exceeded" {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(response, err.Error(), status)
		return
	}
	partial := &collectortrace.ExportTracePartialSuccess{RejectedSpans: int64(len(result.Rejected)), ErrorMessage: rejectionSummary(result)}
	output := &collectortrace.ExportTraceServiceResponse{PartialSuccess: partial}
	response.Header().Set("Content-Type", contentType)
	response.WriteHeader(http.StatusOK)
	if contentType == "application/json" {
		encoded, _ := protojson.Marshal(output)
		_, _ = response.Write(encoded)
	} else {
		encoded, _ := proto.Marshal(output)
		_, _ = response.Write(encoded)
	}
}

func rejectionSummary(result Result) string {
	parts := []string{}
	if len(result.Rejected) > 0 {
		parts = append(parts, "partial_invalid_spans")
	}
	if result.Redacted > 0 {
		parts = append(parts, "fields_redacted")
	}
	if result.Duplicates > 0 {
		parts = append(parts, "duplicates_ignored")
	}
	return strings.Join(parts, ",")
}

func attributes(values []*common.KeyValue) map[string]string {
	result := map[string]string{}
	for _, value := range values {
		if value == nil || value.Value == nil {
			continue
		}
		switch typed := value.Value.Value.(type) {
		case *common.AnyValue_StringValue:
			result[value.Key] = typed.StringValue
		case *common.AnyValue_IntValue:
			result[value.Key] = strconv.FormatInt(typed.IntValue, 10)
		case *common.AnyValue_BoolValue:
			result[value.Key] = strconv.FormatBool(typed.BoolValue)
		case *common.AnyValue_DoubleValue:
			result[value.Key] = strconv.FormatFloat(typed.DoubleValue, 'g', -1, 64)
		}
	}
	return result
}

func clone(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func parseInt(value string) int64 {
	result, _ := strconv.ParseInt(value, 10, 64)
	return result
}

func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
