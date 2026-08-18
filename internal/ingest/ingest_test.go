package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func kv(key, value string) *common.KeyValue {
	return &common.KeyValue{Key: key, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: value}}}
}

func otlpRequest() *collectortrace.ExportTraceServiceRequest {
	start := uint64(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	resourceAttributes := []*common.KeyValue{kv("tenant.id", "tenant-fictional"), kv("project.id", "agent-lab"), kv("run.id", "run-otlp"), kv("session.id", "session-1"), kv("user.id", "user-1"), kv("cohort.id", "cohort-1"), kv("privacy.class", "internal"), kv("retention.days", "30"), kv("version.harness", "harness-v1")}
	span := &trace.Span{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8), Name: "tool.search", StartTimeUnixNano: start, EndTimeUnixNano: start + uint64(time.Second), Status: &trace.Status{Code: trace.Status_STATUS_CODE_OK}, Attributes: []*common.KeyValue{kv("tool.intent", "search fixtures"), kv("tool.authority", "read_only"), kv("tool.result", "ok"), kv("retry.count", "0"), kv("authorization", corpus.SecretCanary), kv("user.email", "fictional@example.test")}}
	return &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*trace.ResourceSpans{{Resource: &resource.Resource{Attributes: resourceAttributes}, ScopeSpans: []*trace.ScopeSpans{{Spans: []*trace.Span{span}}}}}}
}

func newHandler(t *testing.T) (Handler, *Store) {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "traces.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return Handler{Ingestor: New(store, 100, 2)}, store
}

func send(t *testing.T, handler http.Handler, contentType string, message *collectortrace.ExportTraceServiceRequest) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	var err error
	if contentType == "application/json" {
		payload, err = protojson.Marshal(message)
	} else {
		payload, err = proto.Marshal(message)
	}
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(payload))
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestHandlerAcceptsJSONAndProtobufAndDeduplicates(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/x-protobuf"} {
		t.Run(contentType, func(t *testing.T) {
			handler, store := newHandler(t)
			first := send(t, handler, contentType, otlpRequest())
			second := send(t, handler, contentType, otlpRequest())
			if first.Code != http.StatusOK || second.Code != http.StatusOK {
				t.Fatalf("first=%d second=%d", first.Code, second.Code)
			}
			spans, err := store.ReadAll()
			if err != nil || len(spans) != 1 {
				t.Fatalf("spans=%d err=%v", len(spans), err)
			}
			stored, _ := os.ReadFile(store.Path)
			if bytes.Contains(stored, []byte(corpus.SecretCanary)) || bytes.Contains(stored, []byte("fictional@example.test")) {
				t.Fatalf("sensitive value reached storage: %s", stored)
			}
			if spans[0].Tool == nil || spans[0].Tool.Authority != "read_only" || spans[0].Attributes["retry.count"] != "0" {
				t.Fatalf("normalized span=%#v", spans[0])
			}
		})
	}
}

func TestPartialAcceptanceUsesFixedReasonCodes(t *testing.T) {
	handler, store := newHandler(t)
	message := otlpRequest()
	invalid := proto.Clone(message.ResourceSpans[0].ScopeSpans[0].Spans[0]).(*trace.Span)
	invalid.SpanId = bytes.Repeat([]byte{3}, 8)
	invalid.Attributes = append(invalid.Attributes, kv("run.id", ""))
	message.ResourceSpans[0].ScopeSpans[0].Spans = append(message.ResourceSpans[0].ScopeSpans[0].Spans, invalid)
	response := send(t, handler, "application/json", message)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "partial_invalid_spans") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	spans, _ := store.ReadAll()
	if len(spans) != 1 {
		t.Fatalf("stored=%d", len(spans))
	}
}

func TestOutOfOrderSpansReconstructChronologically(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "traces.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	data := corpus.Generate()
	first := data.Spans[0]
	second := data.Spans[1]
	ingestor := New(store, 10, 1)
	result, err := ingestor.Ingest(context.Background(), []domain.Span{second, first})
	if err != nil || result.Accepted != 2 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	spans, err := store.ReadAll()
	if err != nil || spans[0].SpanID != first.SpanID || spans[1].SpanID != second.SpanID {
		t.Fatalf("spans=%#v err=%v", spans, err)
	}
}

func TestBatchLimitAndMalformedPayload(t *testing.T) {
	handler, store := newHandler(t)
	handler.Ingestor.MaxBatch = 0
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader("not protobuf"))
	request.Header.Set("Content-Type", "application/x-protobuf")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || strings.Contains(response.Body.String(), "protobuf:") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := store.ReadAll(); err != nil {
		t.Fatal(err)
	}
}

func FuzzSanitizeNeverReturnsSecretCanary(f *testing.F) {
	f.Add("safe")
	f.Add(corpus.SecretCanary)
	f.Fuzz(func(t *testing.T, value string) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		span := domain.Span{Schema: domain.SchemaVersion, TenantID: "tenant", ProjectID: "project", RunID: "run", TraceID: "trace", SpanID: "span", Name: "agent.run", StartedAt: now, EndedAt: now.Add(time.Second), Status: domain.StatusOK, Attributes: map[string]string{"retry.count": value}, Privacy: domain.PrivacyInternal, RetentionDays: 1}
		sanitized, _, err := Sanitize(span)
		if err != nil {
			return
		}
		encoded, err := json.Marshal(sanitized)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte(corpus.SecretCanary)) {
			t.Fatal("secret canary survived sanitization")
		}
	})
}
