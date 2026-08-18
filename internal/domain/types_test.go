package domain

import (
	"testing"
	"time"
)

func TestStableIDIsDeterministic(t *testing.T) {
	first, err := StableID("span", map[string]string{"b": "2", "a": "1"})
	if err != nil {
		t.Fatal(err)
	}
	second, _ := StableID("span", map[string]string{"a": "1", "b": "2"})
	if first != second {
		t.Fatalf("first=%s second=%s", first, second)
	}
}

func TestSpanValidation(t *testing.T) {
	now := time.Now().UTC()
	span := Span{Schema: SchemaVersion, TenantID: "tenant", ProjectID: "project", RunID: "run", TraceID: "trace", SpanID: "span", Name: "agent", StartedAt: now, EndedAt: now.Add(time.Second), Status: StatusOK, Privacy: PrivacyInternal, RetentionDays: 7}
	if err := span.Validate(); err != nil {
		t.Fatal(err)
	}
	span.EndedAt = now.Add(-time.Second)
	if err := span.Validate(); err == nil {
		t.Fatal("expected invalid timing")
	}
}

func TestReleasePacketCurrentUsesTTL(t *testing.T) {
	now := time.Now().UTC()
	packet := ReleasePacket{ID: "release", DeployedAt: now, ExpiresAt: now.Add(time.Hour)}
	if !packet.Current(now.Add(time.Minute)) || packet.Current(now.Add(2*time.Hour)) {
		t.Fatal("release TTL was not enforced")
	}
}
