package governance

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/corpus"
)

func TestTenantQuotaAndRetentionFailClosed(t *testing.T) {
	spans := corpus.Generate().Spans[:4]
	spans[3].TenantID = "other"
	spans[2].RetentionDays = 99
	policy := Policy{AllowedTenants: map[string]bool{"tenant-fictional": true}, MaxSpansPerTenant: 2, MaxRetentionDays: 30}
	result := policy.Admit(spans, nil)
	if len(result.Accepted) != 2 || result.Rejected != 2 || result.Reasons["retention_exceeds_policy"] != 1 || result.Reasons["tenant_not_allowed"] != 1 {
		t.Fatalf("result=%#v", result)
	}
}

func TestDeletionIsTenantScopedAndReceipted(t *testing.T) {
	spans := corpus.Generate().Spans[:2]
	other := spans[0]
	other.TenantID = "other"
	other.SpanID = "other-span"
	spans = append(spans, other)
	at := spans[1].EndedAt.Add(31 * 24 * time.Hour)
	kept, receipt := DeleteExpired(spans, "tenant-fictional", at)
	if len(kept) != 1 || kept[0].TenantID != "other" || receipt.Deleted != 2 || receipt.EvidenceDigest == "" {
		t.Fatalf("kept=%#v receipt=%#v", kept, receipt)
	}
}

func TestSignedDigestDetectsTampering(t *testing.T) {
	seed := sha256.Sum256([]byte("fixture-only-signing-key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	report := map[string]any{"passed": true, "spans": 5000}
	signed, err := Sign(report, private)
	if err != nil || Verify(report, signed) != nil {
		t.Fatalf("signed=%#v err=%v", signed, err)
	}
	report["spans"] = 4999
	if err := Verify(report, signed); err == nil {
		t.Fatal("tampered report verified")
	}
}
