package governance

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jpequegn/agent-trace-reliability-control-plane/internal/domain"
)

type Policy struct {
	AllowedTenants    map[string]bool `json:"allowed_tenants"`
	MaxSpansPerTenant int             `json:"max_spans_per_tenant"`
	MaxRetentionDays  int             `json:"max_retention_days"`
}

type Admission struct {
	Accepted []domain.Span  `json:"accepted"`
	Rejected int            `json:"rejected"`
	Reasons  map[string]int `json:"reasons"`
}

func (p Policy) Admit(spans []domain.Span, current map[string]int) Admission {
	result := Admission{Reasons: map[string]int{}}
	counts := map[string]int{}
	for tenant, count := range current {
		counts[tenant] = count
	}
	for _, span := range spans {
		reason := ""
		switch {
		case !p.AllowedTenants[span.TenantID]:
			reason = "tenant_not_allowed"
		case span.RetentionDays > p.MaxRetentionDays:
			reason = "retention_exceeds_policy"
		case counts[span.TenantID] >= p.MaxSpansPerTenant:
			reason = "tenant_quota_exceeded"
		}
		if reason != "" {
			result.Rejected++
			result.Reasons[reason]++
			continue
		}
		counts[span.TenantID]++
		result.Accepted = append(result.Accepted, span)
	}
	return result
}

type DeletionReceipt struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	Deleted        int       `json:"deleted"`
	Cutoff         time.Time `json:"cutoff"`
	EvidenceDigest string    `json:"evidence_digest"`
	GeneratedAt    time.Time `json:"generated_at"`
}

func DeleteExpired(spans []domain.Span, tenant string, at time.Time) ([]domain.Span, DeletionReceipt) {
	kept := make([]domain.Span, 0, len(spans))
	deletedIDs := []string{}
	for _, span := range spans {
		expires := span.EndedAt.Add(time.Duration(span.RetentionDays) * 24 * time.Hour)
		if span.TenantID == tenant && !expires.After(at) {
			deletedIDs = append(deletedIDs, span.SpanID)
			continue
		}
		kept = append(kept, span)
	}
	sort.Strings(deletedIDs)
	digest, _ := domain.Digest(deletedIDs)
	receipt := DeletionReceipt{TenantID: tenant, Deleted: len(deletedIDs), Cutoff: at, EvidenceDigest: digest, GeneratedAt: at}
	receipt.ID, _ = domain.StableID("deletion", struct {
		Tenant, Digest string
		At             time.Time
	}{tenant, digest, at})
	return kept, receipt
}

type SignedDigest struct {
	Algorithm string `json:"algorithm"`
	Digest    string `json:"digest"`
	Signature string `json:"signature"`
	PublicKey string `json:"public_key"`
}

func Sign(value any, private ed25519.PrivateKey) (SignedDigest, error) {
	if len(private) != ed25519.PrivateKeySize {
		return SignedDigest{}, errors.New("invalid Ed25519 private key")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return SignedDigest{}, err
	}
	sum := sha256.Sum256(data)
	signature := ed25519.Sign(private, sum[:])
	public := private.Public().(ed25519.PublicKey)
	return SignedDigest{Algorithm: "Ed25519-SHA256", Digest: hex.EncodeToString(sum[:]), Signature: base64.StdEncoding.EncodeToString(signature), PublicKey: base64.StdEncoding.EncodeToString(public)}, nil
}

func Verify(value any, signed SignedDigest) error {
	if signed.Algorithm != "Ed25519-SHA256" {
		return errors.New("unsupported signature algorithm")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != signed.Digest {
		return errors.New("report digest mismatch")
	}
	public, err := base64.StdEncoding.DecodeString(signed.PublicKey)
	if err != nil || len(public) != ed25519.PublicKeySize {
		return errors.New("invalid public key")
	}
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(public), sum[:], signature) {
		return errors.New("invalid report signature")
	}
	return nil
}
