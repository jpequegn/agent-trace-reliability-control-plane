package corpus

import (
	"reflect"
	"testing"
)

func TestCorpusIsDeterministicAndComplete(t *testing.T) {
	first := Generate()
	second := Generate()
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("corpus generation is not deterministic")
	}
	if len(first.Spans) != SpanCount || len(first.Truth) != RunCount || len(first.ReleasePackets) != 3 {
		t.Fatalf("spans=%d truth=%d releases=%d", len(first.Spans), len(first.Truth), len(first.ReleasePackets))
	}
}

func TestCorpusContainsSecretCanariesAndReviewedTruth(t *testing.T) {
	data := Generate()
	canaries := 0
	classes := map[string]bool{}
	for _, span := range data.Spans {
		if span.Attributes["authorization"] == SecretCanary {
			canaries++
		}
	}
	for _, truth := range data.Truth {
		classes[truth.Class] = true
	}
	if canaries == 0 || len(classes) != 9 {
		t.Fatalf("canaries=%d classes=%d", canaries, len(classes))
	}
}
