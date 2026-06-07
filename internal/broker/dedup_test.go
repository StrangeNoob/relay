package broker

import (
	"testing"
	"time"
)

func TestDedupKeyFormat(t *testing.T) {
	if got := dedupKey("emails", "abc"); got != "q:emails:dedup:abc" {
		t.Errorf("dedupKey = %q, want %q", got, "q:emails:dedup:abc")
	}
}

func TestDedupTTLDefaultAndOverride(t *testing.T) {
	if b := New(nil); b.dedupTTL != 24*time.Hour {
		t.Errorf("default dedupTTL = %v, want 24h", b.dedupTTL)
	}
	if b := New(nil, WithDedupTTL(time.Hour)); b.dedupTTL != time.Hour {
		t.Errorf("dedupTTL = %v, want 1h", b.dedupTTL)
	}
}
