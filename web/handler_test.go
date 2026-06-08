package web_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/StrangeNoob/relay/web"
)

func TestHandlerServesIndex(t *testing.T) {
	srv := httptest.NewServer(web.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
}

func TestHandlerSpaFallback(t *testing.T) {
	srv := httptest.NewServer(web.Handler())
	defer srv.Close()

	// A client-side route that is not a real asset must still return index.html (200).
	resp, err := http.Get(srv.URL + "/queues/emails")
	if err != nil {
		t.Fatalf("GET /queues/emails: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (SPA fallback)", resp.StatusCode)
	}
}
