package webui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServesStaticAssetsAndHead(t *testing.T) {
	h := newTestHandler(t)

	rec := request(h, http.MethodGet, "/assets/app.abc123.js")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "console.log('ok');" {
		t.Fatalf("asset body = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("asset cache-control = %q", got)
	}

	rec = request(h, http.MethodHead, "/assets/app.abc123.js")
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD wrote body %q", rec.Body.String())
	}
}

func TestUnknownFrontendRouteFallsBackToIndex(t *testing.T) {
	h := newTestHandler(t)

	rec := request(h, http.MethodGet, "/tasks/task-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != testIndex {
		t.Fatalf("fallback body = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("index cache-control = %q", got)
	}
}

func TestReservedRoutesAreNotSwallowedBySPAFallback(t *testing.T) {
	h := newTestHandler(t)

	for _, path := range []string{
		"/v1",
		"/v1/unknown",
		"/mcp",
		"/mcp/tools",
		"/healthz",
		"/readyz",
	} {
		t.Run(path, func(t *testing.T) {
			rec := request(h, http.MethodGet, path)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d body=%s, want 404", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), testIndex) {
				t.Fatalf("reserved route received SPA index")
			}
		})
	}
}

func TestRejectsTraversalAndEncodedTraversal(t *testing.T) {
	h := newTestHandler(t)

	for _, path := range []string{
		"/../secret.txt",
		"/assets/../index.html",
		"/%2e%2e/secret.txt",
		"/assets/%2e%2e/index.html",
		"/assets\\app.abc123.js",
	} {
		t.Run(path, func(t *testing.T) {
			rec := request(h, http.MethodGet, path)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d body=%s, want 404", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), testIndex) {
				t.Fatalf("unsafe path received SPA index")
			}
		})
	}
}

func TestInvalidDistFailsClosed(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatalf("empty dist dir accepted")
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := New(Options{DistDir: missing}); err == nil {
		t.Fatalf("missing dist dir accepted")
	}
	noIndex := t.TempDir()
	if _, err := New(Options{DistDir: noIndex}); err == nil {
		t.Fatalf("dist without index.html accepted")
	}
	fileDist := filepath.Join(t.TempDir(), "dist.txt")
	if err := os.WriteFile(fileDist, []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{DistDir: fileDist}); err == nil {
		t.Fatalf("file dist dir accepted")
	}
}

func TestNonGetAndHeadMethodsAreRejected(t *testing.T) {
	h := newTestHandler(t)

	rec := request(h, http.MethodPost, "/")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d body=%s, want 405", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("allow = %q", got)
	}
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	dist := t.TempDir()
	if err := os.Mkdir(filepath.Join(dist, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "index.html"), []byte(testIndex), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "assets", "app.abc123.js"), []byte("console.log('ok');"), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := New(Options{DistDir: dist})
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func request(h http.Handler, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const testIndex = "<!doctype html><div id=\"root\"></div>"
