package runtime

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPMCPToolDiscovererUsesTrustedHeaderOnly(t *testing.T) {
	const token = "test-token-b"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Threadmill-Execution-Token"); got != token {
			t.Fatalf("execution token header = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		// The request body is a fixed JSON-RPC tools/list envelope and must not
		// carry the trusted authorization material.
		if strings.Contains(string(body), token) {
			t.Fatal("token appeared in JSON-RPC request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"rehydration-discovery","result":{"tools":[{"name":"artifact.register"}]}}`))
	}))
	defer server.Close()

	tools, err := (HTTPMCPToolDiscoverer{URL: server.URL}).DiscoverMCPTools(context.Background(), ProvisionedWorker{}, IssuedExecutionAuthorization{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0] != "artifact.register" {
		t.Fatalf("tools = %#v", tools)
	}
}

func TestHTTPMCPToolDiscovererRedactsTokenFromErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "failure", http.StatusUnauthorized) }))
	defer server.Close()
	_, err := (HTTPMCPToolDiscoverer{URL: server.URL}).DiscoverMCPTools(context.Background(), ProvisionedWorker{}, IssuedExecutionAuthorization{Token: "test-token-b"})
	if err == nil || strings.Contains(err.Error(), "test-token-b") {
		t.Fatalf("error leaked token: %v", err)
	}
}
