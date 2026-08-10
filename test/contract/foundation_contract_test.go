package contract

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestThreadmillV1OpenAPIIsValidAndKeepsControlPlaneInternal(t *testing.T) {
	t.Parallel()

	specPath := filepath.Join("..", "..", "api", "openapi", "threadmill-v1.yaml")
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}

	required := map[string]string{
		"/v1/requirements":          "POST",
		"/v1/capacity":              "GET",
		"/v1/capacity-adjustments":  "POST",
		"/v1/human-decisions":       "POST",
		"/v1/tasks/{task_id}":       "GET",
		"/v1/coordination/snapshot": "GET",
		"/v1/coordination/endpoints/{task_id}/{endpoint_id}/inspector": "GET",
		"/v1/manager/messages":                        "POST",
		"/v1/manager/conversations/{conversation_id}": "GET",
		"/v1/events":        "GET",
		"/v1/events/stream": "GET",
		"/healthz":          "GET",
		"/readyz":           "GET",
	}
	for path, method := range required {
		item := doc.Paths.Value(path)
		if item == nil {
			t.Errorf("required path %q is missing", path)
			continue
		}
		if item.GetOperation(method) == nil {
			t.Errorf("required operation %s %s is missing", method, path)
		}
	}

	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read OpenAPI document: %v", err)
	}
	text := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"graphruntime",
		"phasecontroller",
		"agentteamshostadapter",
		"pendingsubgraph",
		"json patch",
	} {
		if strings.Contains(text, forbidden) {
			t.Errorf("OpenAPI document exposes forbidden internal surface %q", forbidden)
		}
	}
}

func TestFoundationADRsAreAcceptedAndFreezeRequiredDefaults(t *testing.T) {
	t.Parallel()

	adrDir := filepath.Join("..", "..", "docs", "adr")
	files := []string{
		"0001-modular-monolith.md",
		"0002-postgres-outbox-minio.md",
		"0003-mcp-capability-auth.md",
		"0004-context-physical-model.md",
		"0005-agentteams-mvp.md",
		"0006-web-ui-projection-and-sse.md",
	}
	for _, name := range files {
		content, err := os.ReadFile(filepath.Join(adrDir, name))
		if err != nil {
			t.Fatalf("read ADR %s: %v", name, err)
		}
		text := string(content)
		if !strings.Contains(text, "Status: Accepted") {
			t.Errorf("ADR %s is not Accepted", name)
		}
		for _, unresolved := range []string{"TODO", "TBD", "待定"} {
			if strings.Contains(text, unresolved) {
				t.Errorf("ADR %s still contains unresolved marker %q", name, unresolved)
			}
		}
	}

	contextADR, err := os.ReadFile(filepath.Join(adrDir, "0004-context-physical-model.md"))
	if err != nil {
		t.Fatalf("read context ADR: %v", err)
	}
	contextText := string(contextADR)
	for _, frozenDefault := range []string{"64", "32", "250 ms", "30 days", "365 days", "fail closed"} {
		if !strings.Contains(contextText, frozenDefault) {
			t.Errorf("context ADR does not freeze required default %q", frozenDefault)
		}
	}
}
