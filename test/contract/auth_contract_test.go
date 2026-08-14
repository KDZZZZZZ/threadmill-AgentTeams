package contract

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestOperatorWriteOperationsDeclareCSRFGateAndStableErrors(t *testing.T) {
	t.Parallel()

	specPath := filepath.Join("..", "..", "api", "openapi", "threadmill-v1.yaml")
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}

	for _, path := range []string{
		"/v1/requirements",
		"/v1/capacity-adjustments",
		"/v1/human-decisions",
		"/v1/manager/messages",
	} {
		operation := doc.Paths.Value(path).Post
		if !hasParameterRef(operation.Parameters, "#/components/parameters/CSRFToken") {
			t.Errorf("POST %s does not declare the operator CSRF header", path)
		}
	}

	errorSchema := doc.Components.Schemas["Error"].Value
	codeSchema := errorSchema.Properties["code"].Value
	gotCodes := make(map[string]struct{}, len(codeSchema.Enum))
	for _, value := range codeSchema.Enum {
		if code, ok := value.(string); ok {
			gotCodes[code] = struct{}{}
		}
	}
	for _, required := range []string{
		"forbidden",
		"csrf_invalid",
		"origin_invalid",
		"revision_conflict",
		"idempotency_conflict",
		"stale_binding",
		"lease_conflict",
	} {
		if _, ok := gotCodes[required]; !ok {
			t.Errorf("Error.code is missing %q", required)
		}
	}
}

func hasParameterRef(parameters openapi3.Parameters, ref string) bool {
	for _, parameter := range parameters {
		if parameter.Ref == ref {
			return true
		}
	}
	return false
}
