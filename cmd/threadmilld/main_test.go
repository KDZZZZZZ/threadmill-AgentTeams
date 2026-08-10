package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCheckReportsStructuredDiagnosticWhenConfigIsMissing(t *testing.T) {
	t.Setenv("THREADMILL_DATABASE_URL", "")
	t.Setenv("THREADMILL_OBJECT_STORE_ENDPOINT", "")

	var stdout, stderr bytes.Buffer
	code := run([]string{"check"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("run(check) exit code = 0, want non-zero")
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty structured stdout only", stderr.String())
	}

	var payload struct {
		OK          bool     `json:"ok"`
		Code        string   `json:"code"`
		Message     string   `json:"message"`
		Missing     []string `json:"missing"`
		Recoverable bool     `json:"recoverable"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if payload.OK {
		t.Fatal("diagnostic ok = true, want false")
	}
	if payload.Code != "configuration_missing" {
		t.Fatalf("diagnostic code = %q, want configuration_missing", payload.Code)
	}
	if !payload.Recoverable {
		t.Fatal("diagnostic recoverable = false, want true")
	}
	if len(payload.Missing) == 0 {
		t.Fatal("diagnostic missing list is empty")
	}
}
