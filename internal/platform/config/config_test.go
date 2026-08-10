package config

import (
	"testing"
)

func TestLoadReportsMissingRequiredConfiguration(t *testing.T) {
	t.Setenv("THREADMILL_DATABASE_URL", "")
	t.Setenv("THREADMILL_OBJECT_STORE_ENDPOINT", "")

	_, err := Load(Environment{})
	if err == nil {
		t.Fatal("Load() error = nil, want missing configuration error")
	}

	diag, ok := AsDiagnostic(err)
	if !ok {
		t.Fatalf("Load() error type = %T, want DiagnosticError", err)
	}
	if diag.Code != "configuration_missing" {
		t.Fatalf("diagnostic code = %q, want configuration_missing", diag.Code)
	}
	if len(diag.Missing) != 2 {
		t.Fatalf("missing keys = %v, want database and object store endpoint", diag.Missing)
	}
}

func TestLoadAppliesDefaultsWhenRequiredConfigurationExists(t *testing.T) {
	t.Setenv("THREADMILL_DATABASE_URL", "postgres://threadmill:threadmill@localhost:5432/threadmill")
	t.Setenv("THREADMILL_OBJECT_STORE_ENDPOINT", "http://localhost:9000")

	cfg, err := Load(Environment{})
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
}
