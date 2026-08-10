package config

import (
	"strings"
	"testing"
)

func TestLoadReportsMissingRequiredConfiguration(t *testing.T) {
	t.Setenv("THREADMILL_DATABASE_URL", "")
	t.Setenv("THREADMILL_OBJECT_STORE_ENDPOINT", "")
	t.Setenv("THREADMILL_ALLOWED_ORIGINS", "")

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
	if len(diag.Missing) != 6 {
		t.Fatalf("missing keys = %v, want database, object store, and browser origin configuration", diag.Missing)
	}
}

func TestLoadAppliesDefaultsWhenRequiredConfigurationExists(t *testing.T) {
	t.Setenv("THREADMILL_DATABASE_URL", "postgres://threadmill:threadmill@localhost:5432/threadmill")
	t.Setenv("THREADMILL_OBJECT_STORE_ENDPOINT", "localhost:9000")
	t.Setenv("THREADMILL_OBJECT_STORE_ACCESS_KEY", "threadmill")
	t.Setenv("THREADMILL_OBJECT_STORE_SECRET_KEY", "super-secret")
	t.Setenv("THREADMILL_OBJECT_STORE_BUCKET", "artifacts")
	t.Setenv("THREADMILL_ALLOWED_ORIGINS", "https://threadmill.example")

	cfg, err := Load(Environment{})
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.ProjectID != "default-project" {
		t.Fatalf("ProjectID = %q, want default-project", cfg.ProjectID)
	}
	if cfg.WebDistDir != "web/dist" {
		t.Fatalf("WebDistDir = %q, want web/dist", cfg.WebDistDir)
	}
	if !cfg.ObjectStoreSecure {
		t.Fatal("ObjectStoreSecure = false, want default true")
	}
}

func TestLoadParsesObjectStoreSecureFlag(t *testing.T) {
	cfg, err := Load(Environment{
		"THREADMILL_DATABASE_URL":            "postgres://threadmill:threadmill@localhost:5432/threadmill",
		"THREADMILL_OBJECT_STORE_ENDPOINT":   "localhost:9000",
		"THREADMILL_OBJECT_STORE_ACCESS_KEY": "threadmill",
		"THREADMILL_OBJECT_STORE_SECRET_KEY": "super-secret",
		"THREADMILL_OBJECT_STORE_BUCKET":     "artifacts",
		"THREADMILL_OBJECT_STORE_SECURE":     "false",
		"THREADMILL_ALLOWED_ORIGINS":         "http://127.0.0.1:8080",
	})
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.ObjectStoreSecure {
		t.Fatal("ObjectStoreSecure = true, want false")
	}
}

func TestLoadParsesAndDeduplicatesAllowedOrigins(t *testing.T) {
	cfg, err := Load(Environment{
		"THREADMILL_DATABASE_URL":            "postgres://threadmill:threadmill@localhost:5432/threadmill",
		"THREADMILL_OBJECT_STORE_ENDPOINT":   "localhost:9000",
		"THREADMILL_OBJECT_STORE_ACCESS_KEY": "threadmill",
		"THREADMILL_OBJECT_STORE_SECRET_KEY": "super-secret",
		"THREADMILL_OBJECT_STORE_BUCKET":     "artifacts",
		"THREADMILL_ALLOWED_ORIGINS":         " https://threadmill.example, http://127.0.0.1:8080,https://threadmill.example ",
	})
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	want := []string{"https://threadmill.example", "http://127.0.0.1:8080"}
	if len(cfg.AllowedOrigins) != len(want) {
		t.Fatalf("AllowedOrigins = %v, want %v", cfg.AllowedOrigins, want)
	}
	for i := range want {
		if cfg.AllowedOrigins[i] != want[i] {
			t.Fatalf("AllowedOrigins = %v, want %v", cfg.AllowedOrigins, want)
		}
	}
}

func TestLoadRejectsAllowedOriginWithPath(t *testing.T) {
	_, err := Load(Environment{
		"THREADMILL_DATABASE_URL":            "postgres://threadmill:threadmill@localhost:5432/threadmill",
		"THREADMILL_OBJECT_STORE_ENDPOINT":   "localhost:9000",
		"THREADMILL_OBJECT_STORE_ACCESS_KEY": "threadmill",
		"THREADMILL_OBJECT_STORE_SECRET_KEY": "super-secret",
		"THREADMILL_OBJECT_STORE_BUCKET":     "artifacts",
		"THREADMILL_ALLOWED_ORIGINS":         "https://threadmill.example/app",
	})
	if err == nil {
		t.Fatal("Load() error = nil, want invalid origin error")
	}
	diag, ok := AsDiagnostic(err)
	if !ok || diag.Code != "configuration_invalid" {
		t.Fatalf("Load() diagnostic = %#v, %t, want configuration_invalid", diag, ok)
	}
}

func TestCheckDoesNotExposeObjectStoreSecret(t *testing.T) {
	cfg := Config{
		DatabaseURL:          "postgres://user:password@example/db",
		ObjectStoreEndpoint:  "localhost:9000",
		ObjectStoreAccessKey: "access-key",
		ObjectStoreSecretKey: "do-not-leak",
		ObjectStoreBucket:    "artifacts",
		ObjectStoreSecure:    false,
		HTTPAddr:             ":8080",
		AllowedOrigins:       []string{"https://threadmill.example"},
	}
	diag := Check(cfg)
	if diag.Message == "" {
		t.Fatal("diagnostic message is empty")
	}
	if containsAny(diag.Message, []string{"do-not-leak", "access-key", "password"}) {
		t.Fatalf("diagnostic leaked secret material: %q", diag.Message)
	}
}

func containsAny(value string, needles []string) bool {
	for _, needle := range needles {
		if needle != "" && strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
