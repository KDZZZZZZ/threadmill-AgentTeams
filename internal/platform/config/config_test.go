package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadReportsMissingRequiredConfiguration(t *testing.T) {
	env := productionConfigEnv()
	for key := range env {
		env[key] = ""
	}
	_, err := Load(env)
	if err == nil {
		t.Fatal("Load() error = nil, want missing configuration error")
	}
	diag, ok := AsDiagnostic(err)
	if !ok || diag.Code != "configuration_missing" {
		t.Fatalf("Load() diagnostic = %#v, %t, want configuration_missing", diag, ok)
	}
	if len(diag.Missing) != 19 {
		t.Fatalf("missing keys = %v, want all 19 production secrets/endpoints", diag.Missing)
	}
}

func TestLoadAppliesDefaultsAndParsesProductionWiring(t *testing.T) {
	cfg, err := Load(productionConfigEnv())
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.ProjectID != "default-project" || cfg.WebDistDir != "web/dist" {
		t.Fatalf("defaults = addr %q project %q web %q", cfg.HTTPAddr, cfg.ProjectID, cfg.WebDistDir)
	}
	if !cfg.ObjectStoreSecure {
		t.Fatal("ObjectStoreSecure = false, want default true")
	}
	if cfg.AgentTeamsContainers["manager-a"] != "qwen-manager-a" || len(cfg.RuntimeTokenKey) != 32 {
		t.Fatalf("production wiring = containers %#v key bytes %d", cfg.AgentTeamsContainers, len(cfg.RuntimeTokenKey))
	}
}

func TestLoadParsesObjectStoreSecureAndDeduplicatesOrigins(t *testing.T) {
	env := productionConfigEnv()
	env[envObjectStoreSecure] = "false"
	env[envAllowedOrigins] = " https://threadmill.example, http://127.0.0.1:8080,https://threadmill.example "
	cfg, err := Load(env)
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.ObjectStoreSecure || len(cfg.AllowedOrigins) != 2 {
		t.Fatalf("secure=%t origins=%v", cfg.ObjectStoreSecure, cfg.AllowedOrigins)
	}
}

func TestLoadRejectsUnsafeProductionConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "origin path", key: envAllowedOrigins, value: "https://threadmill.example/app"},
		{name: "controller userinfo", key: envAgentTeamsControllerURL, value: "https://secret@example.test"},
		{name: "controller bearer whitespace", key: envAgentTeamsControllerBearer, value: "secret token"},
		{name: "container mapping", key: envAgentTeamsContainers, value: "manager-a=qwen-a,manager-a=qwen-b"},
		{name: "container alias", key: envAgentTeamsContainers, value: "manager-a=qwen-a,worker-a=qwen-a"},
		{name: "container shell separator", key: envAgentTeamsContainers, value: "manager-a=qwen:a"},
		{name: "container MCP path", key: envContainerMCPURL, value: "http://host.docker.internal:8080/not-mcp"},
		{name: "room whitespace", key: envAgentTeamsRoomID, value: "!threadmill room:example.test"},
		{name: "runtime key", key: envRuntimeTokenKey, value: base64.StdEncoding.EncodeToString([]byte("too-short"))},
		{name: "shared prefix traversal", key: envAgentTeamsSharedPrefix, value: "runtime/../escape"},
		{name: "relative runtime assets", key: envRuntimeAssetsRoot, value: "./runtime-assets"},
		{name: "relative repo", key: envRepositoryPath, value: "./repo"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := productionConfigEnv()
			env[test.key] = test.value
			_, err := Load(env)
			if err == nil {
				t.Fatal("Load() error = nil, want invalid configuration")
			}
			diag, ok := AsDiagnostic(err)
			if !ok || diag.Code != "configuration_invalid" || strings.Contains(diag.Message, test.value) {
				t.Fatalf("diagnostic = %#v, %t; value must not be echoed", diag, ok)
			}
		})
	}
}

func TestCheckDoesNotExposeSecrets(t *testing.T) {
	cfg, err := Load(productionConfigEnv())
	if err != nil {
		t.Fatal(err)
	}
	diag := Check(cfg)
	if diag.Message == "" {
		t.Fatal("diagnostic message is empty")
	}
	if containsAny(diag.Message, []string{cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey, cfg.AgentTeamsSharedAccessKey, cfg.AgentTeamsSharedSecretKey, cfg.AgentTeamsControllerBearer, base64.StdEncoding.EncodeToString(cfg.RuntimeTokenKey), "password"}) {
		t.Fatalf("diagnostic leaked secret material: %q", diag.Message)
	}
}

func productionConfigEnv() Environment {
	return Environment{
		envDatabaseURL:                "postgres://threadmill:password@localhost:5432/threadmill",
		envObjectStoreEndpoint:        "localhost:9000",
		envObjectStoreAccessKey:       "threadmill-access",
		envObjectStoreSecretKey:       "object-secret",
		envObjectStoreBucket:          "artifacts",
		envAllowedOrigins:             "https://threadmill.example",
		envAgentTeamsControllerURL:    "http://127.0.0.1:8091",
		envAgentTeamsControllerBearer: "controller-secret",
		envContainerMCPURL:            "http://host.docker.internal:8080/mcp",
		envAgentTeamsRoomID:           "!threadmill:example.test",
		envAgentTeamsContainers:       "manager-a=qwen-manager-a,worker-a=qwen-worker-a",
		envAgentTeamsSharedBucket:     "artifacts",
		envAgentTeamsSharedPrefix:     "agentteams/runtime",
		envAgentTeamsSharedAccessKey:  "threadmill-dispatcher",
		envAgentTeamsSharedSecretKey:  "dispatcher-secret",
		envRuntimeAssetsRoot:          filepath.Join(os.TempDir(), "threadmill-runtime-assets"),
		envRepositoryPath:             filepath.Join(os.TempDir(), "threadmill-repository"),
		envWorktreeParent:             filepath.Join(os.TempDir(), "threadmill-worktrees"),
		envRuntimeTokenKey:            base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
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
