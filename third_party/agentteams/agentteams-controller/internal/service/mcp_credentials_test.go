package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss/ossfake"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"
)

func TestMCPCredentialBindingStoreRedactsAndRevokes(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryMCPCredentialBindingStore()
	const secret = "test-threadmill-token-a"
	view, err := store.Create(ctx, MCPCredentialBinding{WorkerName: "worker-a", HeaderName: "X-Threadmill-Execution-Token", Value: secret})
	if err != nil {
		t.Fatal(err)
	}
	if view.ID == "" || view.State != "active" {
		t.Fatalf("unexpected create view: %#v", view)
	}
	redacted, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(redacted), secret) {
		t.Fatalf("redacted view leaked secret: %s", redacted)
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-b"); !errors.Is(err, ErrMCPBindingOwner) {
		t.Fatalf("wrong worker resolve error = %v, want ownership rejection", err)
	}
	if err := store.Revoke(ctx, view.ID); err != nil {
		t.Fatal(err)
	}
	view, err = store.Get(ctx, view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.State != "revoked" {
		t.Fatalf("state = %q, want revoked", view.State)
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-a"); !errors.Is(err, ErrMCPBindingRevoked) {
		t.Fatalf("revoked resolve error = %v, want revoked", err)
	}
	if _, err := store.Resolve(ctx, "unknown", "worker-a"); !errors.Is(err, ErrMCPBindingNotFound) {
		t.Fatalf("unknown resolve error = %v, want not found", err)
	}
}

func TestSecretMCPCredentialBindingStoreRedactsAndRevokes(t *testing.T) {
	ctx := context.Background()
	store := &SecretMCPCredentialBindingStore{Client: fake.NewSimpleClientset(), Namespace: "default", ControllerName: "controller-a"}
	view, err := store.Create(ctx, MCPCredentialBinding{WorkerName: "worker-a", HeaderName: "X-Test", Value: "test-threadmill-token-a"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mustJSON(t, view)), "test-threadmill-token-a") {
		t.Fatal("secret store readback leaked secret")
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-a"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := store.Revoke(ctx, view.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-a"); !errors.Is(err, ErrMCPBindingRevoked) {
		t.Fatalf("resolve after revoke error = %v", err)
	}
}

func TestFileMCPCredentialBindingStoreKeepsControllerPrivateMaterial(t *testing.T) {
	ctx := context.Background()
	store := &FileMCPCredentialBindingStore{Dir: t.TempDir()}
	view, err := store.Create(ctx, MCPCredentialBinding{WorkerName: "worker-a", HeaderName: "X-Test", Value: "test-threadmill-token-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-a"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := store.Revoke(ctx, view.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(ctx, view.ID, "worker-a"); !errors.Is(err, ErrMCPBindingRevoked) {
		t.Fatalf("resolve after revoke error = %v", err)
	}
}

func TestDeployMemberRuntimeConfigProjectsOwnedMCPCredentialBinding(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryMCPCredentialBindingStore()
	const secret = "test-threadmill-token-a"
	view, err := store.Create(ctx, MCPCredentialBinding{WorkerName: "worker-a", HeaderName: "X-Threadmill-Execution-Token", Value: secret})
	if err != nil {
		t.Fatal(err)
	}
	ossStore := ossfake.NewMemory()
	deployer := NewDeployer(DeployerConfig{OSS: ossStore, MCPCredentialBindings: store})
	req := MemberRuntimeConfigDeployRequest{
		Name: "worker-a", RuntimeName: "worker-a", Runtime: "qwenpaw",
		Spec: v1beta1.WorkerSpec{McpServers: []v1beta1.MCPServer{{Name: "threadmill", URL: "http://threadmill/mcp", Transport: "streamable_http", CredentialBindingRef: view.ID}}},
	}
	if err := deployer.DeployMemberRuntimeConfig(ctx, req); err != nil {
		t.Fatalf("deploy runtime config: %v", err)
	}
	payload, err := ossStore.GetObject(ctx, memberRuntimeConfigObjectKey("worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), secret) {
		t.Fatalf("runtime projection did not contain private header value: %s", payload)
	}
	if strings.Contains(string(mustJSON(t, view)), secret) {
		t.Fatal("credential readback leaked secret")
	}
	var doc map[string]any
	if err := yaml.Unmarshal(payload, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["desired"] == nil {
		t.Fatal("runtime projection omitted desired configuration")
	}
	if err := store.Revoke(ctx, view.ID); err != nil {
		t.Fatal(err)
	}
	if err := deployer.DeployMemberRuntimeConfig(ctx, req); !errors.Is(err, ErrMCPBindingRevoked) {
		t.Fatalf("revoked projection error = %v, want revoked", err)
	}
	req.Name, req.RuntimeName = "worker-b", "worker-b"
	if err := deployer.DeployMemberRuntimeConfig(ctx, req); !errors.Is(err, ErrMCPBindingOwner) {
		t.Fatalf("wrong owner projection error = %v, want ownership rejection", err)
	}
	req.Spec.McpServers[0].CredentialBindingRef = "unknown"
	if err := deployer.DeployMemberRuntimeConfig(ctx, req); !errors.Is(err, ErrMCPBindingNotFound) {
		t.Fatalf("unknown projection error = %v, want not found", err)
	}
}

func TestDeployMemberRuntimeConfigKeepsMCPServerWithoutCredentialBindingCompatible(t *testing.T) {
	ctx := context.Background()
	ossStore := ossfake.NewMemory()
	deployer := NewDeployer(DeployerConfig{OSS: ossStore})
	err := deployer.DeployMemberRuntimeConfig(ctx, MemberRuntimeConfigDeployRequest{
		Name: "worker-a", RuntimeName: "worker-a", Runtime: "qwenpaw",
		Spec: v1beta1.WorkerSpec{McpServers: []v1beta1.MCPServer{{Name: "public", URL: "http://public/mcp", Transport: "streamable_http"}}},
	})
	if err != nil {
		t.Fatalf("legacy MCP server projection failed: %v", err)
	}
	payload, err := ossStore.GetObject(ctx, memberRuntimeConfigObjectKey("worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "headers:") || !strings.Contains(string(payload), "http://public/mcp") {
		t.Fatalf("legacy runtime projection changed unexpectedly: %s", payload)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
