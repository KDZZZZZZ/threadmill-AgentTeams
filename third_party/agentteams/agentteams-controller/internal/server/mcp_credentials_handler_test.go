package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss/ossfake"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/service"
)

func TestMCPCredentialBindingHandlerRedactsAndSharesStoreWithProjection(t *testing.T) {
	const secret = "test-threadmill-token-a"
	store := service.NewInMemoryMCPCredentialBindingStore()
	handler := NewMCPCredentialBindingHandler(store)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/mcp-credentials", bytes.NewBufferString(`{"workerName":"worker-a","headerName":"X-Threadmill-Execution-Token","secretValue":"`+secret+`"}`))
	response := httptest.NewRecorder()
	handler.Create(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), secret) {
		t.Fatalf("create response leaked secret: %s", response.Body.String())
	}
	var view service.MCPCredentialBindingView
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	getRequest := httptest.NewRequest(http.MethodGet, "/api/v1/mcp-credentials/"+view.ID, nil)
	getRequest.SetPathValue("id", view.ID)
	getResponse := httptest.NewRecorder()
	handler.Get(getResponse, getRequest)
	if getResponse.Code != http.StatusOK || strings.Contains(getResponse.Body.String(), secret) {
		t.Fatalf("redacted GET failed: %d %s", getResponse.Code, getResponse.Body.String())
	}

	ossStore := ossfake.NewMemory()
	deployer := service.NewDeployer(service.DeployerConfig{OSS: ossStore, MCPCredentialBindings: store})
	err := deployer.DeployMemberRuntimeConfig(context.Background(), service.MemberRuntimeConfigDeployRequest{
		Name: "worker-a", RuntimeName: "worker-a", Runtime: "qwenpaw",
		Spec: v1beta1.WorkerSpec{McpServers: []v1beta1.MCPServer{{Name: "threadmill", URL: "http://threadmill/mcp", CredentialBindingRef: view.ID}}},
	})
	if err != nil {
		t.Fatalf("shared store projection failed: %v", err)
	}
	config, err := ossStore.GetObject(context.Background(), "agents/worker-a/runtime/runtime.yaml")
	if err != nil || !strings.Contains(string(config), secret) {
		t.Fatalf("shared store was not projected: %v %s", err, config)
	}

	revokeRequest := httptest.NewRequest(http.MethodPost, "/api/v1/mcp-credentials/"+view.ID+"/revoke", nil)
	revokeRequest.SetPathValue("id", view.ID)
	revokeResponse := httptest.NewRecorder()
	handler.Revoke(revokeResponse, revokeRequest)
	if revokeResponse.Code != http.StatusOK || strings.Contains(revokeResponse.Body.String(), secret) || !strings.Contains(revokeResponse.Body.String(), `"state":"revoked"`) {
		t.Fatalf("revoke response unexpected: %d %s", revokeResponse.Code, revokeResponse.Body.String())
	}
}
