package mcpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type fakeAgentTokenAuthenticator struct {
	token     string
	principal auth.Principal
	err       error
	calls     int
}

func (f *fakeAgentTokenAuthenticator) AuthenticateAgentToken(_ context.Context, token string) (auth.Principal, error) {
	f.calls++
	if f.err != nil {
		return auth.Principal{}, f.err
	}
	if token != f.token {
		return auth.Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "bad token"}
	}
	return f.principal, nil
}

func TestHTTPHandlerRunsInitializeListsOnlyCallableToolsAndCallsWithTrustedScope(t *testing.T) {
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextExplore, auth.ToolContextSearch)
	authenticator := &fakeAgentTokenAuthenticator{token: "secret", principal: principal}
	type contextKey string
	const traceKey contextKey = "trace"
	var receivedScope auth.BoundScope
	var receivedPrincipal auth.Principal
	var receivedPayload json.RawMessage
	var receivedTrace any
	registry, err := NewRegistry(
		ToolSpec{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(ctx context.Context, gotPrincipal auth.Principal, scope auth.BoundScope, payload json.RawMessage) (any, error) {
			receivedScope = scope
			receivedPrincipal = gotPrincipal
			receivedPayload = append(json.RawMessage(nil), payload...)
			receivedTrace = ctx.Value(traceKey)
			return map[string]any{"ok": true, "scope": scope.InvocationID}, nil
		})},
		// Even if a forged principal claims this tool, the executor role policy
		// excludes it from tools/list and tools/call authorization.
		ToolSpec{ID: auth.ToolContextSearch, Handler: HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) {
			return nil, errors.New("must not run")
		})},
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{ServerVersion: "test"})
	if err != nil {
		t.Fatal(err)
	}

	initialize := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`, nil)
	if initialize.Code != http.StatusOK {
		t.Fatalf("initialize status = %d body=%s", initialize.Code, initialize.Body.String())
	}
	if got := initialize.Header().Get("MCP-Protocol-Version"); got != "2025-11-25" {
		t.Fatalf("protocol header = %q", got)
	}
	var initialized struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			ServerInfo      struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	decodeRecorderJSON(t, initialize, &initialized)
	if initialized.Result.ProtocolVersion != "2025-11-25" || initialized.Result.ServerInfo.Name != "threadmill" || initialized.Result.ServerInfo.Version != "test" {
		t.Fatalf("initialize result = %#v", initialized.Result)
	}

	list := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":"list-1","method":"tools/list","params":{}}`, map[string]string{"MCP-Protocol-Version": "2025-11-25"})
	var listed struct {
		Result struct {
			Tools []toolDefinition `json:"tools"`
		} `json:"result"`
	}
	decodeRecorderJSON(t, list, &listed)
	if len(listed.Result.Tools) != 1 || listed.Result.Tools[0].Name != string(auth.ToolContextExplore) {
		t.Fatalf("visible tools = %#v", listed.Result.Tools)
	}
	if listed.Result.Tools[0].InputSchema["type"] != "object" || listed.Result.Tools[0].Description == "" {
		t.Fatalf("tool definition = %#v", listed.Result.Tools[0])
	}

	callBody := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"context.explore","arguments":{"anchor_ref":"node:n1","project_id":"forged"}}}`
	request := httptest.NewRequest(http.MethodPost, "http://threadmill.test/mcp", bytes.NewBufferString(callBody))
	request = request.WithContext(context.WithValue(request.Context(), traceKey, "trace-1"))
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("Content-Type", "application/json")
	call := httptest.NewRecorder()
	handler.ServeHTTP(call, request)
	var called struct {
		Result toolCallResult `json:"result"`
	}
	decodeRecorderJSON(t, call, &called)
	if call.Code != http.StatusOK || called.Result.IsError || len(called.Result.Content) != 1 {
		t.Fatalf("call status=%d result=%#v", call.Code, called.Result)
	}
	if receivedScope.ProjectID != principal.ProjectID || receivedScope.TaskID != principal.TaskID || receivedScope.InvocationID != principal.InvocationID {
		t.Fatalf("handler scope = %#v", receivedScope)
	}
	if receivedPrincipal.InvocationID != principal.InvocationID || receivedTrace != "trace-1" {
		t.Fatalf("principal=%#v trace=%#v", receivedPrincipal, receivedTrace)
	}
	if string(receivedPayload) != `{"anchor_ref":"node:n1","project_id":"forged"}` {
		t.Fatalf("payload = %s", receivedPayload)
	}
}

func TestHTTPHandlerRejectsMissingBearerAndUntrustedOrigin(t *testing.T) {
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextExplore)
	authenticator := &fakeAgentTokenAuthenticator{token: "secret", principal: principal}
	registry, err := NewRegistry(ToolSpec{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) {
		return map[string]any{"ok": true}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{AllowedOrigins: []string{"https://console.example"}})
	if err != nil {
		t.Fatal(err)
	}

	missing := serveMCP(t, handler, "", "", `{"jsonrpc":"2.0","id":1,"method":"ping"}`, nil)
	if missing.Code != http.StatusUnauthorized || missing.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("missing bearer status=%d headers=%v", missing.Code, missing.Header())
	}

	badOrigin := serveMCP(t, handler, "secret", "https://attacker.example", `{"jsonrpc":"2.0","id":1,"method":"ping"}`, nil)
	if badOrigin.Code != http.StatusForbidden {
		t.Fatalf("bad origin status=%d body=%s", badOrigin.Code, badOrigin.Body.String())
	}
	if authenticator.calls != 0 {
		t.Fatalf("authenticator called %d times before origin rejection", authenticator.calls)
	}

	allowed := serveMCP(t, handler, "secret", "https://console.example", `{"jsonrpc":"2.0","id":1,"method":"ping"}`, nil)
	if allowed.Code != http.StatusOK {
		t.Fatalf("allowed origin status=%d body=%s", allowed.Code, allowed.Body.String())
	}
}

func TestHTTPHandlerMapsToolErrorsAndProtocolErrors(t *testing.T) {
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextExplore)
	authenticator := &fakeAgentTokenAuthenticator{token: "secret", principal: principal}
	registry, err := NewRegistry(ToolSpec{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) {
		return nil, kernel.Error{Code: kernel.CodeLeaseConflict, Message: "lease changed", Recoverable: true}
	})})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{})
	if err != nil {
		t.Fatal(err)
	}

	toolFailure := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"context.explore","arguments":{}}}`, nil)
	var failed struct {
		Error  *rpcError      `json:"error"`
		Result toolCallResult `json:"result"`
	}
	decodeRecorderJSON(t, toolFailure, &failed)
	if failed.Error != nil || !failed.Result.IsError || failed.Result.StructuredContent["code"] != string(kernel.CodeLeaseConflict) {
		t.Fatalf("tool failure = %#v", failed)
	}

	unknown := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"unknown.tool","arguments":{}}}`, nil)
	var unknownResponse rpcResponse
	decodeRecorderJSON(t, unknown, &unknownResponse)
	if unknownResponse.Error == nil || unknownResponse.Error.Code != -32602 {
		t.Fatalf("unknown response = %#v", unknownResponse)
	}

	arrayArguments := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"context.explore","arguments":[]}}`, nil)
	var arrayResponse rpcResponse
	decodeRecorderJSON(t, arrayArguments, &arrayResponse)
	if arrayResponse.Error == nil || arrayResponse.Error.Code != -32602 {
		t.Fatalf("array response = %#v", arrayResponse)
	}

	trailing := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":4,"method":"ping"} {}`, nil)
	var trailingResponse rpcResponse
	decodeRecorderJSON(t, trailing, &trailingResponse)
	if trailingResponse.Error == nil || trailingResponse.Error.Code != -32600 || string(trailingResponse.ID) != "null" {
		t.Fatalf("trailing response = %#v", trailingResponse)
	}
}

func TestHTTPHandlerAcceptsInitializedNotificationAndRejectsStatefulMethods(t *testing.T) {
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextExplore)
	authenticator := &fakeAgentTokenAuthenticator{token: "secret", principal: principal}
	registry, err := NewRegistry(ToolSpec{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) {
		return nil, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{})
	if err != nil {
		t.Fatal(err)
	}

	notification := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","method":"notifications/initialized"}`, nil)
	if notification.Code != http.StatusAccepted || notification.Body.Len() != 0 {
		t.Fatalf("notification status=%d body=%q", notification.Code, notification.Body.String())
	}

	request := httptest.NewRequest(http.MethodGet, "http://threadmill.test/mcp", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}
}

func TestDefinitionSchemasMatchStrictJSONFieldNames(t *testing.T) {
	definition := definitionForTool(auth.ToolContextExplore)
	properties, ok := definition.InputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema properties = %#v", definition.InputSchema["properties"])
	}
	if _, ok := properties["anchor_ref"]; !ok {
		t.Fatalf("schema properties = %#v", properties)
	}
	if definition.InputSchema["additionalProperties"] != false {
		t.Fatalf("schema should mirror strict decoder: %#v", definition.InputSchema)
	}
	if got := definitionForTool(auth.ToolCoordinationReplacePending).InputSchema["required"]; !reflect.DeepEqual(got, []string{"endpoints"}) {
		t.Fatalf("replace pending required = %#v", got)
	}
	if got := definitionForTool(auth.ToolContextListSubgraphs).InputSchema["required"]; got != nil {
		t.Fatalf("list subgraphs required = %#v, want none", got)
	}
	if got := definitionForTool(auth.ToolContextExplore).InputSchema["required"]; got != nil {
		t.Fatalf("context explore required = %#v, want none", got)
	}
	if got := definitionForTool(auth.ToolContextSearch).InputSchema["required"]; got != nil {
		t.Fatalf("context search required = %#v, want none", got)
	}
	if got := definitionForTool(auth.ToolWorkspaceRead).InputSchema["required"]; !reflect.DeepEqual(got, []string{"path"}) {
		t.Fatalf("workspace read required = %#v", got)
	}
	empty := definitionForTool(auth.ToolCoordinationTransition).InputSchema
	if got := empty["properties"]; !reflect.DeepEqual(got, map[string]any{}) {
		t.Fatalf("empty schema properties = %#v", got)
	}
}

func TestHTTPHandlerRejectsUnsupportedProtocolHeaderWithHTTP400(t *testing.T) {
	principal := principalWithTools(auth.RoleExecutor, auth.ToolContextExplore)
	authenticator := &fakeAgentTokenAuthenticator{token: "secret", principal: principal}
	registry, err := NewRegistry(ToolSpec{ID: auth.ToolContextExplore, Handler: HandlerFunc(func(context.Context, auth.Principal, auth.BoundScope, json.RawMessage) (any, error) {
		return nil, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(authenticator, registry, HTTPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	response := serveMCP(t, handler, "secret", "", `{"jsonrpc":"2.0","id":1,"method":"ping"}`, map[string]string{"MCP-Protocol-Version": "2099-01-01"})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported version status=%d body=%s", response.Code, response.Body.String())
	}
	var payload rpcResponse
	decodeRecorderJSON(t, response, &payload)
	if payload.Error == nil || payload.Error.Code != -32600 {
		t.Fatalf("unsupported version response = %#v", payload)
	}
}

func serveMCP(t *testing.T, handler http.Handler, token, origin, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "http://threadmill.test/mcp", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeRecorderJSON(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, target); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
}
