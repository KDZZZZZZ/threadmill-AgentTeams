package phasemcp

// HTTPServer is the thin streamable-HTTP MCP transport for Phase Handler. It
// decodes JSON-RPC, gets the opaque token only from an HTTP header, and never
// accepts trusted invocation identity from the request body.

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

const ExecutionTokenHeader = "X-Threadmill-Execution-Token"

type HTTPServer struct{ handler *Handler }

func NewHTTPServer(handler *Handler) *HTTPServer { return &HTTPServer{handler: handler} }

func (s *HTTPServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s == nil || s.handler == nil {
		http.Error(writer, "phase MCP is unavailable", http.StatusServiceUnavailable)
		return
	}
	var rpc struct {
		JSONRPC string `json:"jsonrpc"`
		ID      any    `json:"id"`
		Method  string `json:"method"`
		Params  struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"params"`
	}
	if err := json.NewDecoder(request.Body).Decode(&rpc); err != nil {
		writeRPCError(writer, nil, -32700, "parse error")
		return
	}
	token := request.Header.Get(ExecutionTokenHeader)
	result, err := s.call(request.Context(), token, rpc.Method, rpc.Params.Name, rpc.Params.Arguments)
	if err != nil {
		writeRPCError(writer, rpc.ID, -32000, err.Error())
		return
	}
	writeRPCResult(writer, rpc.ID, result)
}

func (s *HTTPServer) call(ctx context.Context, token, method, name string, args json.RawMessage) (any, error) {
	switch method {
	case "initialize":
		return map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "threadmill-phase", "version": "m3.5.2"}}, nil
	case "tools/list":
		tools, err := s.handler.Tools(token)
		if err != nil {
			return nil, err
		}
		items := make([]map[string]any, 0, len(tools))
		for _, tool := range tools {
			items = append(items, mcpToolDescriptor(tool))
		}
		return map[string]any{"tools": items}, nil
	case "tools/call":
		return s.toolCall(ctx, token, name, args)
	default:
		return nil, errHTTPMethod
	}
}

// mcpToolDescriptor supplies the MCP-required inputSchema without exposing
// trusted binding fields. Invocation, Task, endpoint, and credentials remain
// transport-bound and deliberately cannot be provided by an agent argument.
func mcpToolDescriptor(name string) map[string]any {
	descriptor := map[string]any{
		"name":        name,
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
	}
	switch name {
	case ToolAwaitInputs:
		descriptor["inputSchema"] = map[string]any{"type": "object", "properties": map[string]any{"input_ids": map[string]any{"type": "array", "items": map[string]string{"type": "string"}}}}
	case ToolRegisterArtifact:
		descriptor["inputSchema"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"controlled_path": map[string]string{"type": "string"},
				"kind":            map[string]string{"type": "string"},
				"media_type":      map[string]string{"type": "string"},
			},
			"required": []string{"controlled_path", "kind"},
		}
	case ToolSubmitPhaseOutput:
		descriptor["inputSchema"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"phase":         map[string]string{"type": "string"},
				"delivery_refs": map[string]any{"type": "array", "items": map[string]string{"type": "string"}},
				"report_ref":    map[string]string{"type": "string"},
				"evidence_refs": map[string]any{"type": "array", "items": map[string]string{"type": "string"}},
			},
		}
	case ToolConfirmPackageConsumption:
		descriptor["inputSchema"] = map[string]any{
			"type": "object",
			"properties": map[string]any{
				"package_digest":   map[string]string{"type": "string"},
				"session_identity": map[string]string{"type": "string"},
				"consumed":         map[string]string{"type": "boolean"},
			},
			"required": []string{"package_digest", "consumed"},
		}
	}
	return descriptor
}

func (s *HTTPServer) toolCall(ctx context.Context, token, name string, raw json.RawMessage) (any, error) {
	switch name {
	case ToolAwaitInputs:
		var request phaseagent.AwaitInputsRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		result, err := s.handler.AwaitInputs(ctx, token, request)
		if err != nil {
			return nil, err
		}
		return mcpText(result), nil
	case ToolRegisterArtifact:
		var input struct {
			ControlledPath string                 `json:"controlled_path"`
			Kind           artifacts.ArtifactType `json:"kind"`
			MediaType      string                 `json:"media_type"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		ref, err := s.handler.RegisterArtifact(ctx, token, input.ControlledPath, input.Kind, input.MediaType)
		if err != nil {
			return nil, err
		}
		return mcpText(map[string]string{"artifact_ref": string(ref)}), nil
	case ToolSubmitPhaseOutput:
		var output phaseagent.PhaseOutput
		if err := json.Unmarshal(raw, &output); err != nil {
			return nil, err
		}
		if err := s.handler.SubmitPhaseOutput(ctx, token, output); err != nil {
			return nil, err
		}
		return mcpText(map[string]bool{"accepted": true}), nil
	case ToolConfirmPackageConsumption:
		var submission executionreceipt.Submission
		if err := json.Unmarshal(raw, &submission); err != nil {
			return nil, err
		}
		receipt, err := s.handler.ConfirmPackageConsumption(ctx, token, submission)
		if err != nil {
			return nil, err
		}
		return mcpText(receipt), nil
	default:
		return nil, ErrToolDenied
	}
}

func mcpText(value any) map[string]any {
	encoded, _ := json.Marshal(value)
	return map[string]any{"content": []map[string]string{{"type": "text", "text": string(encoded)}}}
}
func writeRPCResult(w http.ResponseWriter, id, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": value})
}
func writeRPCError(w http.ResponseWriter, id any, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}

type httpMethodError string

func (e httpMethodError) Error() string { return string(e) }

const errHTTPMethod = httpMethodError("MCP method is not supported")
