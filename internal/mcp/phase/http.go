package phasemcp

// HTTPServer is the thin streamable-HTTP MCP transport for Phase Handler. It
// decodes JSON-RPC, gets the opaque token only from an HTTP header, and never
// accepts trusted invocation identity from the request body.

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
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
			items = append(items, map[string]any{"name": tool})
		}
		return map[string]any{"tools": items}, nil
	case "tools/call":
		return s.toolCall(ctx, token, name, args)
	default:
		return nil, errHTTPMethod
	}
}

func (s *HTTPServer) toolCall(ctx context.Context, token, name string, raw json.RawMessage) (any, error) {
	switch name {
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
