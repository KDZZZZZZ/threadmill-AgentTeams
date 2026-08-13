package mcpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	latestProtocolVersion = "2025-11-25"
	defaultMaxRequestSize = int64(1 << 20)
)

var supportedProtocolVersions = map[string]struct{}{
	"2025-11-25": {},
	"2025-06-18": {},
	"2025-03-26": {},
	"2024-11-05": {},
}

type AgentTokenAuthenticator interface {
	AuthenticateAgentToken(context.Context, string) (auth.Principal, error)
}

type HTTPOptions struct {
	AllowedOrigins []string
	MaxRequestSize int64
	ServerName     string
	ServerVersion  string
}

type httpServer struct {
	authenticator AgentTokenAuthenticator
	registry      *Registry
	allowedOrigin map[string]struct{}
	maxBody       int64
	serverName    string
	serverVersion string
}

// NewHTTPHandler exposes the invocation-scoped Registry through MCP
// Streamable HTTP. The server is deliberately stateless: it supports JSON
// responses over POST and returns 405 for GET/DELETE because Threadmill does
// not currently emit server-initiated MCP messages.
func NewHTTPHandler(authenticator AgentTokenAuthenticator, registry *Registry, options HTTPOptions) (http.Handler, error) {
	if authenticator == nil {
		return nil, fmt.Errorf("MCP agent token authenticator is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("MCP registry is required")
	}
	maxBody := options.MaxRequestSize
	if maxBody == 0 {
		maxBody = defaultMaxRequestSize
	}
	if maxBody < 0 {
		return nil, fmt.Errorf("MCP max request size must be positive")
	}
	serverName := strings.TrimSpace(options.ServerName)
	if serverName == "" {
		serverName = "threadmill"
	}
	serverVersion := strings.TrimSpace(options.ServerVersion)
	if serverVersion == "" {
		serverVersion = "dev"
	}
	allowed := make(map[string]struct{}, len(options.AllowedOrigins))
	for _, origin := range options.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			return nil, fmt.Errorf("MCP allowed origin cannot be empty")
		}
		allowed[origin] = struct{}{}
	}
	return &httpServer{
		authenticator: authenticator,
		registry:      registry,
		allowedOrigin: allowed,
		maxBody:       maxBody,
		serverName:    serverName,
		serverVersion: serverVersion,
	}, nil
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.originAllowed(r.Header.Get("Origin")) {
		writeHTTPError(w, http.StatusForbidden, kernel.Error{Code: kernel.CodeOriginInvalid, Message: "request origin is not allowed"})
		return
	}
	principal, err := s.authenticate(r)
	if err != nil {
		status := http.StatusUnauthorized
		if !kernel.IsCode(err, kernel.CodeUnauthorized) {
			status = http.StatusInternalServerError
		}
		if status == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Bearer realm="threadmill-mcp"`)
		}
		writeHTTPError(w, status, publicTransportError(err))
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r, principal)
	case http.MethodGet, http.MethodDelete:
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "stateless MCP endpoint only supports POST"})
	default:
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "method is not allowed"})
	}
}

func (s *httpServer) originAllowed(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	_, ok := s.allowedOrigin[origin]
	return ok
}

func (s *httpServer) authenticate(r *http.Request) (auth.Principal, error) {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return auth.Principal{}, kernel.Error{Code: kernel.CodeUnauthorized, Message: "valid bearer token is required"}
	}
	return s.authenticator.AuthenticateAgentToken(r.Context(), parts[1])
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type initializeParams struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    json.RawMessage `json:"capabilities,omitempty"`
	ClientInfo      json.RawMessage `json:"clientInfo,omitempty"`
	Meta            json.RawMessage `json:"_meta,omitempty"`
}

type toolsListParams struct {
	Cursor string          `json:"cursor,omitempty"`
	Meta   json.RawMessage `json:"_meta,omitempty"`
}

type toolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Meta      json.RawMessage `json:"_meta,omitempty"`
}

type toolTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolCallResult struct {
	Content           []toolTextContent `json:"content"`
	StructuredContent map[string]any    `json:"structuredContent,omitempty"`
	IsError           bool              `json:"isError,omitempty"`
}

func (s *httpServer) handlePost(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeHTTPError(w, http.StatusUnsupportedMediaType, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "Content-Type must be application/json"})
		return
	}

	request, decodeErr := decodeRPCRequest(http.MaxBytesReader(w, r.Body, s.maxBody))
	if decodeErr != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(decodeErr, &tooLarge) {
			writeHTTPError(w, http.StatusRequestEntityTooLarge, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "MCP request body is too large"})
			return
		}
		code := -32600
		message := "invalid JSON-RPC request"
		var syntax *json.SyntaxError
		if errors.As(decodeErr, &syntax) || errors.Is(decodeErr, io.ErrUnexpectedEOF) {
			code = -32700
			message = "parse error"
		}
		writeRPCError(w, nullRPCID(), code, message, nil)
		return
	}

	if request.Method != "initialize" {
		if version := strings.TrimSpace(r.Header.Get("MCP-Protocol-Version")); version != "" {
			if _, ok := supportedProtocolVersions[version]; !ok {
				writeRPCErrorStatus(w, http.StatusBadRequest, request.responseID(), -32600, "unsupported MCP protocol version", map[string]any{"supported": sortedProtocolVersions()})
				return
			}
		}
	}

	if request.isNotification() {
		if request.Method == "notifications/initialized" || strings.HasPrefix(request.Method, "notifications/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch request.Method {
	case "initialize":
		s.handleInitialize(w, request)
	case "ping":
		writeRPCResult(w, request.ID, map[string]any{})
	case "tools/list":
		s.handleToolsList(w, request, principal)
	case "tools/call":
		s.handleToolsCall(r.Context(), w, request, principal)
	default:
		writeRPCError(w, request.ID, -32601, "method not found", nil)
	}
}

func decodeRPCRequest(reader io.Reader) (rpcRequest, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var request rpcRequest
	if err := decoder.Decode(&request); err != nil {
		return rpcRequest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return rpcRequest{}, fmt.Errorf("trailing JSON data")
	}
	if request.JSONRPC != "2.0" || strings.TrimSpace(request.Method) == "" {
		return rpcRequest{}, fmt.Errorf("invalid JSON-RPC envelope")
	}
	if len(request.ID) > 0 && !validRPCID(request.ID) {
		return rpcRequest{}, fmt.Errorf("invalid JSON-RPC id")
	}
	return request, nil
}

func validRPCID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var text string
	if json.Unmarshal(trimmed, &text) == nil {
		return true
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var number json.Number
	if decoder.Decode(&number) != nil {
		return false
	}
	value := number.String()
	return !strings.ContainsAny(value, ".eE")
}

func (r rpcRequest) isNotification() bool {
	return len(bytes.TrimSpace(r.ID)) == 0
}

func (r rpcRequest) responseID() json.RawMessage {
	if r.isNotification() {
		return nullRPCID()
	}
	return r.ID
}

func (s *httpServer) handleInitialize(w http.ResponseWriter, request rpcRequest) {
	var params initializeParams
	if err := decodeStrictParams(request.Params, &params); err != nil || strings.TrimSpace(params.ProtocolVersion) == "" {
		writeRPCError(w, request.ID, -32602, "invalid initialize parameters", nil)
		return
	}
	version := params.ProtocolVersion
	if _, ok := supportedProtocolVersions[version]; !ok {
		version = latestProtocolVersion
	}
	w.Header().Set("MCP-Protocol-Version", version)
	writeRPCResult(w, request.ID, map[string]any{
		"protocolVersion": version,
		"capabilities": map[string]any{
			"tools": map[string]any{"listChanged": false},
		},
		"serverInfo": map[string]any{
			"name":    s.serverName,
			"version": s.serverVersion,
		},
	})
}

func (s *httpServer) handleToolsList(w http.ResponseWriter, request rpcRequest, principal auth.Principal) {
	var params toolsListParams
	if err := decodeStrictParams(request.Params, &params); err != nil || strings.TrimSpace(params.Cursor) != "" {
		writeRPCError(w, request.ID, -32602, "invalid tools/list parameters", nil)
		return
	}
	visible := s.registry.VisibleToolIDs(principal)
	tools := make([]toolDefinition, 0, len(visible))
	for _, tool := range visible {
		tools = append(tools, definitionForTool(tool))
	}
	writeRPCResult(w, request.ID, map[string]any{"tools": tools})
}

func (s *httpServer) handleToolsCall(ctx context.Context, w http.ResponseWriter, request rpcRequest, principal auth.Principal) {
	var params toolsCallParams
	if err := decodeStrictParams(request.Params, &params); err != nil || strings.TrimSpace(params.Name) == "" {
		writeRPCError(w, request.ID, -32602, "invalid tools/call parameters", nil)
		return
	}
	tool := auth.Tool(params.Name)
	if _, registered := s.registry.AvailableTools()[tool]; !registered {
		writeRPCError(w, request.ID, -32602, "unknown tool", nil)
		return
	}
	arguments, err := normalizeToolArguments(params.Arguments)
	if err != nil {
		writeRPCError(w, request.ID, -32602, "tool arguments must be a JSON object", nil)
		return
	}
	result, err := s.registry.Invoke(ctx, principal, tool, auth.Scope{}, arguments)
	if err != nil {
		writeRPCResult(w, request.ID, errorToolResult(err))
		return
	}
	writeRPCResult(w, request.ID, successToolResult(result))
}

func decodeStrictParams(raw json.RawMessage, target any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		trimmed = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing params")
	}
	return nil
}

func normalizeToolArguments(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return json.RawMessage(`{}`), nil
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &value); err != nil || value == nil {
		return nil, fmt.Errorf("arguments are not an object")
	}
	return append(json.RawMessage(nil), trimmed...), nil
}

func successToolResult(value any) toolCallResult {
	encoded, err := json.Marshal(value)
	if err != nil {
		return errorToolResult(kernel.Error{Code: kernel.CodeInternalError, Message: "tool result is not serializable"})
	}
	text := string(encoded)
	structured := make(map[string]any)
	if len(encoded) == 0 || bytes.Equal(encoded, []byte("null")) {
		text = "{}"
	} else if err := json.Unmarshal(encoded, &structured); err != nil || structured == nil {
		structured = map[string]any{"result": value}
	}
	return toolCallResult{
		Content:           []toolTextContent{{Type: "text", Text: text}},
		StructuredContent: structured,
	}
}

func errorToolResult(err error) toolCallResult {
	stable := publicTransportError(err)
	encoded, _ := json.Marshal(stable)
	structured := map[string]any{
		"code":        stable.Code,
		"message":     stable.Message,
		"recoverable": stable.Recoverable,
	}
	if len(stable.Details) > 0 {
		structured["details"] = stable.Details
	}
	return toolCallResult{
		Content:           []toolTextContent{{Type: "text", Text: string(encoded)}},
		StructuredContent: structured,
		IsError:           true,
	}
}

func publicTransportError(err error) kernel.Error {
	if err == nil {
		return kernel.Error{}
	}
	var stable kernel.Error
	if errors.As(err, &stable) {
		return stable
	}
	var stablePointer *kernel.Error
	if errors.As(err, &stablePointer) && stablePointer != nil {
		return *stablePointer
	}
	return kernel.Error{Code: kernel.CodeInternalError, Message: "internal runtime error"}
}

func sortedProtocolVersions() []string {
	return []string{"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"}
}

func nullRPCID() json.RawMessage {
	return json.RawMessage("null")
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string, data any) {
	writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: data}})
}

func writeRPCErrorStatus(w http.ResponseWriter, status int, id json.RawMessage, code int, message string, data any) {
	writeRPCStatus(w, status, rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message, Data: data}})
}

func writeRPC(w http.ResponseWriter, response rpcResponse) {
	writeRPCStatus(w, http.StatusOK, response)
}

func writeRPCStatus(w http.ResponseWriter, status int, response rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func writeHTTPError(w http.ResponseWriter, status int, err kernel.Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(err)
}
