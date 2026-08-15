//go:build ignore

// Temporary, auditable M3.8-C fixture. Run with: go run provider.go.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	m "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type rt struct{ out string }

// emptyReader and emptyContextAgent satisfy the full trusted binding contract.
// The MCP-only fixture never invokes these read/retrieval tools.
type emptyReader struct{}

func (emptyReader) ListSubgraphs(context.Context, phaseagent.ListSubgraphsRequest) ([]phaseagent.ContextSubgraph, error) {
	return nil, nil
}
func (emptyReader) Explore(context.Context, phaseagent.ExploreRequest) (phaseagent.ContextSliceDelta, error) {
	return phaseagent.ContextSliceDelta{}, nil
}
func (emptyReader) Subscribe(context.Context, phaseagent.SubscribeRequest) (phaseagent.ContextSubscription, error) {
	return phaseagent.ContextSubscription{}, nil
}
func (emptyReader) Unsubscribe(context.Context, string) error { return nil }

type emptyContextAgent struct{}

func (emptyContextAgent) Retrieve(context.Context, phaseagent.ContextRetrieveRequest) (phaseagent.ContextRetrieveResult, error) {
	return phaseagent.ContextRetrieveResult{}, nil
}

func (r rt) AwaitInputs(context.Context, phaseagent.AwaitInputsRequest) (phaseagent.InputWaitResult, error) {
	return phaseagent.InputWaitResult{}, nil
}
func (r rt) SubmitPhaseOutput(_ context.Context, o phaseagent.PhaseOutput) error {
	b, _ := json.Marshal(o)
	return os.WriteFile(r.out, b, 0600)
}
func (rt) ProposeOrchestration(context.Context, phaseagent.OrchestrationProposal) error { return nil }
func (rt) SubmitRequirement(context.Context, phaseagent.Requirement) error              { return nil }
func (rt) ListTaskMemoryCandidates(context.Context) (phaseagent.TaskMemoryBufferView, error) {
	return phaseagent.TaskMemoryBufferView{}, nil
}
func (rt) SubmitMemoryCandidate(context.Context, phaseagent.MemoryCandidate) (phaseagent.CandidateBufferedReceipt, error) {
	return phaseagent.CandidateBufferedReceipt{}, nil
}

type ev struct{ p string }

func (e ev) Record(_ context.Context, x artifacts.Event) error {
	b, _ := json.Marshal(x)
	f, er := os.OpenFile(e.p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if er == nil {
		defer f.Close()
		_, er = f.Write(append(b, '\n'))
	}
	return er
}

type model struct {
	sync.Mutex
	stage    int
	artifact string
}

type tracedMCP struct {
	http.Handler
	expectedTokenHash string
	tracePath         string
}

type capturedResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *capturedResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *capturedResponse) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	_, _ = w.body.Write(body)
	return w.ResponseWriter.Write(body)
}

func (t tracedMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	provided := sha256.Sum256([]byte(r.Header.Get(m.ExecutionTokenHeader)))
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var rpc struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &rpc)
	capture := &capturedResponse{ResponseWriter: w}
	t.Handler.ServeHTTP(capture, r)
	var response struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	_ = json.Unmarshal(capture.body.Bytes(), &response)
	entry, _ := json.Marshal(map[string]any{
		"method":                r.Method,
		"path":                  r.URL.Path,
		"rpc_method":            rpc.Method,
		"tool_name":             rpc.Params.Name,
		"expected_token_sha256": t.expectedTokenHash,
		"provided_token_sha256": fmt.Sprintf("%x", provided),
		"matches_issued_token":  fmt.Sprintf("%x", provided) == t.expectedTokenHash,
		"response_status":       capture.status,
		"response_error":        response.Error.Message,
		"response_content":      response.Result.Content,
		"listed_tools":          response.Result.Tools,
	})
	if t.tracePath != "" {
		if f, err := os.OpenFile(t.tracePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600); err == nil {
			_, _ = f.Write(append(entry, '\n'))
			_ = f.Close()
		}
	}
}

func (x *model) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/models" {
		json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]string{"id": "qwen-plus"}}})
		return
	}
	var q map[string]any
	json.NewDecoder(r.Body).Decode(&q)
	raw, _ := json.Marshal(q)
	x.Lock()
	if x.stage == 1 && x.artifact == "" {
		x.artifact = find(string(raw))
	}
	name, args, id := "", "", ""
	if x.stage == 0 && (strings.Contains(string(raw), "threadmill__artifact_register") || strings.Contains(string(raw), "artifact.register")) {
		name, args, id = "threadmill__artifact_register", `{"controlled_path":"out/report.md","kind":"generated_report"}`, "call-artifact-register"
		x.stage = 1
	} else if x.stage == 1 && x.artifact != "" {
		name, args, id = "threadmill__agent_submitPhaseOutput", fmt.Sprintf(`{"phase":"execute","delivery_refs":[],"report_ref":"%s","evidence_refs":[]}`, x.artifact), "call-submit-phase-output"
		if os.Getenv("M3C_DUPLICATE_CALL_IDS") == "1" {
			id = "call-artifact-register"
		}
		x.stage = 2
	}
	x.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	if name == "" {
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		return
	}
	fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"%s\",\"type\":\"function\",\"function\":{\"name\":\"%s\",\"arguments\":%q}}]},\"finish_reason\":null}]}\n\n", id, name, args)
	fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
}
func find(s string) string {
	return artifactRefPattern.FindString(s)
}

var artifactRefPattern = regexp.MustCompile(`artifact-[0-9a-f]{24}`)

func main() {
	root := os.Getenv("M3C_WORKSPACE")
	if root == "" {
		panic("M3C_WORKSPACE")
	}
	listener, err := net.Listen("tcp", ":18091")
	if err != nil {
		panic(fmt.Sprintf("listen M3.8-C fixture endpoint: %v", err))
	}
	os.MkdirAll(filepath.Join(root, "out"), 0755)
	os.WriteFile(filepath.Join(root, "out", "report.md"), []byte("m3c report\n"), 0600)
	caps, _ := phaseagent.CapabilitiesFor(phaseagent.PhaseExecute)
	if os.Getenv("M3C_DENY_SUBMIT") == "1" {
		caps.AllowOutputSubmission = false
	}
	reg := m.NewBindingRegistry()
	token := "m3c-fixed-test-token" // registry token remains opaque to the agent body
	// Keep the fixture's token out of logs; write it only to the private local file.
	_ = token
	sum := sha256.Sum256([]byte("m3c report\n"))
	_ = sum
	b, err := reg.Issue(m.BoundServices{Binding: m.InvocationBinding{InvocationID: "m3c-invocation", TaskID: "m3c-task", Endpoint: phaseagent.PhaseEndpointRef{TaskID: "m3c-task", EndpointID: "execute"}, Generation: 1, Role: phaseagent.PhaseExecute, BindingRef: "m3c", WorkspaceRoot: root, AllowedDirs: []string{"out"}, Capabilities: caps}, Runtime: rt{os.Getenv("M3C_OUTPUT")}, Reader: emptyReader{}, Agent: emptyContextAgent{}})
	if err != nil {
		panic(fmt.Sprintf("issue trusted M3.8-C binding: %v", err))
	}
	os.WriteFile(os.Getenv("M3C_TOKEN_FILE"), []byte(b.Token), 0600)
	e := ev{os.Getenv("M3C_EVENTS")}
	h, _ := m.NewHandler(reg, artifacts.NewInMemoryRegistry(e), e)
	mux := http.NewServeMux()
	issued := sha256.Sum256([]byte(b.Token))
	mux.Handle("/mcp", tracedMCP{Handler: m.NewHTTPServer(h), expectedTokenHash: fmt.Sprintf("%x", issued), tracePath: os.Getenv("M3C_MCP_TRACE")})
	mux.Handle("/v1/", &model{})
	if err := http.Serve(listener, mux); err != nil {
		panic(fmt.Sprintf("serve M3.8-C fixture endpoint: %v", err))
	}
}
