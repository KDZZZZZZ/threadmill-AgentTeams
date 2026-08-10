package demo

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInitialStateAndInspector(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()

	state := getState(t, ts.URL)
	if state.GraphRevision != 0 {
		t.Fatalf("graph revision = %d, want 0", state.GraphRevision)
	}
	if state.Capacity.Active != 2 {
		t.Fatalf("active = %d, want 2", state.Capacity.Active)
	}
	if len(state.Tasks) < 5 || len(state.Endpoints) < 5 || len(state.Edges) == 0 {
		t.Fatalf("state is too small: tasks=%d endpoints=%d edges=%d", len(state.Tasks), len(state.Endpoints), len(state.Edges))
	}
	if countPhase(state.Endpoints, "active") < 1 || countRunnable(state.Endpoints) < 2 {
		t.Fatalf("expected active and multiple runnable pending endpoints: %#v", state.Endpoints)
	}

	resp, err := http.Get(ts.URL + "/api/endpoints/ep-execute/inspector")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("inspector status = %d", resp.StatusCode)
	}
	var inspector Inspector
	if err := json.NewDecoder(resp.Body).Decode(&inspector); err != nil {
		t.Fatal(err)
	}
	if inspector.Current == nil {
		t.Fatal("expected current invocation")
	}
	if len(inspector.Subscriptions) < 4 || len(inspector.ContextSlice) == 0 || len(inspector.TaskMemoryBuffer) == 0 {
		t.Fatalf("inspector missing semantic projections: %#v", inspector)
	}
	if len(inspector.Candidates) == 0 || inspector.Candidates[0].CreatedByInvocationID == "" {
		t.Fatalf("candidates missing CreatedByInvocationID: %#v", inspector.Candidates)
	}
}

func TestCapacityCASDispatchAndNoGraphMutation(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()

	state := getState(t, ts.URL)
	body := postJSON(t, ts.URL+"/api/capacity", capacityRequest{Desired: 1, ExpectedRevision: state.Capacity.Revision})
	var decreased State
	decodeBody(t, body, &decreased)
	if decreased.GraphRevision != state.GraphRevision {
		t.Fatalf("capacity changed graph revision: got %d want %d", decreased.GraphRevision, state.GraphRevision)
	}
	if decreased.Capacity.Active != state.Capacity.Active {
		t.Fatalf("decrease killed active work: active got %d want %d", decreased.Capacity.Active, state.Capacity.Active)
	}

	body = postJSON(t, ts.URL+"/api/capacity", capacityRequest{Desired: 4, ExpectedRevision: decreased.Capacity.Revision})
	var increased State
	decodeBody(t, body, &increased)
	if increased.Capacity.Active <= decreased.Capacity.Active {
		t.Fatalf("increase did not dispatch more runnable endpoints: before=%d after=%d", decreased.Capacity.Active, increased.Capacity.Active)
	}
	if increased.GraphRevision != state.GraphRevision {
		t.Fatalf("capacity dispatch changed graph revision: got %d want %d", increased.GraphRevision, state.GraphRevision)
	}

	resp, err := http.Post(ts.URL+"/api/capacity", "application/json", bytes.NewReader(mustJSON(capacityRequest{Desired: 2, ExpectedRevision: decreased.Capacity.Revision})))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale capacity CAS status = %d, want 409", resp.StatusCode)
	}
}

func TestManagerIntentsAreTraceableAndCASGuarded(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()

	state := getState(t, ts.URL)
	var hold struct {
		InputRef      string `json:"input_ref"`
		DecisionRef   string `json:"decision_ref"`
		GraphRevision int64  `json:"graph_revision"`
		State         State  `json:"state"`
	}
	body := postJSON(t, ts.URL+"/api/manager/messages", managerMessageRequest{Text: "hold this endpoint", EndpointID: "ep-check", ExpectedRevision: state.GraphRevision})
	decodeBody(t, body, &hold)
	if hold.InputRef == "" || hold.DecisionRef == "" || hold.GraphRevision != state.GraphRevision+1 {
		t.Fatalf("hold response not traceable: %#v", hold)
	}
	held := endpointByIDLocal(hold.State.Endpoints, "ep-check")
	if held == nil || !held.Held || held.Phase != "held" || held.Checkpoint == "" {
		t.Fatalf("endpoint not held with checkpoint: %#v", held)
	}

	body = postJSON(t, ts.URL+"/api/manager/messages", managerMessageRequest{Text: "恢复继续", EndpointID: "ep-check", ExpectedRevision: hold.GraphRevision})
	var resume struct {
		GraphRevision int64 `json:"graph_revision"`
		State         State `json:"state"`
	}
	decodeBody(t, body, &resume)
	resumed := endpointByIDLocal(resume.State.Endpoints, "ep-check")
	if resumed == nil || resumed.Held || resumed.Generation < 2 || resumed.LeaseID == "" {
		t.Fatalf("endpoint not resumed with generation/lease: %#v", resumed)
	}

	body = postJSON(t, ts.URL+"/api/manager/messages", managerMessageRequest{Text: "完成并通过", EndpointID: "ep-execute", ExpectedRevision: resume.GraphRevision})
	var satisfy struct {
		GraphRevision int64 `json:"graph_revision"`
		State         State `json:"state"`
	}
	decodeBody(t, body, &satisfy)
	satisfied := endpointByIDLocal(satisfy.State.Endpoints, "ep-execute")
	if satisfied == nil || !satisfied.Satisfied || satisfied.FormalOutput == "" {
		t.Fatalf("endpoint not satisfied: %#v", satisfied)
	}

	resp, err := http.Post(ts.URL+"/api/manager/messages", "application/json", bytes.NewReader(mustJSON(managerMessageRequest{Text: "dependency", EndpointID: "ep-review", ExpectedRevision: resume.GraphRevision})))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale graph CAS status = %d, want 409", resp.StatusCode)
	}
}

func TestSSESendsInitialAndChangeEvents(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sse status = %d", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	if !readUntilData(t, reader, time.Second) {
		t.Fatal("missing initial SSE state")
	}

	state := getState(t, ts.URL)
	postJSON(t, ts.URL+"/api/manager/messages", managerMessageRequest{Text: "暂停", EndpointID: "ep-check", ExpectedRevision: state.GraphRevision})
	if !readUntilData(t, reader, time.Second) {
		t.Fatal("missing change SSE state")
	}
}

func getState(t *testing.T, baseURL string) State {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("state status = %d", resp.StatusCode)
	}
	var state State
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func postJSON(t *testing.T, url string, payload any) []byte {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(mustJSON(payload)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("POST %s status = %d", url, resp.StatusCode)
	}
	var out bytes.Buffer
	if _, err := out.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func decodeBody(t *testing.T, body []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode %s: %v", string(body), err)
	}
}

func countPhase(endpoints []Endpoint, phase string) int {
	count := 0
	for _, ep := range endpoints {
		if ep.Phase == phase {
			count++
		}
	}
	return count
}

func countRunnable(endpoints []Endpoint) int {
	count := 0
	for _, ep := range endpoints {
		if ep.Phase == "pending" && !ep.Held && !ep.Satisfied {
			count++
		}
	}
	return count
}

func endpointByIDLocal(endpoints []Endpoint, id string) *Endpoint {
	for i := range endpoints {
		if endpoints[i].ID == id {
			return &endpoints[i]
		}
	}
	return nil
}

func readUntilData(t *testing.T, reader *bufio.Reader, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			return true
		}
	}
	return false
}
