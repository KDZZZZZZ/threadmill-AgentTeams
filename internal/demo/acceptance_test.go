package demo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type acceptanceClient struct {
	t      *testing.T
	server *httptest.Server
}

func newAcceptanceClient(t *testing.T) *acceptanceClient {
	t.Helper()

	ts := httptest.NewServer(NewServer().Handler())
	t.Cleanup(ts.Close)

	return &acceptanceClient{t: t, server: ts}
}

func (c *acceptanceClient) get(path string, want int) map[string]any {
	c.t.Helper()

	req, err := http.NewRequest(http.MethodGet, c.server.URL+path, nil)
	if err != nil {
		c.t.Fatalf("new GET %s: %v", path, err)
	}
	return c.do(req, want)
}

func (c *acceptanceClient) post(path string, body map[string]any, want int) map[string]any {
	c.t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		c.t.Fatalf("marshal POST %s body: %v", path, err)
	}
	req, err := http.NewRequest(http.MethodPost, c.server.URL+path, bytes.NewReader(payload))
	if err != nil {
		c.t.Fatalf("new POST %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, want)
}

func (c *acceptanceClient) do(req *http.Request, want int) map[string]any {
	c.t.Helper()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read %s %s response: %v", req.Method, req.URL.Path, err)
	}
	if resp.StatusCode != want {
		c.t.Fatalf("%s %s status = %d, want %d; body: %s", req.Method, req.URL.Path, resp.StatusCode, want, string(body))
	}
	if len(bytes.TrimSpace(body)) == 0 || !json.Valid(body) {
		return map[string]any{}
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		c.t.Fatalf("decode %s %s response %q: %v", req.Method, req.URL.Path, string(body), err)
	}
	return out
}

func TestIncreasingCapacityStartsMoreRunnableInvocationsWithoutChangingGraphRevision(t *testing.T) {
	c := newAcceptanceClient(t)
	before := c.get("/api/state", http.StatusOK)
	graphBefore := intField(t, before, "graph_revision")
	capacityBefore := objectField(t, before, "capacity")
	activeBefore := intField(t, capacityBefore, "active")

	decreased := c.post("/api/capacity", map[string]any{
		"expected_revision": intField(t, capacityBefore, "revision"),
		"desired":           1,
	}, http.StatusOK)
	assertIntField(t, decreased, "graph_revision", graphBefore)
	assertIntField(t, objectField(t, decreased, "capacity"), "active", activeBefore)

	increased := c.post("/api/capacity", map[string]any{
		"expected_revision": intField(t, objectField(t, decreased, "capacity"), "revision"),
		"desired":           4,
	}, http.StatusOK)
	assertIntField(t, increased, "graph_revision", graphBefore)
	if got := intField(t, objectField(t, increased, "capacity"), "active"); got <= activeBefore {
		t.Fatalf("active invocations after capacity increase = %d, want greater than %d", got, activeBefore)
	}
	if got := intField(t, objectField(t, increased, "capacity"), "revision"); got <= intField(t, capacityBefore, "revision") {
		t.Fatalf("capacity revision = %d, want greater than initial revision", got)
	}
}

func TestOnlyManagerMessagesMutateGraphAndSuccessfulChangesRecordRefsAndRevision(t *testing.T) {
	c := newAcceptanceClient(t)
	before := c.get("/api/state", http.StatusOK)

	c.post("/api/graph/endpoints", map[string]any{
		"name": "browser-direct-mutation",
	}, http.StatusNotFound)
	afterDirectAttempt := c.get("/api/state", http.StatusOK)
	assertIntField(t, afterDirectAttempt, "graph_revision", intField(t, before, "graph_revision"))
	assertSameLength(t, before, afterDirectAttempt, "endpoints")

	changed := c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, before, "graph_revision"),
		"endpoint_id":       "ep-check",
		"text":              "hold this endpoint for checkpoint",
	}, http.StatusOK)
	assertNonEmptyStringField(t, changed, "input_ref")
	assertNonEmptyStringField(t, changed, "decision_ref")
	assertIntField(t, changed, "graph_revision", intField(t, before, "graph_revision")+1)

	event := lastObject(t, arrayField(t, changed, "events"))
	assertNonEmptyStringField(t, event, "input_ref")
	assertNonEmptyStringField(t, event, "decision_ref")
}

func TestHoldStopIsRecoverableWithCheckpointAndResumeCreatesNewGenerationInvocationLeaseAndSubscription(t *testing.T) {
	c := newAcceptanceClient(t)
	before := c.get("/api/state", http.StatusOK)

	heldResponse := c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, before, "graph_revision"),
		"endpoint_id":       "ep-check",
		"text":              "hold this endpoint for checkpoint",
	}, http.StatusOK)
	held := acceptanceEndpointByID(t, arrayField(t, objectField(t, heldResponse, "state"), "endpoints"), "ep-check")
	assertBoolField(t, held, "held", true)
	assertStringContains(t, held, "checkpoint", "checkpoint")

	heldInspector := c.get("/api/endpoints/ep-check/inspector", http.StatusOK)
	oldSubscriptionIDs := stringSetFromObjects(t, arrayField(t, heldInspector, "subscriptions"), "id")
	oldRecent := arrayField(t, heldInspector, "recent")

	resumedResponse := c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, heldResponse, "graph_revision"),
		"endpoint_id":       "ep-check",
		"text":              "resume this endpoint from checkpoint",
	}, http.StatusOK)
	resumed := acceptanceEndpointByID(t, arrayField(t, objectField(t, resumedResponse, "state"), "endpoints"), "ep-check")
	assertBoolField(t, resumed, "held", false)
	if got, previous := intField(t, resumed, "generation"), intField(t, held, "generation"); got <= previous {
		t.Fatalf("generation = %d, want greater than held generation %d", got, previous)
	}
	newLease := stringField(t, resumed, "lease_id")
	if oldLease, ok := held["lease_id"].(string); ok && oldLease != "" && newLease == oldLease {
		t.Fatalf("resume reused lease %q, want a new lease", oldLease)
	}

	resumedInspector := c.get("/api/endpoints/ep-check/inspector", http.StatusOK)
	newRecent := arrayField(t, resumedInspector, "recent")
	if len(newRecent) <= len(oldRecent) {
		t.Fatalf("recent invocations after resume = %d, want more than %d", len(newRecent), len(oldRecent))
	}
	assertStringField(t, firstObject(t, newRecent), "lease_id", newLease)
	assertIntField(t, firstObject(t, newRecent), "generation", intField(t, resumed, "generation"))

	newSubscriptions := arrayField(t, resumedInspector, "subscriptions")
	assertHasNewActiveSubscription(t, newSubscriptions, oldSubscriptionIDs)
	assertOldSubscriptionsInactive(t, newSubscriptions, oldSubscriptionIDs)
}

func TestCompletingEndpointThroughManagerUnlocksDownstreamScheduling(t *testing.T) {
	c := newAcceptanceClient(t)
	state := c.get("/api/state", http.StatusOK)

	state = c.post("/api/capacity", map[string]any{
		"expected_revision": intField(t, objectField(t, state, "capacity"), "revision"),
		"desired":           4,
	}, http.StatusOK)
	state = objectField(t, c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, state, "graph_revision"),
		"endpoint_id":       "ep-execute",
		"text":              "complete this endpoint successfully",
	}, http.StatusOK), "state")
	afterExecuteOnly := acceptanceEndpointByID(t, arrayField(t, state, "endpoints"), "ep-publish")
	if stringField(t, afterExecuteOnly, "phase") == "active" {
		t.Fatalf("ep-publish became active before all prerequisites were completed")
	}

	completedReview := c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, state, "graph_revision"),
		"endpoint_id":       "ep-review",
		"text":              "complete this endpoint successfully",
	}, http.StatusOK)
	after := objectField(t, completedReview, "state")
	publish := acceptanceEndpointByID(t, arrayField(t, after, "endpoints"), "ep-publish")
	assertStringField(t, publish, "phase", "active")
}

func TestInspectorReturnsScopedSubscriptionsEffectiveSubgraphContextSliceAndMemoryCandidates(t *testing.T) {
	c := newAcceptanceClient(t)

	first := c.get("/api/endpoints/ep-execute/inspector", http.StatusOK)
	second := c.get("/api/endpoints/ep-retrieval/inspector", http.StatusOK)

	assertNonEmptyArrayField(t, first, "subscriptions")
	assertNonEmptyArrayField(t, first, "effective_subgraph_union")
	assertNoDuplicateStrings(t, arrayField(t, first, "effective_subgraph_union"))
	assertNonEmptyArrayField(t, first, "context_slice")
	assertNonEmptyArrayField(t, first, "task_memory_buffer")
	assertNonEmptyArrayField(t, first, "candidates")
	assertCandidatesProvenancedByInvocation(t, first)
	assertCandidatesProvenancedByInvocation(t, second)

	if reflect.DeepEqual(first["context_slice"], second["context_slice"]) {
		t.Fatalf("context_slice for different endpoints is identical; want task-specific context")
	}
	if reflect.DeepEqual(first["task_memory_buffer"], second["task_memory_buffer"]) {
		t.Fatalf("task_memory_buffer for different endpoints is identical; want endpoint-filtered candidates")
	}
}

func TestStaleExpectedRevisionsReturnConflictWithoutMutation(t *testing.T) {
	c := newAcceptanceClient(t)
	beforeGraphConflict := c.get("/api/state", http.StatusOK)

	c.post("/api/manager/messages", map[string]any{
		"expected_revision": intField(t, beforeGraphConflict, "graph_revision") - 1,
		"endpoint_id":       "ep-check",
		"text":              "hold stale graph revision",
	}, http.StatusConflict)
	afterGraphConflict := c.get("/api/state", http.StatusOK)
	assertIntField(t, afterGraphConflict, "graph_revision", intField(t, beforeGraphConflict, "graph_revision"))
	assertSameLength(t, beforeGraphConflict, afterGraphConflict, "endpoints")

	beforeCapacityConflict := c.post("/api/capacity", map[string]any{
		"expected_revision": intField(t, objectField(t, afterGraphConflict, "capacity"), "revision"),
		"desired":           1,
	}, http.StatusOK)
	c.post("/api/capacity", map[string]any{
		"expected_revision": intField(t, objectField(t, beforeCapacityConflict, "capacity"), "revision") - 1,
		"desired":           3,
	}, http.StatusConflict)
	afterCapacityConflict := c.get("/api/state", http.StatusOK)
	assertIntField(t, objectField(t, afterCapacityConflict, "capacity"), "revision", intField(t, objectField(t, beforeCapacityConflict, "capacity"), "revision"))
	assertIntField(t, objectField(t, afterCapacityConflict, "capacity"), "desired", intField(t, objectField(t, beforeCapacityConflict, "capacity"), "desired"))
}

func objectField(t *testing.T, obj map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := obj[key]
	if !ok {
		t.Fatalf("missing object field %q in %v", key, obj)
	}
	value, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("field %q = %T, want object", key, raw)
	}
	return value
}

func objectAt(t *testing.T, values []any, index int) map[string]any {
	t.Helper()
	if index >= len(values) {
		t.Fatalf("array index %d out of range %d", index, len(values))
	}
	obj, ok := values[index].(map[string]any)
	if !ok {
		t.Fatalf("array[%d] = %T, want object", index, values[index])
	}
	return obj
}

func firstObject(t *testing.T, values []any) map[string]any {
	t.Helper()
	return objectAt(t, values, 0)
}

func lastObject(t *testing.T, values []any) map[string]any {
	t.Helper()
	return objectAt(t, values, len(values)-1)
}

func arrayField(t *testing.T, obj map[string]any, key string) []any {
	t.Helper()
	raw, ok := obj[key]
	if !ok {
		t.Fatalf("missing array field %q in %v", key, obj)
	}
	if raw == nil {
		return nil
	}
	values, ok := raw.([]any)
	if !ok {
		t.Fatalf("field %q = %T, want array", key, raw)
	}
	return values
}

func stringField(t *testing.T, obj map[string]any, key string) string {
	t.Helper()
	raw, ok := obj[key]
	if !ok {
		t.Fatalf("missing string field %q in %v", key, obj)
	}
	value, ok := raw.(string)
	if !ok {
		t.Fatalf("field %q = %T, want string", key, raw)
	}
	if value == "" {
		t.Fatalf("field %q is empty", key)
	}
	return value
}

func intField(t *testing.T, obj map[string]any, key string) int {
	t.Helper()
	raw, ok := obj[key]
	if !ok {
		t.Fatalf("missing int field %q in %v", key, obj)
	}
	switch value := raw.(type) {
	case float64:
		return int(value)
	case int:
		return value
	default:
		t.Fatalf("field %q = %T, want number", key, raw)
		return 0
	}
}

func assertBoolField(t *testing.T, obj map[string]any, key string, want bool) {
	t.Helper()
	raw, ok := obj[key]
	if !ok {
		t.Fatalf("missing bool field %q in %v", key, obj)
	}
	got, ok := raw.(bool)
	if !ok {
		t.Fatalf("field %q = %T, want bool", key, raw)
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", key, got, want)
	}
}

func assertIntField(t *testing.T, obj map[string]any, key string, want int) {
	t.Helper()
	if got := intField(t, obj, key); got != want {
		t.Fatalf("%s = %d, want %d", key, got, want)
	}
}

func assertStringField(t *testing.T, obj map[string]any, key, want string) {
	t.Helper()
	if got := stringField(t, obj, key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

func assertStringContains(t *testing.T, obj map[string]any, key, want string) {
	t.Helper()
	if got := stringField(t, obj, key); !strings.Contains(got, want) {
		t.Fatalf("%s = %q, want substring %q", key, got, want)
	}
}

func assertNonEmptyStringField(t *testing.T, obj map[string]any, key string) {
	t.Helper()
	_ = stringField(t, obj, key)
}

func assertNonEmptyArrayField(t *testing.T, obj map[string]any, key string) {
	t.Helper()
	if values := arrayField(t, obj, key); len(values) == 0 {
		t.Fatalf("%s is empty", key)
	}
}

func assertSameLength(t *testing.T, before, after map[string]any, key string) {
	t.Helper()
	if got, want := len(arrayField(t, after, key)), len(arrayField(t, before, key)); got != want {
		t.Fatalf("%s length = %d, want %d", key, got, want)
	}
}

func acceptanceEndpointByID(t *testing.T, endpoints []any, id string) map[string]any {
	t.Helper()
	for _, raw := range endpoints {
		endpoint, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("endpoint = %T, want object", raw)
		}
		if endpoint["id"] == id {
			return endpoint
		}
	}
	t.Fatalf("endpoint %q not found", id)
	return nil
}

func stringSetFromObjects(t *testing.T, values []any, key string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, raw := range values {
		obj, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("array contains %T, want object", raw)
		}
		out[stringField(t, obj, key)] = true
	}
	return out
}

func assertHasNewActiveSubscription(t *testing.T, values []any, old map[string]bool) {
	t.Helper()
	for _, raw := range values {
		sub, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("subscription = %T, want object", raw)
		}
		id := stringField(t, sub, "id")
		active, _ := sub["active"].(bool)
		if !old[id] && active {
			return
		}
	}
	t.Fatalf("resume did not create a new active subscription")
}

func assertOldSubscriptionsInactive(t *testing.T, values []any, old map[string]bool) {
	t.Helper()
	for _, raw := range values {
		sub, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("subscription = %T, want object", raw)
		}
		id := stringField(t, sub, "id")
		if old[id] {
			if active, _ := sub["active"].(bool); active {
				t.Fatalf("old subscription %q is still active after resume", id)
			}
		}
	}
}

func assertNoDuplicateStrings(t *testing.T, values []any) {
	t.Helper()
	seen := map[string]bool{}
	for i, raw := range values {
		id, ok := raw.(string)
		if !ok {
			t.Fatalf("value[%d] = %T, want string", i, raw)
		}
		if seen[id] {
			t.Fatalf("contains duplicate value %q", id)
		}
		seen[id] = true
	}
}

func assertCandidatesProvenancedByInvocation(t *testing.T, inspector map[string]any) {
	t.Helper()
	recent := arrayField(t, inspector, "recent")
	if len(recent) == 0 {
		t.Fatalf("inspector has no recent invocations")
	}
	invocationID := stringField(t, firstObject(t, recent), "id")
	for i, raw := range arrayField(t, inspector, "candidates") {
		candidate, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("candidates[%d] = %T, want object", i, raw)
		}
		provenance := fmt.Sprint(candidate["created_by_invocation_id"])
		if !strings.Contains(provenance, invocationID) {
			t.Fatalf("candidates[%d] provenance %q does not include invocation %q", i, provenance, invocationID)
		}
	}
}
