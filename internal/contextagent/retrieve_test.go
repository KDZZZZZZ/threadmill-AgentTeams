package contextagent

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type recordingSearcher struct {
	principal auth.Principal
	req       contextgraph.SearchRequest
	result    contextgraph.ContextSearchResult
	err       error
	calls     int
}

func (s *recordingSearcher) Search(_ context.Context, principal auth.Principal, req contextgraph.SearchRequest) (contextgraph.ContextSearchResult, error) {
	s.calls++
	s.principal = principal
	s.req = req
	return s.result, s.err
}

func TestBuildSearchRequestExtractsKeywordsScopeAndAnchors(t *testing.T) {
	req := BuildSearchRequest("Find release safety in subgraph:General-A near node:Node-1 and node:Node-1")
	if want := []string{"release", "safety"}; !reflect.DeepEqual(req.Keywords, want) {
		t.Fatalf("keywords = %#v, want %#v", req.Keywords, want)
	}
	if want := []string{"subgraph:general-a"}; !reflect.DeepEqual(req.Scope, want) {
		t.Fatalf("scope = %#v, want %#v", req.Scope, want)
	}
	if want := []string{"subgraph:general-a", "node:node-1"}; !reflect.DeepEqual(req.AnchorRefs, want) {
		t.Fatalf("anchors = %#v, want %#v", req.AnchorRefs, want)
	}
}

func TestRetrieveRejectsEmptyQueryAndUntrustedPrincipalWithoutSearching(t *testing.T) {
	searcher := &recordingSearcher{}
	agent := Agent{Searcher: searcher}
	trusted := trustedRetrievePrincipal()
	for _, query := range []string{"", "   "} {
		if _, err := agent.Retrieve(context.Background(), trusted, ContextRetrieveRequest{Query: query}); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
			t.Fatalf("empty query err = %v, want invalid_request", err)
		}
	}
	phase := auth.Principal{
		Role:         auth.RoleExecutor,
		ProjectID:    "project-a",
		TaskID:       "task-a",
		InvocationID: "inv-original",
		Tools:        auth.ToolSet(auth.ToolContextAgentRetrieve),
	}
	if _, err := agent.Retrieve(context.Background(), phase, ContextRetrieveRequest{Query: "release safety"}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("phase principal retrieve err = %v, want forbidden", err)
	}
	if searcher.calls != 0 {
		t.Fatalf("search called %d times for invalid retrieve requests", searcher.calls)
	}
}

func TestRetrieveDelegatesSearchWithTrustedConsumerPrincipal(t *testing.T) {
	searcher := &recordingSearcher{result: contextgraph.ContextSearchResult{
		Slice: contextgraph.ContextSliceDelta{
			Nodes: []contextgraph.ContextNode{{ID: "n1", Statement: "release safety"}},
		},
		SubscriptionIDs: []string{"sub-1"},
	}}
	principal := trustedRetrievePrincipal()
	result, err := (Agent{Searcher: searcher}).Retrieve(context.Background(), principal, ContextRetrieveRequest{Query: "release safety"})
	if err != nil {
		t.Fatal(err)
	}
	if searcher.principal.ConsumerInvocationID != "inv-original" || searcher.principal.InvocationID != "inv-context" {
		t.Fatalf("search principal lost trusted consumer binding: %#v", searcher.principal)
	}
	if want := []string{"release", "safety"}; !reflect.DeepEqual(searcher.req.Keywords, want) {
		t.Fatalf("search request = %#v, want keywords %#v", searcher.req, want)
	}
	if !reflect.DeepEqual(result.SubscriptionIDs, []string{"sub-1"}) || len(result.Slice.Nodes) != 1 {
		t.Fatalf("retrieve result = %#v", result)
	}
}

func TestRetrieveThroughRuntimeDispatcherBindsSearchSubscriptionToOriginalConsumer(t *testing.T) {
	store := contextgraph.NewMemoryStore(func() time.Time { return time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC) })
	curator := auth.Principal{
		ActorPrincipalID: "ctx-curator",
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		Role:             auth.RoleContext,
		Operation:        "curate",
		InvocationID:     "inv-curate",
		Tools:            auth.ToolSet(auth.ToolContextCreateSubgraph, auth.ToolContextCreateNode),
	}
	subgraph, err := store.CreateSubgraph(context.Background(), curator, contextgraph.CreateGeneralSubgraphRequest{Name: "General"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateNode(context.Background(), curator, contextgraph.CreateGeneralNodeRequest{
		Statement:   "search-hit release safety",
		Kind:        string(contextgraph.NodeKindFact),
		SourceRefs:  []string{"source:1"},
		SubgraphIDs: []string{subgraph.ID},
	}); err != nil {
		t.Fatal(err)
	}
	caller := auth.Principal{
		ActorPrincipalID: "phase-agent",
		Kind:             auth.PrincipalAgent,
		ProjectID:        "project-a",
		TaskID:           "task-a",
		InvocationID:     "inv-original",
		Role:             auth.RoleExecutor,
		Tools:            auth.ToolSet(auth.ToolContextAgentRetrieve, auth.ToolContextUnsubscribe),
	}
	dispatcher := runtimeRetrieveDispatcher{agent: Agent{Searcher: store}}
	result, err := dispatcher.Retrieve(context.Background(), caller, ContextRetrieveRequest{Query: "search-hit"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SubscriptionIDs) != 1 {
		t.Fatalf("subscription ids = %#v", result.SubscriptionIDs)
	}
	inspected, err := store.InspectSubscriptions(context.Background(), caller, caller.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inspected) != 1 || inspected[0].ID != result.SubscriptionIDs[0] || inspected[0].ConsumerInvocationID != "inv-original" {
		t.Fatalf("inspected subscriptions = %#v", inspected)
	}
	if err := store.Unsubscribe(context.Background(), caller, result.SubscriptionIDs[0]); err != nil {
		t.Fatalf("original caller could not unsubscribe search subscription: %v", err)
	}
}

type runtimeRetrieveDispatcher struct {
	agent Agent
}

func (d runtimeRetrieveDispatcher) Retrieve(ctx context.Context, caller auth.Principal, req ContextRetrieveRequest) (ContextRetrieveResult, error) {
	contextPrincipal := auth.Principal{
		ActorPrincipalID:     "ctx-retrieve",
		Kind:                 auth.PrincipalAgent,
		ProjectID:            caller.ProjectID,
		Role:                 auth.RoleContext,
		Operation:            "retrieve",
		TaskID:               caller.TaskID,
		InvocationID:         "inv-context-retrieve",
		ConsumerInvocationID: caller.InvocationID,
		ConsumerTaskID:       caller.TaskID,
		ConsumerRole:         caller.Role,
		Tools:                auth.ToolSet(auth.ToolContextSearch),
	}
	return d.agent.Retrieve(ctx, contextPrincipal, req)
}

func trustedRetrievePrincipal() auth.Principal {
	return auth.Principal{
		Role:                 auth.RoleContext,
		Operation:            "retrieve",
		ProjectID:            "project-a",
		TaskID:               "task-a",
		InvocationID:         "inv-context",
		ConsumerInvocationID: "inv-original",
		ConsumerTaskID:       "task-a",
		ConsumerRole:         auth.RoleExecutor,
		Tools:                auth.ToolSet(auth.ToolContextSearch),
	}
}
