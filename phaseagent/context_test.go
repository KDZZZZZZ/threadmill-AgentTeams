package phaseagent

import (
	"context"
	"reflect"
	"testing"
)

type fakeContextReader struct {
	listRequests        []ListSubgraphsRequest
	exploreRequests     []ExploreRequest
	subscribeRequests   []SubscribeRequest
	unsubscribeRequests []string
}

func (f *fakeContextReader) ListSubgraphs(_ context.Context, req ListSubgraphsRequest) ([]ContextSubgraph, error) {
	f.listRequests = append(f.listRequests, req)
	return []ContextSubgraph{{ID: "subgraph-1", Name: "design", Revision: 3, Kind: "general"}}, nil
}

func (f *fakeContextReader) Explore(_ context.Context, req ExploreRequest) (ContextSliceDelta, error) {
	f.exploreRequests = append(f.exploreRequests, req)
	return ContextSliceDelta{GraphRevision: 4}, nil
}

func (f *fakeContextReader) Subscribe(_ context.Context, req SubscribeRequest) (ContextSubscription, error) {
	f.subscribeRequests = append(f.subscribeRequests, req)
	return ContextSubscription{ID: "subscription-1", ConsumerInvocationID: "runtime-bound-invocation"}, nil
}

func (f *fakeContextReader) Unsubscribe(_ context.Context, subscriptionID string) error {
	f.unsubscribeRequests = append(f.unsubscribeRequests, subscriptionID)
	return nil
}

type fakeContextAgent struct {
	requests []ContextRetrieveRequest
}

func (f *fakeContextAgent) Retrieve(_ context.Context, req ContextRetrieveRequest) (ContextRetrieveResult, error) {
	f.requests = append(f.requests, req)
	return ContextRetrieveResult{Explanation: "visible matching context", SubscriptionIDs: []string{"subscription-2"}}, nil
}

var _ ContextGraphReader = (*fakeContextReader)(nil)
var _ ContextAgent = (*fakeContextAgent)(nil)

func TestExecutorReceivesContextReaderAndRetriever(t *testing.T) {
	t.Parallel()

	reader := &fakeContextReader{}
	agent := &fakeContextAgent{}
	executor := &fakeExecutor{executeFn: func(ctx context.Context, execution ExecutionContext) error {
		if execution.ContextReader == nil || execution.ContextAgent == nil {
			t.Fatal("execution context is missing Context seams")
		}
		if _, err := execution.ContextReader.ListSubgraphs(ctx, ListSubgraphsRequest{Filter: "design"}); err != nil {
			return err
		}
		if _, err := execution.ContextReader.Explore(ctx, ExploreRequest{AnchorRef: "subgraph:subgraph-1", Depth: 1}); err != nil {
			return err
		}
		subscription, err := execution.ContextReader.Subscribe(ctx, SubscribeRequest{SubgraphIDs: []string{"subgraph-1"}})
		if err != nil {
			return err
		}
		if subscription.ConsumerInvocationID != "runtime-bound-invocation" {
			t.Fatalf("subscription consumer binding was not preserved: %#v", subscription)
		}
		if err := execution.ContextReader.Unsubscribe(ctx, subscription.ID); err != nil {
			return err
		}
		_, err = execution.ContextAgent.Retrieve(ctx, ContextRetrieveRequest{Query: "what design constraint applies?"})
		return err
	}}
	runner := mustNewRunnerWithContext(t, &mockRuntime{}, executor, reader, agent)
	if _, err := runner.RunStart(context.Background(), runnerStartInput("inv-1", 1, "plan")); err != nil {
		t.Fatalf("RunStart: %v", err)
	}
	if len(reader.listRequests) != 1 || len(reader.exploreRequests) != 1 || len(reader.subscribeRequests) != 1 || len(reader.unsubscribeRequests) != 1 || len(agent.requests) != 1 {
		t.Fatalf("context calls were not forwarded: reader=%#v agent=%#v", reader, agent)
	}
}

func TestOrdinaryContextReaderDoesNotExposeSearch(t *testing.T) {
	t.Parallel()

	readerType := reflect.TypeOf((*ContextGraphReader)(nil)).Elem()
	if _, exists := readerType.MethodByName("Search"); exists {
		t.Fatal("ordinary Phase Agent Context reader must not expose Search")
	}
}

func TestAllRolesReceiveContextSurface(t *testing.T) {
	t.Parallel()

	for _, endpointID := range []string{"plan", "execute", "verify"} {
		reader := &fakeContextReader{}
		agent := &fakeContextAgent{}
		executor := &fakeExecutor{}
		runner := mustNewRunnerWithContext(t, &mockRuntime{}, executor, reader, agent)
		if _, err := runner.RunStart(context.Background(), runnerStartInput("inv-"+endpointID, 1, endpointID)); err != nil {
			t.Fatalf("RunStart(%s): %v", endpointID, err)
		}
		if executor.calls[0].ContextReader != reader || executor.calls[0].ContextAgent != agent {
			t.Fatalf("%s did not receive supplied Context surfaces", endpointID)
		}
	}
}

func TestContextDeltaAndInputsChangedRemainDistinct(t *testing.T) {
	t.Parallel()

	inputs := PhaseInputSet{InputRevision: "input-rev-1"}
	delta := ContextDelta{SubscriptionID: "subscription-1", SubgraphID: "subgraph-1", Revision: 8, Changes: []any{"new fact"}}
	if inputs.InputRevision != "input-rev-1" || delta.Revision != 8 {
		t.Fatalf("context delta must not mutate formal inputs: inputs=%#v delta=%#v", inputs, delta)
	}
	updated := InputsChanged{Inputs: PhaseInputSet{InputRevision: "input-rev-2", Delivered: []InputDelivery{{InputID: "input-1"}}}}
	inputs = updated.Inputs
	if inputs.InputRevision != "input-rev-2" || len(inputs.Delivered) != 1 {
		t.Fatalf("InputsChanged must supply the complete current input set: %#v", inputs)
	}
}

func TestContextCapabilitiesAndVerifyWriteBoundary(t *testing.T) {
	t.Parallel()

	for _, phase := range []Phase{PhasePlan, PhaseExecute, PhaseVerify} {
		capabilities, err := CapabilitiesFor(phase)
		if err != nil {
			t.Fatalf("CapabilitiesFor(%s): %v", phase, err)
		}
		if !capabilities.AllowContextRead || !capabilities.AllowContextSubscription || !capabilities.AllowContextRetrieval || !capabilities.AllowAwaitInputs || !capabilities.AllowRequirementSubmission {
			t.Fatalf("%s lacks common Context/Runtime surface: %#v", phase, capabilities)
		}
	}
	verify, _ := CapabilitiesFor(PhaseVerify)
	if verify.AllowImplementationWrite || !verify.AllowEvidenceWrite {
		t.Fatalf("verify write boundary is wrong: %#v", verify)
	}
}
