package phase

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestContextBindingResolverCreatesInitialOnceAndReplaysMaterializedContext(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	source := &fakeBaseBindingSource{initial: []string{sgA.ID}}
	runtime := newTestContextRuntime(store)
	resolver := NewContextBindingResolver(source, runtime)
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	invocationID := invocationIDForCommand(cmd)

	first, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
	if err != nil {
		t.Fatal(err)
	}
	if source.resolveCalls != 2 {
		t.Fatalf("base resolve calls = %d", source.resolveCalls)
	}
	if first.ContextSliceRef != second.ContextSliceRef || first.TaskMemoryBufferRef != second.TaskMemoryBufferRef {
		t.Fatalf("refs changed on replay: %q/%q then %q/%q", first.ContextSliceRef, first.TaskMemoryBufferRef, second.ContextSliceRef, second.TaskMemoryBufferRef)
	}
	if got := materializedSubgraphs(t, first.ContextSlice); !reflect.DeepEqual(got, []string{sgA.ID}) {
		t.Fatalf("context subgraphs = %#v", got)
	}
	subs, err := runtime.InspectSubscriptions(ctx, phasePrincipal("project-a", "task-a", "execute", invocationID), invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeSubscriptions(subs)) != 1 {
		t.Fatalf("active subscriptions after replay = %#v", subs)
	}
	if runtime.ensureCalls != 1 {
		t.Fatalf("ensure initial calls = %d, want 1", runtime.ensureCalls)
	}
}

func TestContextBindingResolverMaterializesExplicitSubscriptionUnion(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	sgB := createContextSubgraph(t, store, "project-a", "beta")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	createContextNode(t, store, "project-a", sgB.ID, "beta node")
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	invocationID := invocationIDForCommand(cmd)
	principal := phasePrincipal("project-a", "task-a", "execute", invocationID)
	if _, err := store.Subscribe(ctx, principal, contextgraph.SubscribeRequest{SubgraphIDs: []string{sgA.ID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Subscribe(ctx, principal, contextgraph.SubscribeRequest{SubgraphIDs: []string{sgB.ID}}); err != nil {
		t.Fatal(err)
	}
	source := &fakeBaseBindingSource{initial: []string{sgA.ID}}
	runtime := newTestContextRuntime(store)
	resolver := NewContextBindingResolver(source, runtime)

	binding, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
	if err != nil {
		t.Fatal(err)
	}
	if got := materializedSubgraphs(t, binding.ContextSlice); !reflect.DeepEqual(got, []string{sgA.ID, sgB.ID}) {
		t.Fatalf("context subgraphs = %#v, want explicit subscription union", got)
	}
	subs, err := runtime.InspectSubscriptions(ctx, principal, invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeSubscriptions(subs)) != 2 {
		t.Fatalf("initial subscription should not be created when explicit exists: %#v", subs)
	}
	if runtime.ensureCalls != 0 {
		t.Fatalf("ensure initial calls = %d, want 0", runtime.ensureCalls)
	}
}

func TestContextBindingResolverIsolatesDifferentInvocationsAndTasks(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	sgB := createContextSubgraph(t, store, "project-a", "beta")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	createContextNode(t, store, "project-a", sgB.ID, "beta node")
	resolver := NewContextBindingResolver(&fakeBaseBindingSource{
		initialByTask: map[kernel.TaskID][]string{
			"task-a": {sgA.ID},
			"task-b": {sgB.ID},
		},
	}, newTestContextRuntime(store))
	cmdA := contextBindingCommand("run-a", "task-a", "execute")
	cmdB := contextBindingCommand("run-b", "task-b", "execute")

	a, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmdA, InvocationID: invocationIDForCommand(cmdA)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmdB, InvocationID: invocationIDForCommand(cmdB)})
	if err != nil {
		t.Fatal(err)
	}
	if got := materializedSubgraphs(t, a.ContextSlice); !reflect.DeepEqual(got, []string{sgA.ID}) {
		t.Fatalf("task-a subgraphs = %#v", got)
	}
	if got := materializedSubgraphs(t, b.ContextSlice); !reflect.DeepEqual(got, []string{sgB.ID}) {
		t.Fatalf("task-b subgraphs = %#v", got)
	}
	if a.ContextSliceRef == b.ContextSliceRef || a.TaskMemoryBufferRef == b.TaskMemoryBufferRef {
		t.Fatalf("refs were not scoped per invocation/task: a=%#v b=%#v", a, b)
	}
}

func TestContextBindingResolverRefreshUsesActiveInvocationAndListsTaskMemory(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	submitter := phasePrincipal("project-a", "task-a", "execute", "producer")
	submitter.Tools = auth.ToolSet(auth.ToolAgentSubmitMemoryCandidate, auth.ToolAgentListTaskMemoryCandidates, auth.ToolContextSubscribe)
	if _, err := store.SubmitCandidate(ctx, submitter, contextgraph.SubmitCandidateRequest{Candidate: contextgraph.MemoryCandidate{Statement: "candidate", Kind: "fact", SourceRefs: []string{"artifact://one"}, SubgraphIDs: []string{sgA.ID}}}); err != nil {
		t.Fatal(err)
	}
	source := &fakeBaseBindingSource{initial: []string{sgA.ID}}
	resolver := NewContextBindingResolver(source, newTestContextRuntime(store))
	cmd := contextBindingCommand("run-a", "task-a", "verify")
	binding := baseContextBinding("task-a", "verify")
	active := ActiveInvocation{
		Invocation: invocationForCommand(cmd, binding),
		Command:    cmd,
		Binding:    binding,
	}

	refreshed, err := resolver.Refresh(ctx, active)
	if err != nil {
		t.Fatal(err)
	}
	if source.refreshCalls != 1 {
		t.Fatalf("base refresh calls = %d", source.refreshCalls)
	}
	var memory contextgraph.TaskMemoryBufferView
	if err := json.Unmarshal([]byte(refreshed.TaskMemoryBuffer), &memory); err != nil {
		t.Fatal(err)
	}
	if len(memory.Candidates) != 1 || memory.Candidates[0].Candidate.Statement != "candidate" {
		t.Fatalf("task memory buffer = %#v", memory)
	}
}

func TestContextBindingLifecycleEndsInvocationSubscriptions(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	runtime := newTestContextRuntime(store)
	resolver := NewContextBindingResolver(&fakeBaseBindingSource{initial: []string{sgA.ID}}, runtime)
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	invocationID := invocationIDForCommand(cmd)
	binding, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
	if err != nil {
		t.Fatal(err)
	}

	if err := (ContextBindingLifecycle{Contexts: runtime}).End(ctx, baseruntime.Invocation{
		ID:               invocationID,
		ActorPrincipalID: binding.ActorPrincipalID,
		ProjectID:        binding.ProjectID,
		TaskID:           binding.TaskID,
		EndpointID:       binding.EndpointID,
		Role:             auth.RoleExecutor,
	}); err != nil {
		t.Fatal(err)
	}
	subs, err := runtime.InspectSubscriptions(ctx, phasePrincipal("project-a", "task-a", "execute", invocationID), invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeSubscriptions(subs)) != 0 {
		t.Fatalf("active subscriptions after End = %#v", subs)
	}
}

func TestContextBindingResolverDoesNotSwallowContextErrors(t *testing.T) {
	source := &fakeBaseBindingSource{initial: []string{"sg-a"}}
	resolver := NewContextBindingResolver(source, failingContextRuntime{err: errors.New("boom")})
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	_, err := resolver.ResolveForInvocation(context.Background(), BindingResolveRequest{Command: cmd, InvocationID: invocationIDForCommand(cmd)})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v, want boom", err)
	}
	if source.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", source.abortCalls)
	}
}

func TestContextBindingResolverFailsClosedWhenActorPrincipalMissing(t *testing.T) {
	source := &fakeBaseBindingSource{missingActor: true}
	resolver := NewContextBindingResolver(source, newTestContextRuntime(contextgraph.NewMemoryStore(fixedNow)))
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	_, err := resolver.ResolveForInvocation(context.Background(), BindingResolveRequest{Command: cmd, InvocationID: invocationIDForCommand(cmd)})
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("err = %v, want stale_binding", err)
	}
}

func TestContextBindingResolverValidatesBeforeContextSideEffects(t *testing.T) {
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	tests := []struct {
		name    string
		binding BindingSnapshot
		active  bool
	}{
		{name: "wrong task", binding: baseContextBinding("other-task", "execute")},
		{name: "wrong binding", binding: func() BindingSnapshot {
			b := baseContextBinding("task-a", "execute")
			b.BindingRef = "other-binding"
			return b
		}()},
		{name: "wrong generation", binding: func() BindingSnapshot {
			b := baseContextBinding("task-a", "execute")
			b.Generation = 2
			return b
		}()},
		{name: "refresh active mismatch", active: true, binding: baseContextBinding("task-a", "execute")},
		{name: "refresh actor drift", active: true, binding: func() BindingSnapshot {
			b := baseContextBinding("task-a", "execute")
			b.ActorPrincipalID = "drifted-agent"
			return b
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := newTestContextRuntime(contextgraph.NewMemoryStore(fixedNow))
			source := &fakeBaseBindingSource{binding: tt.binding, initial: []string{"sg-a"}}
			resolver := NewContextBindingResolver(source, runtime)
			var err error
			if tt.active {
				activeBinding := baseContextBinding("task-a", "execute")
				activeInvocation := invocationForCommand(cmd, activeBinding)
				if tt.name == "refresh active mismatch" {
					activeInvocation.Generation = 2
				}
				_, err = resolver.Refresh(context.Background(), ActiveInvocation{Invocation: activeInvocation, Command: cmd, Binding: activeBinding})
			} else {
				_, err = resolver.ResolveForInvocation(context.Background(), BindingResolveRequest{Command: cmd, InvocationID: invocationIDForCommand(cmd)})
			}
			if !kernel.IsCode(err, kernel.CodeStaleBinding) {
				t.Fatalf("err = %v, want stale_binding", err)
			}
			if runtime.ensureCalls != 0 || runtime.inspectCalls != 0 || runtime.materializeCalls != 0 || runtime.memoryCalls != 0 {
				t.Fatalf("context side effects happened: ensure=%d inspect=%d materialize=%d memory=%d", runtime.ensureCalls, runtime.inspectCalls, runtime.materializeCalls, runtime.memoryCalls)
			}
			if source.abortCalls != 1 {
				t.Fatalf("abort calls = %d, want 1", source.abortCalls)
			}
		})
	}
}

func TestContextBindingResolverStopDoesNotCreateInvocationSubscription(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	source := &fakeBaseBindingSource{initial: []string{sgA.ID}}
	runtime := newTestContextRuntime(store)
	resolver := NewContextBindingResolver(source, runtime)
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	stopCommand := cmd
	stopCommand.ID = "stop-a"
	stopCommand.Action = coordination.CommandStop
	stopInvocationID := invocationIDForCommand(stopCommand)

	binding, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: stopCommand, InvocationID: stopInvocationID})
	if err != nil {
		t.Fatal(err)
	}
	if binding.ContextSliceRef != "" || binding.TaskMemoryBufferRef != "" {
		t.Fatalf("stop binding should not materialize context refs: %#v", binding)
	}
	if runtime.ensureCalls != 0 || runtime.inspectCalls != 0 || runtime.materializeCalls != 0 || runtime.memoryCalls != 0 {
		t.Fatalf("stop created context side effects: ensure=%d inspect=%d materialize=%d memory=%d", runtime.ensureCalls, runtime.inspectCalls, runtime.materializeCalls, runtime.memoryCalls)
	}
	subs, err := runtime.InspectSubscriptions(ctx, phasePrincipal("project-a", "task-a", "execute", stopInvocationID), stopInvocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeSubscriptions(subs)) != 0 {
		t.Fatalf("stop created active subscriptions: %#v", subs)
	}
	if source.abortCalls != 0 {
		t.Fatalf("abort calls = %d, want 0", source.abortCalls)
	}
}

func TestContextBindingResolverAbortsResolvedBindingAfterContextFailure(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	source := &fakeBaseBindingSource{initial: []string{sgA.ID}}
	runtime := newTestContextRuntime(store)
	runtime.materializeErr = errors.New("materialize failed")
	resolver := NewContextBindingResolver(source, runtime)
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	invocationID := invocationIDForCommand(cmd)

	_, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
	if err == nil || err.Error() != "materialize failed" {
		t.Fatalf("err = %v, want materialize failed", err)
	}
	if source.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", source.abortCalls)
	}
	if runtime.endCalls != 1 {
		t.Fatalf("end invocation calls = %d, want 1", runtime.endCalls)
	}
	subs, err := store.InspectSubscriptions(ctx, phasePrincipal("project-a", "task-a", "execute", invocationID), invocationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeSubscriptions(subs)) != 0 {
		t.Fatalf("active subscriptions after abort = %#v", subs)
	}
}

func TestContextBindingResolverRejectsMismatchedInvocationIDBeforeBaseResolve(t *testing.T) {
	source := &fakeBaseBindingSource{}
	resolver := NewContextBindingResolver(source, newTestContextRuntime(contextgraph.NewMemoryStore(fixedNow)))
	_, err := resolver.ResolveForInvocation(context.Background(), BindingResolveRequest{Command: contextBindingCommand("run-a", "task-a", "execute"), InvocationID: "wrong-invocation"})
	if !kernel.IsCode(err, kernel.CodeStaleBinding) {
		t.Fatalf("err = %v, want stale_binding", err)
	}
	if source.resolveCalls != 0 {
		t.Fatalf("base resolve calls = %d, want 0", source.resolveCalls)
	}
}

func TestContextBindingResolverSingleflightsConcurrentInitialSlice(t *testing.T) {
	ctx := context.Background()
	store := contextgraph.NewMemoryStore(fixedNow)
	sgA := createContextSubgraph(t, store, "project-a", "alpha")
	createContextNode(t, store, "project-a", sgA.ID, "alpha node")
	runtime := newTestContextRuntime(store)
	runtime.blockEnsure = make(chan struct{})
	resolver := NewContextBindingResolver(&fakeBaseBindingSource{initial: []string{sgA.ID}}, runtime)
	cmd := contextBindingCommand("run-a", "task-a", "execute")
	invocationID := invocationIDForCommand(cmd)
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := resolver.ResolveForInvocation(ctx, BindingResolveRequest{Command: cmd, InvocationID: invocationID})
			errs <- err
		}()
	}
	close(start)
	runtime.waitEnsureStarted(t)
	runtime.waitInspectCalls(t, 2)
	close(runtime.blockEnsure)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if runtime.ensureCalls != 1 {
		t.Fatalf("ensure initial calls = %d, want 1", runtime.ensureCalls)
	}
}

type fakeBaseBindingSource struct {
	initial       []string
	initialByTask map[kernel.TaskID][]string
	binding       BindingSnapshot
	missingActor  bool
	resolveCalls  int
	refreshCalls  int
	abortCalls    int
}

type testContextRuntime struct {
	store *contextgraph.MemoryStore

	mu                sync.Mutex
	ensureCalls       int
	inspectCalls      int
	materializeCalls  int
	memoryCalls       int
	endCalls          int
	blockEnsure       chan struct{}
	ensureStarted     chan struct{}
	ensureStartedOnce sync.Once
	inspectErr        error
	ensureErr         error
	materializeErr    error
	memoryErr         error
	endErr            error
}

func newTestContextRuntime(store *contextgraph.MemoryStore) *testContextRuntime {
	return &testContextRuntime{store: store, ensureStarted: make(chan struct{})}
}

func (r *testContextRuntime) EnsureInitialSlice(ctx context.Context, principal auth.Principal, subgraphIDs []string) (contextgraph.ContextSlice, error) {
	r.mu.Lock()
	r.ensureCalls++
	block := r.blockEnsure
	r.ensureStartedOnce.Do(func() { close(r.ensureStarted) })
	r.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return contextgraph.ContextSlice{}, ctx.Err()
		}
	}
	if r.ensureErr != nil {
		return contextgraph.ContextSlice{}, r.ensureErr
	}
	subscriptions, err := r.store.InspectSubscriptions(ctx, principal, principal.InvocationID)
	if err != nil {
		return contextgraph.ContextSlice{}, err
	}
	if hasActiveSubscription(subscriptions) {
		return r.store.MaterializeRuntimeContext(ctx, principal)
	}
	return r.store.CreateInitialSlice(ctx, principal, subgraphIDs)
}

func (r *testContextRuntime) InspectSubscriptions(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) ([]contextgraph.SubscriptionInspection, error) {
	r.mu.Lock()
	r.inspectCalls++
	err := r.inspectErr
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return r.store.InspectSubscriptions(ctx, principal, invocationID)
}

func (r *testContextRuntime) MaterializeRuntimeContext(ctx context.Context, principal auth.Principal) (contextgraph.ContextSlice, error) {
	r.mu.Lock()
	r.materializeCalls++
	err := r.materializeErr
	r.mu.Unlock()
	if err != nil {
		return contextgraph.ContextSlice{}, err
	}
	return r.store.MaterializeRuntimeContext(ctx, principal)
}

func (r *testContextRuntime) ListTaskCandidates(ctx context.Context, principal auth.Principal) (contextgraph.TaskMemoryBufferView, error) {
	r.mu.Lock()
	r.memoryCalls++
	err := r.memoryErr
	r.mu.Unlock()
	if err != nil {
		return contextgraph.TaskMemoryBufferView{}, err
	}
	return r.store.ListTaskCandidates(ctx, principal)
}

func (r *testContextRuntime) EndInvocation(ctx context.Context, principal auth.Principal, invocationID kernel.InvocationID) error {
	r.mu.Lock()
	r.endCalls++
	err := r.endErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.store.EndInvocation(ctx, principal, invocationID)
}

func (r *testContextRuntime) waitEnsureStarted(t *testing.T) {
	t.Helper()
	select {
	case <-r.ensureStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for EnsureInitialSlice")
	}
}

func (r *testContextRuntime) waitInspectCalls(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		r.mu.Lock()
		got := r.inspectCalls
		r.mu.Unlock()
		if got >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for inspect calls: got %d want %d", got, want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func (s *fakeBaseBindingSource) ResolvePhaseBinding(_ context.Context, req BindingResolveRequest) (BindingSnapshot, []string, error) {
	s.resolveCalls++
	initial := append([]string(nil), s.initial...)
	if s.initialByTask != nil {
		initial = append([]string(nil), s.initialByTask[req.Command.Endpoint.TaskID]...)
	}
	binding := s.binding
	if binding.TaskID == "" {
		binding = baseContextBinding(req.Command.Endpoint.TaskID, req.Command.Endpoint.EndpointID)
	}
	if s.missingActor {
		binding.ActorPrincipalID = ""
	}
	return binding, initial, nil
}

func (s *fakeBaseBindingSource) RefreshPhaseBinding(_ context.Context, active ActiveInvocation) (BindingSnapshot, []string, error) {
	s.refreshCalls++
	initial := append([]string(nil), s.initial...)
	if s.initialByTask != nil {
		initial = append([]string(nil), s.initialByTask[active.Command.Endpoint.TaskID]...)
	}
	binding := s.binding
	if binding.TaskID == "" {
		binding = baseContextBinding(active.Command.Endpoint.TaskID, active.Command.Endpoint.EndpointID)
	}
	if s.missingActor {
		binding.ActorPrincipalID = ""
	}
	return binding, initial, nil
}

func (s *fakeBaseBindingSource) AbortResolvedPhaseBinding(context.Context, BindingResolveRequest, BindingSnapshot) error {
	s.abortCalls++
	return nil
}

type failingContextRuntime struct {
	err error
}

func (r failingContextRuntime) CreateInitialSlice(context.Context, auth.Principal, []string) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, r.err
}
func (r failingContextRuntime) EnsureInitialSlice(context.Context, auth.Principal, []string) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, r.err
}
func (r failingContextRuntime) InspectSubscriptions(context.Context, auth.Principal, kernel.InvocationID) ([]contextgraph.SubscriptionInspection, error) {
	return nil, r.err
}
func (r failingContextRuntime) MaterializeRuntimeContext(context.Context, auth.Principal) (contextgraph.ContextSlice, error) {
	return contextgraph.ContextSlice{}, r.err
}
func (r failingContextRuntime) ListTaskCandidates(context.Context, auth.Principal) (contextgraph.TaskMemoryBufferView, error) {
	return contextgraph.TaskMemoryBufferView{}, r.err
}
func (r failingContextRuntime) EndInvocation(context.Context, auth.Principal, kernel.InvocationID) error {
	return r.err
}

func contextBindingCommand(id string, taskID kernel.TaskID, endpoint kernel.EndpointID) PhaseCommand {
	return PhaseCommand{
		ID:         id,
		Action:     coordination.CommandStart,
		Endpoint:   PhaseEndpointRef{TaskID: taskID, EndpointID: endpoint},
		Generation: 1,
		BindingRef: kernel.BindingRef("binding-" + taskID),
		LeaseRef:   kernel.LeaseID("lease-" + taskID),
	}
}

func baseContextBinding(taskID kernel.TaskID, endpoint kernel.EndpointID) BindingSnapshot {
	return BindingSnapshot{
		ProjectID:         "project-a",
		ActorPrincipalID:  kernel.ActorPrincipalID("agent-" + taskID),
		TaskID:            taskID,
		EndpointID:        endpoint,
		Generation:        1,
		BindingRef:        kernel.BindingRef("binding-" + taskID),
		LeaseRef:          kernel.LeaseID("lease-" + taskID),
		WorkspaceRef:      "workspace-" + string(taskID),
		WorkspaceRevision: "workspace-revision-" + string(taskID),
		TaskContract:      `{"task":"` + string(taskID) + `"}`,
		PhaseSpec:         `{"endpoint":"` + string(endpoint) + `"}`,
	}
}

func invocationIDForCommand(command PhaseCommand) kernel.InvocationID {
	return deterministicInvocationID(command)
}

func invocationForCommand(command PhaseCommand, binding BindingSnapshot) baseruntime.Invocation {
	return baseruntime.Invocation{
		ID:                  invocationIDForCommand(command),
		ActorPrincipalID:    binding.ActorPrincipalID,
		ProjectID:           binding.ProjectID,
		TaskID:              binding.TaskID,
		EndpointID:          binding.EndpointID,
		Generation:          uint64(binding.Generation),
		Role:                roleForEndpoint(command.Endpoint.EndpointID),
		Status:              baseruntime.InvocationRunning,
		BindingRef:          binding.BindingRef,
		LeaseID:             binding.LeaseRef,
		WorkspaceRef:        binding.WorkspaceRef,
		ContextSliceRef:     binding.ContextSliceRef,
		TaskMemoryBufferRef: binding.TaskMemoryBufferRef,
	}
}

func roleForEndpoint(endpoint kernel.EndpointID) auth.Role {
	role, err := phaseRole(endpoint)
	if err != nil {
		panic(err)
	}
	return role
}

func createContextSubgraph(t *testing.T, store *contextgraph.MemoryStore, projectID kernel.ProjectID, name string) contextgraph.ContextSubgraph {
	t.Helper()
	sg, err := store.CreateSubgraph(context.Background(), contextPrincipal(projectID, auth.ToolContextCreateSubgraph), contextgraph.CreateGeneralSubgraphRequest{Name: name, Summary: name})
	if err != nil {
		t.Fatal(err)
	}
	return sg
}

func createContextNode(t *testing.T, store *contextgraph.MemoryStore, projectID kernel.ProjectID, subgraphID string, statement string) {
	t.Helper()
	if _, err := store.CreateNode(context.Background(), contextPrincipal(projectID, auth.ToolContextCreateNode), contextgraph.CreateGeneralNodeRequest{Statement: statement, Kind: "fact", SourceRefs: []string{"source://" + statement}, SubgraphIDs: []string{subgraphID}}); err != nil {
		t.Fatal(err)
	}
}

func contextPrincipal(projectID kernel.ProjectID, tools ...auth.Tool) auth.Principal {
	return auth.Principal{
		ActorPrincipalID: "ctx",
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		Role:             auth.RoleContext,
		Operation:        "curate",
		Tools:            auth.ToolSet(tools...),
	}
}

func phasePrincipal(projectID kernel.ProjectID, taskID kernel.TaskID, endpoint kernel.EndpointID, invocationID kernel.InvocationID) auth.Principal {
	role, err := phaseRole(endpoint)
	if err != nil {
		panic(err)
	}
	return auth.Principal{
		ActorPrincipalID: "agent-" + kernel.ActorPrincipalID(taskID),
		Kind:             auth.PrincipalAgent,
		ProjectID:        projectID,
		Role:             role,
		TaskID:           taskID,
		InvocationID:     invocationID,
		Tools: auth.ToolSet(
			auth.ToolContextSubscribe,
			auth.ToolAgentListTaskMemoryCandidates,
		),
	}
}

func materializedSubgraphs(t *testing.T, raw string) []string {
	t.Helper()
	var slice contextgraph.ContextSlice
	if err := json.Unmarshal([]byte(raw), &slice); err != nil {
		t.Fatal(err)
	}
	seen := map[string]struct{}{}
	for _, node := range slice.Nodes {
		for _, id := range node.SubgraphIDs {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func activeSubscriptions(subs []contextgraph.SubscriptionInspection) []contextgraph.SubscriptionInspection {
	var out []contextgraph.SubscriptionInspection
	for _, sub := range subs {
		if sub.Active {
			out = append(out, sub)
		}
	}
	return out
}

func fixedNow() time.Time {
	return time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
}
