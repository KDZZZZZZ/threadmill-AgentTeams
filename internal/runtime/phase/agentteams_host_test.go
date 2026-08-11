package phase

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	baseruntime "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

func TestAgentTeamsPhaseHostDispatchPersistsPreparedBeforeDispatch(t *testing.T) {
	host, service, writer, _ := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-dispatch")

	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	if got, want := service.calls[0], "dispatch:"+agentTeamsInvocationRef(req.Invocation.ID); got != want {
		t.Fatalf("first adapter call = %q, want %q", got, want)
	}
	prepared, err := writer.LoadPreparedInvocation(context.Background(), agentTeamsInvocationRef(req.Invocation.ID))
	if err != nil {
		t.Fatalf("prepared invocation was not saved: %v", err)
	}
	if prepared.InvocationID != req.Invocation.ID || prepared.ProjectID != req.Invocation.ProjectID || prepared.Role != auth.RoleExecutor {
		t.Fatalf("prepared identity = %#v", prepared)
	}
	if prepared.RuntimeConfigRef == "" || prepared.EnvelopeRef == "" || strings.Contains(prepared.RuntimeConfigRef, "secret") || strings.Contains(prepared.EnvelopeRef, "secret") {
		t.Fatalf("prepared refs must be stable non-secret values: %#v", prepared)
	}
	if got := prepared.RequiredCapabilities; len(got) != 1 || got[0] != "shell" {
		t.Fatalf("required AgentTeams capabilities = %#v, want shell", got)
	}
	var spec agentTeamsPhaseSpecDocument
	if err := json.Unmarshal([]byte(prepared.Spec), &spec); err != nil {
		t.Fatalf("prepared spec is not JSON: %v", err)
	}
	if spec.Prompt != req.Prompt.Text || spec.WorkspaceRevision != req.Binding.WorkspaceRevision {
		t.Fatalf("prepared spec = %#v", spec)
	}
}

func TestAgentTeamsPhaseHostSuspendThenRehydrateCreatesNewAttempt(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-await")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if err := host.Suspend(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Suspend() error = %v", err)
	}
	if err := host.Rehydrate(context.Background(), req); err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}

	current, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if current.Execution.AgentTeamsTaskID != "agentteams-task-2" {
		t.Fatalf("rehydrated task id = %q, want second attempt", current.Execution.AgentTeamsTaskID)
	}
	if got := service.terminationModes["agentteams-task-1"]; got != adapter.TerminateReleaseWait {
		t.Fatalf("first attempt termination = %q, want release_wait", got)
	}
}

func TestAgentTeamsPhaseHostStopThenRevokeUsesRecoverableStopMode(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-stop")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	result, err := host.Stop(context.Background(), StopRequest{
		Invocation: req.Invocation,
		Binding:    req.Binding,
	})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if result.NonResumable || result.CheckpointRef != req.Binding.CheckpointRef || result.WorkspaceRevision != req.Binding.WorkspaceRevision || result.ResumeStateRef == "" {
		t.Fatalf("stop result = %#v", result)
	}
	if err := host.Revoke(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Revoke() after stop error = %v", err)
	}
	saved, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if saved.TerminationMode != adapter.TerminateRecoverableStop {
		t.Fatalf("persisted termination mode = %q, want recoverable_stop", saved.TerminationMode)
	}
	if got := service.terminateCalls["agentteams-task-1"]; got != 2 {
		t.Fatalf("terminate calls after stop+revoke = %d, want idempotent second call", got)
	}
}

func TestAgentTeamsPhaseHostStopWithoutCheckpointIsNonResumable(t *testing.T) {
	host, _, _, _ := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-stop-hard")
	req.Binding.CheckpointRef = ""
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	result, err := host.Stop(context.Background(), StopRequest{Invocation: req.Invocation, Binding: req.Binding})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !result.NonResumable || result.CheckpointRef != "" || result.ResumeStateRef != "" {
		t.Fatalf("non-resumable stop result = %#v", result)
	}
}

func TestAgentTeamsPhaseHostNormalRevokeCancelsExecution(t *testing.T) {
	host, service, _, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-revoke")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if err := host.Revoke(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}
	saved, ok, err := state.LoadAgentTeamsPhaseHostState(context.Background(), req.Invocation.ID)
	if err != nil || !ok {
		t.Fatalf("load phase host state = ok %v err %v", ok, err)
	}
	if saved.TerminationMode != adapter.TerminateCancel {
		t.Fatalf("normal revoke mode = %q, want cancel", saved.TerminationMode)
	}
	if got := service.terminationModes["agentteams-task-1"]; got != adapter.TerminateCancel {
		t.Fatalf("adapter termination mode = %q, want cancel", got)
	}
}

func TestAgentTeamsPhaseHostRestartReusesPersistentState(t *testing.T) {
	host, service, writer, state := newAgentTeamsPhaseHostHarness(t)
	req := validAgentTeamsDispatchRequest("inv-restart")
	if err := host.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}

	restarted, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  writer,
		State:   state,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Suspend(context.Background(), req.Invocation.ID); err != nil {
		t.Fatalf("restarted Suspend() error = %v", err)
	}
	if service.dispatchCalls != 1 {
		t.Fatalf("restart suspend redispatched unexpectedly: dispatch calls = %d", service.dispatchCalls)
	}
}

func newAgentTeamsPhaseHostHarness(t *testing.T) (*AgentTeamsPhaseHost, *fakeAgentTeamsPhaseAdapter, *MemoryPreparedInvocationWriter, *MemoryAgentTeamsPhaseHostStateStore) {
	t.Helper()
	service := newFakeAgentTeamsPhaseAdapter()
	writer := NewMemoryPreparedInvocationWriter()
	state := NewMemoryAgentTeamsPhaseHostStateStore()
	host, err := NewAgentTeamsPhaseHost(AgentTeamsPhaseHostConfig{
		Adapter: service,
		Writer:  writer,
		State:   state,
		RoomID:  "!threadmill:example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return host, service, writer, state
}

func validAgentTeamsDispatchRequest(invocationID string) DispatchRequest {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	invocation := baseruntime.Invocation{
		ID:               kernel.InvocationID(invocationID),
		ActorPrincipalID: "actor-a",
		ProjectID:        "project-a",
		TaskID:           "task-a",
		EndpointID:       "execute",
		Generation:       1,
		Role:             auth.RoleExecutor,
		Status:           baseruntime.InvocationPrepared,
		BindingRef:       "binding-a",
		LeaseID:          "lease-a",
		WorkspaceRef:     "workspace-a",
		PromptHashes:     map[string]string{"shared": "prompt-hash"},
		SkillHashes:      map[string]string{"phase-runtime": "skill-hash"},
		EffectiveTools:   []auth.Tool{auth.ToolWorkspaceRun, auth.ToolAgentSubmitPhaseOutput},
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
	binding := BindingSnapshot{
		ProjectID:           invocation.ProjectID,
		ActorPrincipalID:    invocation.ActorPrincipalID,
		TaskID:              invocation.TaskID,
		EndpointID:          invocation.EndpointID,
		Generation:          int(invocation.Generation),
		BindingRef:          invocation.BindingRef,
		LeaseRef:            invocation.LeaseID,
		WorkspaceRef:        invocation.WorkspaceRef,
		WorkspaceRevision:   "workspace-rev-a",
		ContextSliceRef:     "context-slice-a",
		TaskMemoryBufferRef: "memory-buffer-a",
		Inputs: PhaseInputSet{
			InputRevision: "inputs-a",
		},
		CheckpointRef: "checkpoint-a",
	}
	return DispatchRequest{
		Invocation: invocation,
		Capability: invocation.Capability(),
		Prompt: promptcatalog.Rendered{
			Text:         "execute the bounded phase",
			PromptHashes: invocation.PromptHashes,
			SkillHashes:  invocation.SkillHashes,
			SHA256:       "rendered-sha",
		},
		Start: StartPhaseInput{
			InvocationID: invocation.ID,
			Endpoint: PhaseEndpointRef{
				TaskID:     invocation.TaskID,
				EndpointID: invocation.EndpointID,
			},
			Generation: int(invocation.Generation),
			BindingRef: invocation.BindingRef,
			Inputs:     binding.Inputs,
		},
		Binding:       binding,
		CheckpointRef: binding.CheckpointRef,
	}
}

type fakeAgentTeamsPhaseAdapter struct {
	dispatchCalls    int
	nextAttempt      int
	current          map[string]adapter.AgentTeamsExecutionRef
	states           map[string]string
	terminationModes map[string]adapter.TerminateMode
	terminateCalls   map[string]int
	calls            []string
}

func newFakeAgentTeamsPhaseAdapter() *fakeAgentTeamsPhaseAdapter {
	return &fakeAgentTeamsPhaseAdapter{
		nextAttempt:      1,
		current:          make(map[string]adapter.AgentTeamsExecutionRef),
		states:           make(map[string]string),
		terminationModes: make(map[string]adapter.TerminateMode),
		terminateCalls:   make(map[string]int),
	}
}

func (a *fakeAgentTeamsPhaseAdapter) Dispatch(_ context.Context, invocationRef string) (adapter.AgentTeamsExecutionRef, error) {
	a.dispatchCalls++
	a.calls = append(a.calls, "dispatch:"+invocationRef)
	if existing, ok := a.current[invocationRef]; ok {
		if a.states[existing.AgentTeamsTaskID] == "terminated" {
			if a.terminationModes[existing.AgentTeamsTaskID] != adapter.TerminateReleaseWait {
				return adapter.AgentTeamsExecutionRef{}, kernel.Error{Code: kernel.CodeStaleCommand, Message: "terminated execution cannot be redispatched"}
			}
		} else {
			return existing, nil
		}
	}
	execution := adapter.AgentTeamsExecutionRef{
		InvocationID:     kernel.InvocationID(strings.TrimPrefix(invocationRef, agentTeamsInvocationRefPrefix)),
		AgentTeamsTaskID: fmt.Sprintf("agentteams-task-%d", a.nextAttempt),
		HostRef:          "worker-a",
	}
	a.nextAttempt++
	a.current[invocationRef] = execution
	a.states[execution.AgentTeamsTaskID] = "dispatched"
	return execution, nil
}

func (a *fakeAgentTeamsPhaseAdapter) Terminate(_ context.Context, execution adapter.AgentTeamsExecutionRef, rawMode string) error {
	mode := adapter.TerminateMode(rawMode)
	a.calls = append(a.calls, "terminate:"+execution.AgentTeamsTaskID+":"+rawMode)
	a.terminateCalls[execution.AgentTeamsTaskID]++
	if a.states[execution.AgentTeamsTaskID] == "terminated" {
		if a.terminationModes[execution.AgentTeamsTaskID] == mode {
			return nil
		}
		return kernel.IdempotencyConflict()
	}
	a.states[execution.AgentTeamsTaskID] = "terminated"
	a.terminationModes[execution.AgentTeamsTaskID] = mode
	return nil
}

func (a *fakeAgentTeamsPhaseAdapter) Collect(context.Context, adapter.AgentTeamsExecutionRef) (adapter.UntrustedExecutionResult, error) {
	return adapter.UntrustedExecutionResult{}, nil
}

func (a *fakeAgentTeamsPhaseAdapter) Observe(context.Context, string) ([]adapter.ExecutionObservation, error) {
	return nil, nil
}
