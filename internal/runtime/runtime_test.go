package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

func TestAssembleInvocationRecordsPromptSkillAndEffectiveToolHashes(t *testing.T) {
	available := make(map[auth.Tool]struct{})
	for _, tool := range auth.CanonicalTools() {
		available[tool] = struct{}{}
	}
	catalog, err := promptcatalog.Load(filepath.Join("..", ".."), available)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	assembler, err := NewAssembler(catalog)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	assembly, err := assembler.Assemble(Invocation{
		ID:               "inv-a",
		ActorPrincipalID: "agent-a",
		ProjectID:        "project-a",
		TaskID:           "task-a",
		EndpointID:       "execute",
		Generation:       1,
		Role:             auth.RoleExecutor,
		Status:           InvocationPrepared,
		BindingRef:       "binding-a",
		LeaseID:          "lease-a",
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}, promptcatalog.RenderData{RuntimeEnvelope: `{"invocation_id":"inv-a"}`})
	if err != nil {
		t.Fatalf("assemble invocation: %v", err)
	}
	if len(assembly.Invocation.PromptHashes) != 2 || len(assembly.Invocation.SkillHashes) == 0 || len(assembly.Invocation.EffectiveTools) == 0 {
		t.Fatalf("assembly did not record authority hashes/tools: %#v", assembly.Invocation)
	}
	if assembly.Prompt.SHA256 == "" {
		t.Fatal("rendered prompt hash is empty")
	}
	capability := assembly.Invocation.Capability()
	if capability.TaskID != "task-a" || capability.InvocationID != "inv-a" || !contains(capability.Tools, auth.ToolWorkspaceWrite) {
		t.Fatalf("capability did not preserve assembled authority: %#v", capability)
	}
}

func TestMemoryInvocationStoreIsIdempotentAndTransitionsClosedStateMachine(t *testing.T) {
	store := NewMemoryInvocationStore()
	invocation := validInvocation()
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("idempotent create failed: %v", err)
	}
	mutations := map[string]func(*Invocation){
		"actor":       func(value *Invocation) { value.ActorPrincipalID = "other-agent" },
		"binding":     func(value *Invocation) { value.BindingRef = "other-binding" },
		"lease":       func(value *Invocation) { value.LeaseID = "other-lease" },
		"tools":       func(value *Invocation) { value.EffectiveTools = []auth.Tool{auth.ToolContextListSubgraphs} },
		"prompt hash": func(value *Invocation) { value.PromptHashes = map[string]string{"shared": "other"} },
		"expiry":      func(value *Invocation) { value.ExpiresAt = value.ExpiresAt.Add(time.Minute) },
	}
	for name, mutate := range mutations {
		changed := invocation
		mutate(&changed)
		if err := store.Create(context.Background(), changed); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
			t.Fatalf("different invocation %s = %v, want idempotency_conflict", name, err)
		}
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationPrepared, InvocationRunning); err != nil {
		t.Fatal(err)
	}
	byLease, ok, err := store.GetByLease(context.Background(), invocation.LeaseID)
	if err != nil || !ok {
		t.Fatalf("get by lease = %#v %v, %v; want invocation", byLease, ok, err)
	}
	if byLease.ID != invocation.ID || byLease.Status != InvocationRunning {
		t.Fatalf("get by lease returned %#v, want running invocation", byLease)
	}
	if err := store.Create(context.Background(), invocation); err != nil {
		t.Fatalf("create replay after status transition failed: %v", err)
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationRunning, InvocationStopped); err != nil {
		t.Fatal(err)
	}
	stopped, ok, err := store.GetByLease(context.Background(), invocation.LeaseID)
	if err != nil || !ok || stopped.Status != InvocationStopped {
		t.Fatalf("get stopped by lease = %#v %v, %v; want stopped invocation", stopped, ok, err)
	}
	if err := store.Transition(context.Background(), invocation.ID, InvocationPrepared, InvocationCompleted); !kernel.IsCode(err, kernel.CodeInvalidRequest) {
		t.Fatalf("illegal transition = %v, want invalid_request", err)
	}
}

func TestConsumerInvocationOnlyValidForContextRetrieveInvocation(t *testing.T) {
	phase := validInvocation()
	phase.ConsumerInvocationID = "inv-victim"
	if err := phase.Validate(); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("phase consumer invocation err = %v, want forbidden", err)
	}

	review := validContextInvocation("review")
	review.ConsumerInvocationID = "inv-victim"
	if err := review.Validate(); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("review consumer invocation err = %v, want forbidden", err)
	}

	retrieve := validContextInvocation("retrieve")
	retrieve.ConsumerInvocationID = "inv-victim"
	retrieve.ConsumerTaskID = "task-a"
	retrieve.ConsumerRole = auth.RoleExecutor
	if err := retrieve.Validate(); err != nil {
		t.Fatalf("retrieve consumer invocation err = %v", err)
	}
	capability := retrieve.Capability()
	if capability.ConsumerInvocationID != "inv-victim" || capability.ConsumerTaskID != "task-a" || capability.ConsumerRole != auth.RoleExecutor {
		t.Fatalf("capability lost consumer scope: %#v", capability)
	}

	badRole := validContextInvocation("retrieve")
	badRole.ConsumerRole = auth.Role("operator")
	if err := badRole.Validate(); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("bad consumer role err = %v, want forbidden", err)
	}
}

func TestEffectiveToolsRequiresThreeWayIntersection(t *testing.T) {
	tools, err := EffectiveTools(ToolSource{
		RoleTools:      auth.ToolSet(auth.ToolContextExplore, auth.ToolContextSearch),
		SkillTools:     auth.ToolSet(auth.ToolContextExplore, auth.ToolContextSubscribe),
		AvailableTools: auth.ToolSet(auth.ToolContextExplore, auth.ToolContextUnsubscribe),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 || tools[0] != auth.ToolContextExplore {
		t.Fatalf("intersection = %v, want only context.explore", tools)
	}
}

func validInvocation() Invocation {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	return Invocation{
		ID:               "inv-a",
		ActorPrincipalID: "agent-a",
		ProjectID:        "project-a",
		TaskID:           "task-a",
		EndpointID:       "execute",
		Generation:       1,
		Role:             auth.RoleExecutor,
		Status:           InvocationPrepared,
		BindingRef:       "binding-a",
		LeaseID:          "lease-a",
		PromptHashes:     map[string]string{"shared": "prompt-hash"},
		SkillHashes:      map[string]string{"phase-runtime": "skill-hash"},
		EffectiveTools:   []auth.Tool{auth.ToolContextExplore},
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
}

func validContextInvocation(operation string) Invocation {
	now := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	return Invocation{
		ID:               kernel.InvocationID("inv-context-" + operation),
		ActorPrincipalID: "ctx-agent",
		ProjectID:        "project-a",
		Role:             auth.RoleContext,
		Operation:        operation,
		Status:           InvocationPrepared,
		PromptHashes:     map[string]string{"shared": "prompt-hash"},
		SkillHashes:      map[string]string{"context": "skill-hash"},
		EffectiveTools:   []auth.Tool{auth.ToolContextSearch},
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
}

func contains(tools map[auth.Tool]struct{}, target auth.Tool) bool {
	_, ok := tools[target]
	return ok
}
