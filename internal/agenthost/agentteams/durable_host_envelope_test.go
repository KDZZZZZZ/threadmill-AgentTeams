package agentteams

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type durableBindingResolver struct {
	binding TrustedMCPBinding
	err     error
}

func (r durableBindingResolver) ResolveTrustedHostBinding(context.Context, phaseagent.ExecutionContext) (TrustedMCPBinding, error) {
	return r.binding, r.err
}

type durableMountResolver struct {
	mount WorkspaceMount
	err   error
}

func (r durableMountResolver) ResolveWorkspaceMount(context.Context, runtime.DurableWorkspace, phaseagent.ExecutionContext) (WorkspaceMount, error) {
	return r.mount, r.err
}

func TestDurableHostEnvelopeResolverUsesRepositoryAuthority(t *testing.T) {
	execution := testExecution(t, phaseagent.PhaseExecute)
	repo, err := runtime.OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	key := runtime.WaitingKey{TaskID: "task-1", InvocationID: "invocation-1", Generation: 1}
	store := repo.Reconstruction()
	if _, err = store.PutWorkspace(context.Background(), runtime.DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"src"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutContextSlice(context.Background(), runtime.DurableContextSlice{Key: key, Ref: "context", BaselineRef: "baseline", Content: "approved context"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutTaskMemory(context.Background(), runtime.DurableTaskMemory{Key: key, Ref: "memory", View: phaseagent.TaskMemoryBufferView{}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.PutExecutionDescriptor(context.Background(), runtime.DurableExecutionDescriptor{Key: key, TaskContract: "contract", PhaseInstruction: "instruction", TaskSpec: "spec", WorkspaceRef: "workspace", ContextSliceRef: "context", TaskMemoryRef: "memory"}); err != nil {
		t.Fatal(err)
	}
	binding := testEnvelope(execution).MCPBinding
	resolver := DurableHostEnvelopeResolver{Reconstruction: store, Bindings: durableBindingResolver{binding: binding}, Mounts: durableMountResolver{mount: WorkspaceMount{Root: "C:/derived/only", AllowedDirs: []string{"src"}}}}
	envelope, err := resolver.ResolveHostEnvelope(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.TaskContract != "contract" || envelope.PhaseInstruction != "instruction" || envelope.Context.Content != "approved context" || envelope.Workspace.Root != "C:/derived/only" {
		t.Fatalf("envelope=%+v", envelope)
	}
	if _, err = resolver.ResolveHostEnvelope(context.Background(), testExecution(t, phaseagent.PhasePlan)); err == nil {
		t.Fatal("mismatched durable binding accepted")
	}
}

func TestDurableHostEnvelopeResolverRejectsBindingAndMountExpansion(t *testing.T) {
	execution := testExecution(t, phaseagent.PhaseExecute)
	repo, err := runtime.OpenSQLiteRuntimeStateRepository(filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	key := runtime.WaitingKey{TaskID: "task-1", InvocationID: "invocation-1", Generation: 1}
	store := repo.Reconstruction()
	for _, put := range []func() error{
		func() error {
			_, err := store.PutWorkspace(context.Background(), runtime.DurableWorkspace{Key: key, Ref: "workspace", AllowedDirs: []string{"src"}})
			return err
		},
		func() error {
			_, err := store.PutContextSlice(context.Background(), runtime.DurableContextSlice{Key: key, Ref: "context", Content: "context"})
			return err
		},
		func() error {
			_, err := store.PutTaskMemory(context.Background(), runtime.DurableTaskMemory{Key: key, Ref: "memory"})
			return err
		},
		func() error {
			_, err := store.PutExecutionDescriptor(context.Background(), runtime.DurableExecutionDescriptor{Key: key, TaskContract: "contract", PhaseInstruction: "instruction", WorkspaceRef: "workspace", ContextSliceRef: "context", TaskMemoryRef: "memory"})
			return err
		},
	} {
		if err := put(); err != nil {
			t.Fatal(err)
		}
	}
	binding := testEnvelope(execution).MCPBinding
	binding.Binding.BindingRef = "wrong"
	resolver := DurableHostEnvelopeResolver{Reconstruction: store, Bindings: durableBindingResolver{binding: binding}, Mounts: durableMountResolver{mount: WorkspaceMount{Root: "derived", AllowedDirs: []string{"src", "other"}}}}
	if _, err := resolver.ResolveHostEnvelope(context.Background(), execution); err == nil {
		t.Fatal("bad binding accepted")
	}
	binding.Binding.BindingRef = execution.Invocation.Start.BindingRef
	resolver.Bindings = durableBindingResolver{binding: binding}
	if _, err := resolver.ResolveHostEnvelope(context.Background(), execution); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("mount expansion err=%v", err)
	}
}
