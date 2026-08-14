package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type InvocationStatus string

const (
	InvocationPrepared  InvocationStatus = "prepared"
	InvocationRunning   InvocationStatus = "running"
	InvocationWaiting   InvocationStatus = "waiting"
	InvocationStopped   InvocationStatus = "stopped"
	InvocationCompleted InvocationStatus = "completed"
	InvocationFailed    InvocationStatus = "failed"
)

type Invocation struct {
	ID                   kernel.InvocationID
	ActorPrincipalID     kernel.ActorPrincipalID
	ProjectID            kernel.ProjectID
	TaskID               kernel.TaskID
	EndpointID           kernel.EndpointID
	ConsumerInvocationID kernel.InvocationID
	ConsumerTaskID       kernel.TaskID
	ConsumerRole         auth.Role
	Generation           uint64
	Role                 auth.Role
	Operation            string
	Status               InvocationStatus
	BindingRef           kernel.BindingRef
	LeaseID              kernel.LeaseID
	WorkspaceRef         string
	ContextSliceRef      string
	TaskMemoryBufferRef  string
	PromptHashes         map[string]string
	SkillHashes          map[string]string
	EffectiveTools       []auth.Tool
	CreatedAt            time.Time
	ExpiresAt            time.Time
}

func (i Invocation) Validate() error {
	if err := kernel.RequireID("invocation_id", i.ID); err != nil {
		return err
	}
	if err := kernel.RequireID("actor_principal_id", i.ActorPrincipalID); err != nil {
		return err
	}
	if err := kernel.RequireID("project_id", i.ProjectID); err != nil {
		return err
	}
	if i.Role == "" || i.Role == auth.RoleOperator {
		return kernel.InvalidArgument("invocation requires an agent role")
	}
	switch i.Status {
	case InvocationPrepared, InvocationRunning, InvocationWaiting, InvocationStopped, InvocationCompleted, InvocationFailed:
	default:
		return kernel.InvalidArgument("invocation status is invalid")
	}
	if !i.ExpiresAt.After(i.CreatedAt) {
		return kernel.InvalidArgument("invocation expiry must be after creation")
	}
	if i.Role.IsPhase() {
		if err := kernel.RequireID("task_id", i.TaskID); err != nil {
			return err
		}
		if err := kernel.RequireID("endpoint_id", i.EndpointID); err != nil {
			return err
		}
		if i.Generation == 0 {
			return kernel.InvalidArgument("phase generation must be positive")
		}
		if err := kernel.RequireID("binding_ref", i.BindingRef); err != nil {
			return err
		}
		if err := kernel.RequireID("lease_id", i.LeaseID); err != nil {
			return err
		}
	}
	if i.Role == auth.RoleContext {
		switch i.Operation {
		case "retrieve", "curate", "review":
		default:
			return kernel.InvalidArgument("context agent operation must be retrieve, curate, or review")
		}
	} else if i.Operation != "" {
		return kernel.InvalidArgument("operation is only valid for context agent")
	}
	if (i.ConsumerInvocationID != "" || i.ConsumerTaskID != "" || i.ConsumerRole != "") && (i.Role != auth.RoleContext || i.Operation != "retrieve") {
		return kernel.Forbidden("consumer invocation is only valid for context retrieve invocation")
	}
	if i.ConsumerRole != "" {
		switch i.ConsumerRole {
		case auth.RoleTaskManager, auth.RoleContext, auth.RolePlanner, auth.RoleExecutor, auth.RoleVerifier:
		default:
			return kernel.Forbidden("consumer role is not a supported agent role")
		}
	}
	if len(i.EffectiveTools) == 0 {
		return kernel.Forbidden("invocation has no effective tools")
	}
	if len(i.PromptHashes) == 0 || len(i.SkillHashes) == 0 {
		return kernel.InvalidArgument("invocation prompt and skill hashes are required")
	}
	return nil
}

func (i Invocation) Capability() auth.Capability {
	return auth.Capability{
		ProjectID:            i.ProjectID,
		TaskID:               i.TaskID,
		InvocationID:         i.ID,
		ConsumerInvocationID: i.ConsumerInvocationID,
		ConsumerTaskID:       i.ConsumerTaskID,
		ConsumerRole:         i.ConsumerRole,
		Role:                 i.Role,
		Operation:            i.Operation,
		Tools:                auth.ToolSet(i.EffectiveTools...),
		ExpiresAt:            i.ExpiresAt,
	}
}

type InvocationStore interface {
	Create(context.Context, Invocation) error
	Get(context.Context, kernel.InvocationID) (Invocation, bool, error)
	GetByLease(context.Context, kernel.LeaseID) (Invocation, bool, error)
	Transition(context.Context, kernel.InvocationID, InvocationStatus, InvocationStatus) error
}

type InvocationLifecycle interface {
	// Complete is keyed by invocation ID and must be idempotent. Implementations
	// finalize successful workspace state and then end invocation-scoped runtime
	// resources exactly once even when completion cleanup is retried.
	Complete(context.Context, Invocation) error
	// End is keyed by invocation ID and must be idempotent. Implementations
	// terminate tokens, model sessions, task handles, subscriptions, and other
	// abort-scoped resources exactly once after stop or failed startup cleanup.
	End(context.Context, Invocation) error
}

type NoopInvocationLifecycle struct{}

func (NoopInvocationLifecycle) Complete(context.Context, Invocation) error {
	return nil
}

func (NoopInvocationLifecycle) End(context.Context, Invocation) error {
	return nil
}

type MemoryInvocationStore struct {
	mu           sync.RWMutex
	invocations  map[kernel.InvocationID]Invocation
	fingerprints map[kernel.InvocationID]string
}

func NewMemoryInvocationStore() *MemoryInvocationStore {
	return &MemoryInvocationStore{
		invocations:  make(map[kernel.InvocationID]Invocation),
		fingerprints: make(map[kernel.InvocationID]string),
	}
}

func (s *MemoryInvocationStore) Create(ctx context.Context, invocation Invocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := invocation.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprint, err := invocationFingerprint(invocation)
	if err != nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "invocation creation payload cannot be canonicalized", Recoverable: false}
	}
	if _, ok := s.invocations[invocation.ID]; ok {
		if s.fingerprints[invocation.ID] == fingerprint {
			return nil
		}
		return kernel.Error{Code: kernel.CodeIdempotencyConflict, Message: "invocation id already has a different creation payload"}
	}
	s.invocations[invocation.ID] = cloneInvocation(invocation)
	s.fingerprints[invocation.ID] = fingerprint
	return nil
}

func (s *MemoryInvocationStore) Get(ctx context.Context, id kernel.InvocationID) (Invocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Invocation{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	invocation, ok := s.invocations[id]
	return cloneInvocation(invocation), ok, nil
}

func (s *MemoryInvocationStore) GetByLease(ctx context.Context, lease kernel.LeaseID) (Invocation, bool, error) {
	if err := ctx.Err(); err != nil {
		return Invocation{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, invocation := range s.invocations {
		if invocation.LeaseID == lease {
			return cloneInvocation(invocation), true, nil
		}
	}
	return Invocation{}, false, nil
}

func (s *MemoryInvocationStore) Transition(ctx context.Context, id kernel.InvocationID, from, to InvocationStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validInvocationTransition(from, to) {
		return kernel.InvalidArgument("invalid invocation status transition")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	invocation, ok := s.invocations[id]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "invocation not found"}
	}
	if invocation.Status != from {
		return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "invocation status changed"}
	}
	invocation.Status = to
	s.invocations[id] = invocation
	return nil
}

func validInvocationTransition(from, to InvocationStatus) bool {
	switch from {
	case InvocationPrepared:
		return to == InvocationRunning || to == InvocationFailed || to == InvocationStopped
	case InvocationRunning:
		return to == InvocationWaiting || to == InvocationStopped || to == InvocationCompleted || to == InvocationFailed
	case InvocationWaiting:
		return to == InvocationRunning || to == InvocationStopped || to == InvocationFailed
	default:
		return false
	}
}

func cloneInvocation(invocation Invocation) Invocation {
	invocation.PromptHashes = cloneStringMap(invocation.PromptHashes)
	invocation.SkillHashes = cloneStringMap(invocation.SkillHashes)
	invocation.EffectiveTools = append([]auth.Tool(nil), invocation.EffectiveTools...)
	return invocation
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func invocationFingerprint(invocation Invocation) (string, error) {
	canonical := cloneInvocation(invocation)
	sort.Slice(canonical.EffectiveTools, func(i, j int) bool {
		return canonical.EffectiveTools[i] < canonical.EffectiveTools[j]
	})
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
