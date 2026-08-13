package uiprojection

import (
	"context"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/scheduler"
)

// Query is the complete read-only UI projection surface. HTTP and SSE
// adapters may depend on it; graph/runtime/context writers must never be added.
type Query interface {
	Snapshot(context.Context, auth.Principal, kernel.ProjectID, kernel.Revision) (CoordinationSnapshot, error)
	InspectEndpoint(context.Context, auth.Principal, kernel.ProjectID, coordination.PhaseEndpointRef, int) (EndpointInspector, error)
}

type CapacityRecord struct {
	Capacity       scheduler.Capacity
	UpdatedAt      time.Time
	DegradedReason string
}

type CapacityReader interface {
	ReadCapacity(context.Context, kernel.ProjectID) (CapacityRecord, error)
}

type GraphReader interface {
	Latest(context.Context, kernel.ProjectID) (coordination.GraphSnapshot, error)
	Snapshot(context.Context, kernel.ProjectID, kernel.Revision) (coordination.GraphSnapshot, error)
}

type InvocationFilter struct {
	ProjectID  kernel.ProjectID
	TaskID     kernel.TaskID
	EndpointID kernel.EndpointID
	Generation uint64
}

// InvocationReader returns authoritative runtime.Invocation records matching
// all non-zero filter fields. The projection defensively rechecks every row.
type InvocationReader interface {
	ListInvocations(context.Context, InvocationFilter) ([]runtime.Invocation, error)
}

type CursorReader interface {
	CurrentCursor(context.Context, kernel.ProjectID) (string, error)
}

// CandidateInspectionRecord carries only the authority metadata needed to
// enforce project/task/creator filtering around the canonical candidate view.
// It is an internal query record, not another TaskMemoryBuffer model.
type CandidateInspectionRecord struct {
	ProjectID             kernel.ProjectID
	TaskID                kernel.TaskID
	CreatedByInvocationID kernel.InvocationID
	View                  contextgraph.TaskMemoryCandidateView
}

// ContextInspection contains already ACL-filtered canonical Context objects.
// The implementation belongs in Context Service; uiprojection additionally
// applies the operator grant and exact Invocation creator filter.
type ContextInspection struct {
	Subscriptions []contextgraph.SubscriptionInspection
	Slice         contextgraph.ContextSlice
	Frontier      []string
	Omitted       []OmittedContext
	Candidates    []CandidateInspectionRecord
}

type ContextInspectionReader interface {
	InspectInvocation(context.Context, auth.Principal, runtime.Invocation) (ContextInspection, error)
}

type TaskReadGrant struct {
	Visible         bool
	ContextBodies   bool
	CandidateBodies bool
}

// PermissionReader is intentionally independent of agent capability tools.
// Browser operators are authorized by their session and project/task ACL.
type PermissionReader interface {
	CanReadProject(context.Context, auth.Principal, kernel.ProjectID) (bool, error)
	TaskGrant(context.Context, auth.Principal, kernel.ProjectID, kernel.TaskID) (TaskReadGrant, error)
}

type Service struct {
	capacity    CapacityReader
	graphs      GraphReader
	invocations InvocationReader
	contexts    ContextInspectionReader
	cursors     CursorReader
	permissions PermissionReader
}

func NewService(
	capacity CapacityReader,
	graphs GraphReader,
	invocations InvocationReader,
	contexts ContextInspectionReader,
	cursors CursorReader,
	permissions PermissionReader,
) *Service {
	return &Service{
		capacity:    capacity,
		graphs:      graphs,
		invocations: invocations,
		contexts:    contexts,
		cursors:     cursors,
		permissions: permissions,
	}
}
