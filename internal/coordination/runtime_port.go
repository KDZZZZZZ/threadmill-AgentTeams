package coordination

import (
	"context"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// RuntimeRunner is the process-internal wake-up seam used by the application
// host. It deliberately exposes only reconciliation: graph reads and writes
// remain on TaskManagerGraph, and no HTTP or MCP adapter receives this value.
type RuntimeRunner interface {
	Reconcile(context.Context) error
}

// RuntimeOptions wires the internal GraphRuntime without publishing its
// implementation or its recovery stores as an external management API.
type RuntimeOptions struct {
	ProjectID       kernel.ProjectID
	Store           Store
	PhaseController PhaseController
	Selection       RuntimeSelectionRuntime
	Scheduling      RuntimeSchedulingStateProvider
}

// NewRuntime constructs the single process-internal GraphRuntime for a
// project. Store must be the authoritative Coordination store; Runtime never
// owns a second graph representation.
func NewRuntime(options RuntimeOptions) (RuntimeRunner, error) {
	if err := kernel.RequireID("project_id", options.ProjectID); err != nil {
		return nil, err
	}
	store, ok := options.Store.(graphRuntimeStore)
	if !ok || store == nil {
		return nil, fmt.Errorf("coordination runtime requires an authoritative runtime-capable store")
	}
	if options.PhaseController == nil {
		return nil, fmt.Errorf("coordination runtime requires a phase controller")
	}
	runtime := newGraphRuntime(options.ProjectID, store, options.PhaseController)
	if options.Selection != nil {
		runtime.selectionRuntime = options.Selection
	}
	if options.Scheduling != nil {
		runtime.schedulingStateProvider = options.Scheduling
	}
	return runtime, nil
}

func (r *graphRuntime) Reconcile(ctx context.Context) error {
	return r.reconcile(ctx)
}

type RuntimeSelectionRuntime interface {
	SelectRunnable(ctx context.Context, request RuntimeSelectionRequest) ([]PhaseEndpoint, error)
}

type RuntimeSelectionRequest struct {
	Candidates []RuntimeCandidate
	Capacity   RuntimeCapacity
	Budget     RuntimeBudgetStatus
}

type RuntimeSchedulingStateProvider interface {
	RuntimeSchedulingState(ctx context.Context) (RuntimeSchedulingState, error)
}

type RuntimeSchedulingState struct {
	Capacity RuntimeCapacity
	Budget   RuntimeBudgetStatus
}

type RuntimeCandidate struct {
	Endpoint                 PhaseEndpoint
	Runnable                 bool
	CapacityCost             int
	CapabilityMatched        bool
	LatestMainMergeCandidate bool
	UnblocksDependents       bool
	WriteConflictRisk        int
	Exploratory              bool
}

type RuntimeCapacity struct {
	Desired  int `json:"desired"`
	Healthy  int `json:"healthy"`
	Active   int `json:"active"`
	Revision int `json:"revision"`
}

func (c RuntimeCapacity) Available() int {
	limit := c.Desired
	if c.Healthy < limit {
		limit = c.Healthy
	}
	available := limit - c.Active
	if available < 0 {
		return 0
	}
	return available
}

type RuntimeBudgetStatus struct {
	TokensUsed           int
	CostUSD              float64
	WallTimeMS           int
	AgentInvocationsUsed int
	RetriesUsed          int
}

type fixedSchedulingStateProvider struct {
	state RuntimeSchedulingState
}

func (p fixedSchedulingStateProvider) RuntimeSchedulingState(_ context.Context) (RuntimeSchedulingState, error) {
	return p.state, nil
}

type fixedCapacitySelectionRuntime struct{}

func (s fixedCapacitySelectionRuntime) SelectRunnable(_ context.Context, request RuntimeSelectionRequest) ([]PhaseEndpoint, error) {
	return selectRuntimeEndpoints(runtimeCandidateEndpoints(request.Candidates), request.Capacity.Available()), nil
}
