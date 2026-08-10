package coordination

import (
	"context"
)

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
