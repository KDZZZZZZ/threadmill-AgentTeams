package scheduler

import (
	"context"
	"sort"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type VerifyLevel string

const (
	VerifyBasic    VerifyLevel = "basic"
	VerifyStandard VerifyLevel = "standard"
	VerifyStrict   VerifyLevel = "strict"
)

type ExplorationLevel string

const (
	ExplorationNone     ExplorationLevel = "none"
	ExplorationTargeted ExplorationLevel = "targeted"
	ExplorationBroad    ExplorationLevel = "broad"
)

type BudgetPolicy struct {
	MaxTokens           int              `json:"max_tokens,omitempty"`
	MaxCostUSD          float64          `json:"max_cost_usd,omitempty"`
	MaxWallTimeMS       int              `json:"max_wall_time_ms,omitempty"`
	MaxAgentInvocations int              `json:"max_agent_invocations,omitempty"`
	MaxRetries          int              `json:"max_retries,omitempty"`
	VerifyLevel         VerifyLevel      `json:"verify_level"`
	ExplorationLevel    ExplorationLevel `json:"exploration_level"`
}

type BudgetStatus = coordination.RuntimeBudgetStatus

type Candidate = coordination.RuntimeCandidate

type Capacity = coordination.RuntimeCapacity

type CapacityLedger struct {
	mu       sync.Mutex
	capacity Capacity
}

type BudgetLedger struct {
	mu     sync.Mutex
	budget BudgetStatus
}

type SchedulingStateProvider struct {
	capacity *CapacityLedger
	budget   *BudgetLedger
}

func NewCapacityLedger(healthy, desired int) *CapacityLedger {
	if healthy < 0 {
		healthy = 0
	}
	if desired < 0 {
		desired = 0
	}
	return &CapacityLedger{capacity: Capacity{Desired: desired, Healthy: healthy, Revision: 1}}
}

func (l *CapacityLedger) Snapshot() Capacity {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity
}

func (l *CapacityLedger) SetDesired(_ context.Context, expectedRevision int, desired int) (Capacity, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if expectedRevision != l.capacity.Revision {
		return l.capacity, kernel.RevisionConflict(kernel.Revision(expectedRevision), kernel.Revision(l.capacity.Revision))
	}
	if desired < 0 {
		return l.capacity, kernel.InvalidArgument("desired concurrency must be zero or greater")
	}
	l.capacity.Desired = desired
	l.capacity.Revision++
	return l.capacity, nil
}

// Observe records scheduler-owned health and active-invocation facts. It never
// cancels active work when desired capacity is later reduced.
func (l *CapacityLedger) Observe(_ context.Context, healthy, active int) (Capacity, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if healthy < 0 || active < 0 || active > healthy {
		return l.capacity, kernel.InvalidArgument("observed capacity requires 0 <= active <= healthy")
	}
	if l.capacity.Healthy == healthy && l.capacity.Active == active {
		return l.capacity, nil
	}
	l.capacity.Healthy = healthy
	l.capacity.Active = active
	l.capacity.Revision++
	return l.capacity, nil
}

func NewBudgetLedger(initial BudgetStatus) *BudgetLedger {
	return &BudgetLedger{budget: initial}
}

func (l *BudgetLedger) Snapshot() BudgetStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.budget
}

func (l *BudgetLedger) Observe(_ context.Context, budget BudgetStatus) (BudgetStatus, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if budget.TokensUsed < 0 || budget.CostUSD < 0 || budget.WallTimeMS < 0 || budget.AgentInvocationsUsed < 0 || budget.RetriesUsed < 0 {
		return l.budget, kernel.InvalidArgument("budget usage cannot be negative")
	}
	l.budget = budget
	return l.budget, nil
}

func NewSchedulingStateProvider(capacity *CapacityLedger, budget *BudgetLedger) *SchedulingStateProvider {
	return &SchedulingStateProvider{capacity: capacity, budget: budget}
}

func (p *SchedulingStateProvider) RuntimeSchedulingState(ctx context.Context) (coordination.RuntimeSchedulingState, error) {
	if err := ctx.Err(); err != nil {
		return coordination.RuntimeSchedulingState{}, err
	}
	state := coordination.RuntimeSchedulingState{}
	if p != nil && p.capacity != nil {
		state.Capacity = p.capacity.Snapshot()
	}
	if p != nil && p.budget != nil {
		state.Budget = p.budget.Snapshot()
	}
	return state, nil
}

type Scheduler struct {
	policy BudgetPolicy
}

func New(policy BudgetPolicy) Scheduler {
	return Scheduler{policy: policy}
}

func (s Scheduler) SelectRunnable(_ context.Context, request coordination.RuntimeSelectionRequest) ([]coordination.PhaseEndpoint, error) {
	return s.Select(request.Candidates, request.Capacity, request.Budget), nil
}

func (s Scheduler) Select(candidates []Candidate, capacity Capacity, budget BudgetStatus) []coordination.PhaseEndpoint {
	available := capacity.Available()
	if available <= 0 {
		return nil
	}
	filtered := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.Runnable || !candidate.CapabilityMatched {
			continue
		}
		if candidate.CapacityCost <= 0 {
			candidate.CapacityCost = 1
		}
		if candidate.CapacityCost > available {
			continue
		}
		if !s.withinBudget(candidate, budget) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left := score(filtered[i], s.policy, budget)
		right := score(filtered[j], s.policy, budget)
		if left == right {
			return endpointLess(filtered[i].Endpoint.Ref, filtered[j].Endpoint.Ref)
		}
		return left > right
	})
	selected := make([]coordination.PhaseEndpoint, 0, len(filtered))
	used := 0
	for _, candidate := range filtered {
		if used+candidate.CapacityCost > available {
			continue
		}
		selected = append(selected, candidate.Endpoint)
		used += candidate.CapacityCost
	}
	return selected
}

func (s Scheduler) withinBudget(candidate Candidate, budget BudgetStatus) bool {
	if s.policy.MaxAgentInvocations > 0 && budget.AgentInvocationsUsed >= s.policy.MaxAgentInvocations {
		return candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify
	}
	if s.policy.MaxRetries > 0 && budget.RetriesUsed >= s.policy.MaxRetries {
		return candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify
	}
	if s.policy.MaxTokens > 0 && budget.TokensUsed >= s.policy.MaxTokens {
		return candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify
	}
	if s.policy.MaxCostUSD > 0 && budget.CostUSD >= s.policy.MaxCostUSD {
		return candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify
	}
	if s.policy.MaxWallTimeMS > 0 && budget.WallTimeMS >= s.policy.MaxWallTimeMS {
		return candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify
	}
	return true
}

func score(candidate Candidate, policy BudgetPolicy, budget BudgetStatus) int {
	value := 0
	if candidate.LatestMainMergeCandidate {
		value += 1000
	}
	if candidate.UnblocksDependents {
		value += 700
	}
	switch candidate.Endpoint.Ref.EndpointID {
	case coordination.EndpointVerify:
		value += 600
	case coordination.EndpointExecute:
		value += 300
	case coordination.EndpointPlan:
		value += 100
	}
	if candidate.Exploratory {
		value -= 200
	}
	if budgetConstrained(policy, budget) && candidate.Endpoint.Ref.EndpointID == coordination.EndpointVerify {
		value += 500
	}
	value -= candidate.WriteConflictRisk * 25
	return value
}

func budgetConstrained(policy BudgetPolicy, budget BudgetStatus) bool {
	return (policy.MaxAgentInvocations > 0 && budget.AgentInvocationsUsed >= policy.MaxAgentInvocations) ||
		(policy.MaxRetries > 0 && budget.RetriesUsed >= policy.MaxRetries) ||
		(policy.MaxTokens > 0 && budget.TokensUsed >= policy.MaxTokens) ||
		(policy.MaxCostUSD > 0 && budget.CostUSD >= policy.MaxCostUSD) ||
		(policy.MaxWallTimeMS > 0 && budget.WallTimeMS >= policy.MaxWallTimeMS)
}

func endpointLess(left, right coordination.PhaseEndpointRef) bool {
	if left.TaskID == right.TaskID {
		return phaseRank(left.EndpointID) < phaseRank(right.EndpointID)
	}
	return left.TaskID < right.TaskID
}

func phaseRank(endpointID coordination.EndpointID) int {
	switch endpointID {
	case coordination.EndpointPlan:
		return 0
	case coordination.EndpointExecute:
		return 1
	case coordination.EndpointVerify:
		return 2
	default:
		return 99
	}
}
