package coordination

import (
	"context"
	"reflect"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type MemoryDecisionLog struct {
	mu        sync.Mutex
	decisions map[decisionKey]memoryDecision
}

type memoryDecision struct {
	kind       DecisionKind
	transition GraphTransition
}

type decisionKey struct {
	projectID kernel.ProjectID
	ref       string
}

func NewMemoryDecisionLog() *MemoryDecisionLog {
	return &MemoryDecisionLog{decisions: make(map[decisionKey]memoryDecision)}
}

func (l *MemoryDecisionLog) RegisterReplacePending(projectID kernel.ProjectID, decisionRef kernel.IdempotencyKey) error {
	if kernel.IsZeroID(projectID) {
		return kernel.InvalidArgument("project_id is required")
	}
	if kernel.IsZeroID(decisionRef) {
		return kernel.InvalidArgument("decision_ref is required")
	}
	return l.put(decisionKey{projectID: projectID, ref: string(decisionRef)}, memoryDecision{kind: DecisionReplacePending})
}

func (l *MemoryDecisionLog) RegisterTransition(projectID kernel.ProjectID, transitionRef string, transition GraphTransition) error {
	if kernel.IsZeroID(projectID) {
		return kernel.InvalidArgument("project_id is required")
	}
	if transitionRef == "" {
		return kernel.InvalidArgument("transition_ref is required")
	}
	if err := validateTransitionShape(transition); err != nil {
		return err
	}
	return l.put(decisionKey{projectID: projectID, ref: transitionRef}, memoryDecision{kind: DecisionTransition, transition: transition})
}

func (l *MemoryDecisionLog) AuthorizeReplacePending(_ context.Context, projectID kernel.ProjectID, decisionRef kernel.IdempotencyKey) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	decision, ok := l.decisions[decisionKey{projectID: projectID, ref: string(decisionRef)}]
	if !ok {
		return kernel.Forbidden("replacePending requires a persisted Task Manager decision")
	}
	if decision.kind != DecisionReplacePending {
		return kernel.Forbidden("decision_ref is not a replacePending decision")
	}
	return nil
}

func (l *MemoryDecisionLog) ResolveTransition(_ context.Context, projectID kernel.ProjectID, transitionRef string) (GraphTransition, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	decision, ok := l.decisions[decisionKey{projectID: projectID, ref: transitionRef}]
	if !ok {
		return GraphTransition{}, kernel.Forbidden("transition requires a persisted Task Manager decision")
	}
	if decision.kind != DecisionTransition {
		return GraphTransition{}, kernel.Forbidden("transition_ref is not a transition decision")
	}
	return decision.transition, nil
}

func (l *MemoryDecisionLog) put(key decisionKey, decision memoryDecision) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if existing, ok := l.decisions[key]; ok {
		if existing.kind != decision.kind || !reflect.DeepEqual(existing.transition, decision.transition) {
			return kernel.IdempotencyConflict()
		}
		return nil
	}
	l.decisions[key] = decision
	return nil
}
