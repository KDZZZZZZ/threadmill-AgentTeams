package app

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

type productionTargetedVerifyFailureRuntime interface {
	FailTargetedInvocation(context.Context, kernel.InvocationID) error
}

type productionTargetedVerifyExecutionMonitor struct {
	db        *sql.DB
	projectID kernel.ProjectID
	registry  *productionTargetedVerifyRegistry
	provider  productionPhaseExecutionProvider
	runtime   productionTargetedVerifyFailureRuntime
	now       func() time.Time
	mu        sync.Mutex
	activeAt  map[string]time.Time
	idleSince map[string]time.Time
	cleaned   map[string]struct{}
}

type productionTargetedVerifyExecutionTarget struct {
	invocationID kernel.InvocationID
	status       runtimepkg.InvocationStatus
	execution    agentteams.AgentTeamsExecutionRef
	startedAt    time.Time
	expiresAt    time.Time
}

func newProductionTargetedVerifyExecutionMonitor(db *sql.DB, projectID kernel.ProjectID, registry *productionTargetedVerifyRegistry, provider productionPhaseExecutionProvider, runtime productionTargetedVerifyFailureRuntime, now func() time.Time) (*productionTargetedVerifyExecutionMonitor, error) {
	if db == nil || kernel.IsZeroID(projectID) || registry == nil || provider == nil || runtime == nil {
		return nil, kernel.InvalidArgument("targeted verify execution monitor requires database, project, registry, provider, and runtime")
	}
	if now == nil {
		now = time.Now
	}
	return &productionTargetedVerifyExecutionMonitor{
		db: db, projectID: projectID, registry: registry, provider: provider, runtime: runtime, now: now,
		activeAt: make(map[string]time.Time), idleSince: make(map[string]time.Time), cleaned: make(map[string]struct{}),
	}, nil
}

// Reconcile watches only targeted-verify invocations registered in this
// process and only after their AgentTeams execution has been durably
// dispatched. Provider terminal, expiry, or confirmed quiescence closes the
// runtime invocation as failed; runtime-terminal invocations are cleaned from
// AgentTeams only after provider terminal/expiry/quiescence confirms it is safe
// to revoke MCP and release the host slot.
func (m *productionTargetedVerifyExecutionMonitor) Reconcile(ctx context.Context) error {
	targets, err := m.registeredDispatchedExecutions(ctx)
	if err != nil {
		return err
	}
	reservedTargets, err := m.registeredExpiredReservedExecutions(ctx)
	if err != nil {
		return err
	}
	expiredTargets, err := m.expiredTargetedInvocations(ctx)
	if err != nil {
		return err
	}
	now := m.now().UTC()
	var reconcileErr error
	for _, target := range reservedTargets {
		if !target.runtimeActive() {
			continue
		}
		if err := m.runtime.FailTargetedInvocation(ctx, target.invocationID); err != nil && !kernel.IsCode(err, kernel.CodeStaleCommand) {
			reconcileErr = errors.Join(reconcileErr, err)
			continue
		}
		if err := m.provider.Terminate(ctx, target.execution, string(agentteams.TerminateCancel)); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
			continue
		}
		m.forgetExecution(target.execution.AgentTeamsTaskID)
	}
	for _, target := range expiredTargets {
		if m.registry.OwnsInvocation(target.invocationID) && target.runtimeActive() {
			continue
		}
		if target.runtimeActive() {
			if err := m.runtime.FailTargetedInvocation(ctx, target.invocationID); err != nil && !kernel.IsCode(err, kernel.CodeStaleCommand) {
				reconcileErr = errors.Join(reconcileErr, err)
				continue
			}
		} else if target.status != runtimepkg.InvocationFailed {
			continue
		}
		if strings.TrimSpace(target.execution.AgentTeamsTaskID) == "" {
			continue
		}
		if err := m.provider.Terminate(ctx, target.execution, string(agentteams.TerminateCancel)); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
			continue
		}
		m.forgetExecution(target.execution.AgentTeamsTaskID)
	}
	for _, target := range targets {
		if target.runtimeTerminal() {
			reconcileErr = errors.Join(reconcileErr, m.cleanupTerminal(ctx, target, now))
			continue
		}
		if !target.runtimeActive() {
			continue
		}
		failed := !target.expiresAt.After(now)
		if !failed {
			var providerTerminal bool
			providerTerminal, err = m.provider.ExecutionTerminal(ctx, target.execution)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
				continue
			}
			failed = providerTerminal
		}
		if !failed {
			activity, err := m.provider.ExecutionActivity(ctx, target.execution)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
				continue
			}
			// A failed QwenPaw turn can leave TeamHarness at in_progress. Once
			// this invocation has been observed and the carrier is durably idle,
			// host quiescence closes it instead of waiting until expiry.
			failed = m.executionAbandoned(target, activity, now)
		}
		if !failed {
			continue
		}
		if err := m.runtime.FailTargetedInvocation(ctx, target.invocationID); err != nil && !kernel.IsCode(err, kernel.CodeStaleCommand) {
			reconcileErr = errors.Join(reconcileErr, err)
			continue
		}
		m.forgetExecution(target.execution.AgentTeamsTaskID)
	}
	return reconcileErr
}

func (m *productionTargetedVerifyExecutionMonitor) cleanupTerminal(ctx context.Context, target productionTargetedVerifyExecutionTarget, now time.Time) error {
	if m.cleanedExecution(target.execution.AgentTeamsTaskID) {
		return nil
	}
	if target.status == runtimepkg.InvocationCompleted {
		if err := m.provider.FinalizeExecution(ctx, target.execution, "Threadmill accepted the targeted verification result"); err != nil {
			return err
		}
	}
	terminal := !target.expiresAt.After(now)
	if !terminal {
		providerTerminal, err := m.provider.ExecutionTerminal(ctx, target.execution)
		if err != nil {
			return err
		}
		terminal = providerTerminal
	}
	if !terminal {
		activity, err := m.provider.ExecutionActivity(ctx, target.execution)
		if err != nil {
			return err
		}
		terminal = m.executionAbandoned(target, activity, now)
	}
	if !terminal {
		return nil
	}
	if err := m.provider.Terminate(ctx, target.execution, string(agentteams.TerminateCancel)); err != nil {
		return err
	}
	m.markCleaned(target.execution.AgentTeamsTaskID)
	m.forgetExecution(target.execution.AgentTeamsTaskID)
	return nil
}

func (m *productionTargetedVerifyExecutionMonitor) executionAbandoned(target productionTargetedVerifyExecutionTarget, activity agentteams.HostActivity, now time.Time) bool {
	key := strings.TrimSpace(target.execution.AgentTeamsTaskID)
	if key == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if productionPhaseActivityIsActive(activity) {
		m.activeAt[key] = now
		delete(m.idleSince, key)
		return false
	}
	if _, observed := m.activeAt[key]; !observed {
		if productionPhaseActivityObserved(activity, target.startedAt) {
			m.activeAt[key] = now
			idleAt := now
			if !activity.LastFinishAt.IsZero() && !activity.LastFinishAt.UTC().Before(target.startedAt.UTC()) {
				idleAt = activity.LastFinishAt.UTC()
			}
			m.idleSince[key] = idleAt
			return !idleAt.Add(productionPhaseExecutionQuiescenceGap).After(now)
		}
		return productionExecutionAbandoned(activity, target.startedAt, now, productionPhaseExecutionColdQuiescenceGap)
	}
	idleAt, observed := m.idleSince[key]
	if !observed {
		m.idleSince[key] = now
		return false
	}
	return !idleAt.Add(productionPhaseExecutionQuiescenceGap).After(now)
}

func (m *productionTargetedVerifyExecutionMonitor) registeredDispatchedExecutions(ctx context.Context) ([]productionTargetedVerifyExecutionTarget, error) {
	invocations := m.registry.allInvocations()
	targets := make([]productionTargetedVerifyExecutionTarget, 0, len(invocations))
	for _, invocationID := range invocations {
		target, ok, err := m.dispatchedExecution(ctx, invocationID)
		if err != nil {
			return nil, err
		}
		if ok {
			targets = append(targets, target)
		}
	}
	return targets, nil
}

func (m *productionTargetedVerifyExecutionMonitor) registeredExpiredReservedExecutions(ctx context.Context) ([]productionTargetedVerifyExecutionTarget, error) {
	invocations := m.registry.allInvocations()
	targets := make([]productionTargetedVerifyExecutionTarget, 0, len(invocations))
	for _, invocationID := range invocations {
		target, ok, err := m.reservedExpiredExecution(ctx, invocationID)
		if err != nil {
			return nil, err
		}
		if ok {
			targets = append(targets, target)
		}
	}
	return targets, nil
}

func (m *productionTargetedVerifyExecutionMonitor) dispatchedExecution(ctx context.Context, invocationID kernel.InvocationID) (productionTargetedVerifyExecutionTarget, bool, error) {
	var target productionTargetedVerifyExecutionTarget
	var status string
	err := m.db.QueryRowContext(ctx, `
SELECT r.invocation_id, r.status, e.agentteams_task_id, e.host_ref, e.created_at, r.expires_at
FROM runtime_invocations r
JOIN phase_agentteams_host_states h
  ON h.invocation_id=r.invocation_id
JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.invocation_id=$2
  AND r.role='verifier'
  AND e.state='dispatched'
ORDER BY e.created_at DESC, e.agentteams_task_id DESC
LIMIT 1`, m.projectID, invocationID).Scan(
		&target.invocationID, &status, &target.execution.AgentTeamsTaskID,
		&target.execution.HostRef, &target.startedAt, &target.expiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTargetedVerifyExecutionTarget{}, false, nil
	}
	if err != nil {
		return productionTargetedVerifyExecutionTarget{}, false, err
	}
	target.status = runtimepkg.InvocationStatus(status)
	target.execution.InvocationID = target.invocationID
	return target, true, nil
}

func (m *productionTargetedVerifyExecutionMonitor) reservedExpiredExecution(ctx context.Context, invocationID kernel.InvocationID) (productionTargetedVerifyExecutionTarget, bool, error) {
	var target productionTargetedVerifyExecutionTarget
	var status string
	err := m.db.QueryRowContext(ctx, `
SELECT r.invocation_id, r.status, e.agentteams_task_id, e.host_ref, e.created_at, r.expires_at
FROM runtime_invocations r
JOIN phase_agentteams_host_states h
  ON h.invocation_id=r.invocation_id
JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.invocation_id=$2
  AND r.role='verifier'
  AND r.status IN ('prepared','running','waiting')
  AND r.expires_at <= $3
  AND e.state='reserved'
ORDER BY e.created_at DESC, e.agentteams_task_id DESC
LIMIT 1`, m.projectID, invocationID, m.now().UTC()).Scan(
		&target.invocationID, &status, &target.execution.AgentTeamsTaskID,
		&target.execution.HostRef, &target.startedAt, &target.expiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTargetedVerifyExecutionTarget{}, false, nil
	}
	if err != nil {
		return productionTargetedVerifyExecutionTarget{}, false, err
	}
	target.status = runtimepkg.InvocationStatus(status)
	target.execution.InvocationID = target.invocationID
	return target, true, nil
}

func (m *productionTargetedVerifyExecutionMonitor) expiredTargetedInvocations(ctx context.Context) ([]productionTargetedVerifyExecutionTarget, error) {
	rows, err := m.db.QueryContext(ctx, `
SELECT r.invocation_id, r.status, COALESCE(e.agentteams_task_id,''), COALESCE(e.host_ref,''), COALESCE(e.created_at, r.created_at), r.expires_at
FROM runtime_invocations r
LEFT JOIN phase_agentteams_host_states h
  ON h.invocation_id=r.invocation_id
LEFT JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.role='verifier'
  AND r.status IN ('prepared','running','waiting','failed')
  AND r.expires_at <= $2
  AND r.binding_ref LIKE 'targeted-verify-binding:%'
  AND (e.state IS NULL OR e.state='reserved')
ORDER BY r.created_at, r.invocation_id`, m.projectID, m.now().UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make([]productionTargetedVerifyExecutionTarget, 0)
	for rows.Next() {
		var target productionTargetedVerifyExecutionTarget
		var status string
		if err := rows.Scan(
			&target.invocationID, &status, &target.execution.AgentTeamsTaskID,
			&target.execution.HostRef, &target.startedAt, &target.expiresAt,
		); err != nil {
			return nil, err
		}
		target.status = runtimepkg.InvocationStatus(status)
		target.execution.InvocationID = target.invocationID
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (target productionTargetedVerifyExecutionTarget) runtimeActive() bool {
	switch target.status {
	case runtimepkg.InvocationPrepared, runtimepkg.InvocationRunning, runtimepkg.InvocationWaiting:
		return true
	default:
		return false
	}
}

func (target productionTargetedVerifyExecutionTarget) runtimeTerminal() bool {
	switch target.status {
	case runtimepkg.InvocationCompleted, runtimepkg.InvocationFailed, runtimepkg.InvocationStopped:
		return true
	default:
		return false
	}
}

func (m *productionTargetedVerifyExecutionMonitor) forgetExecution(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.activeAt, strings.TrimSpace(key))
	delete(m.idleSince, strings.TrimSpace(key))
}

func (m *productionTargetedVerifyExecutionMonitor) cleanedExecution(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.cleaned[key]
	return ok
}

func (m *productionTargetedVerifyExecutionMonitor) markCleaned(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleaned[key] = struct{}{}
}

func (r *productionTargetedVerifyRegistry) allInvocations() []kernel.InvocationID {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]kernel.InvocationID, 0, len(r.byInvocation))
	for invocationID := range r.byInvocation {
		out = append(out, invocationID)
	}
	sortKernelInvocationIDs(out)
	return out
}

func sortKernelInvocationIDs(ids []kernel.InvocationID) {
	if len(ids) < 2 {
		return
	}
	for i := 1; i < len(ids); i++ {
		current := ids[i]
		j := i - 1
		for j >= 0 && ids[j] > current {
			ids[j+1] = ids[j]
			j--
		}
		ids[j+1] = current
	}
}
