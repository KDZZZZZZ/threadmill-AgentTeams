package app

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// A cold QwenPaw worker needs time to restore FileSync state, reload its
// workspace, connect invocation-scoped MCP, and receive the Matrix assignment.
// Treating an idle host as abandoned before that bootstrap window elapsed
// cancels a valid provider task before the model can acknowledge it.
const (
	// QwenPaw can report idle between model turns and while a native tool is
	// installing or waiting on an external process.  That gap is not proof that
	// the AgentTeams task was abandoned: TeamHarness still owns an in_progress
	// task and the same carrier may resume the turn.  Keep a deliberately wide
	// quiescence window so ordinary multi-minute verification work cannot be
	// mistaken for a dead invocation; the invocation lease remains the hard
	// upper bound.
	// Once QwenPaw reports a completed model turn and the carrier remains idle,
	// the invocation either submitted its authoritative PhaseOutput or it did
	// not. Keep a short settling window for provider bookkeeping, then return a
	// missing-output failure to Manager instead of pinning capacity for minutes.
	productionPhaseExecutionQuiescenceGap     = 90 * time.Second
	productionPhaseExecutionColdQuiescenceGap = 3 * time.Minute
)

// productionPhaseFailureReporter is the internal Runtime boundary that closes
// a trusted persisted Phase command. It is deliberately not exposed as an MCP
// tool and does not let the Provider or model choose graph state.
type productionPhaseFailureReporter interface {
	FailInvocation(context.Context, coordination.PhaseCommand) error
}

type productionPhaseExecutionProvider interface {
	FinalizeExecution(context.Context, agentteams.AgentTeamsExecutionRef, string) error
	ExecutionTerminal(context.Context, agentteams.AgentTeamsExecutionRef) (bool, error)
	ExecutionActivity(context.Context, agentteams.AgentTeamsExecutionRef) (agentteams.HostActivity, error)
	Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error
}

type productionPhaseExecutionMonitor struct {
	db        *sql.DB
	projectID kernel.ProjectID
	provider  productionPhaseExecutionProvider
	failures  productionPhaseFailureReporter
	now       func() time.Time
	mu        sync.Mutex
	activeAt  map[string]time.Time
	idleSince map[string]time.Time
}

type productionPhaseExecutionTarget struct {
	command       coordination.PhaseCommand
	execution     agentteams.AgentTeamsExecutionRef
	runtimeStatus string
	startedAt     time.Time
	expiresAt     time.Time
}

func newProductionPhaseExecutionMonitor(db *sql.DB, projectID kernel.ProjectID, provider productionPhaseExecutionProvider, failures productionPhaseFailureReporter, now func() time.Time) (*productionPhaseExecutionMonitor, error) {
	if db == nil || kernel.IsZeroID(projectID) || provider == nil || failures == nil {
		return nil, kernel.InvalidArgument("production Phase execution monitor requires database, project, provider, and failure reporter")
	}
	if now == nil {
		now = time.Now
	}
	return &productionPhaseExecutionMonitor{
		db: db, projectID: projectID, provider: provider, failures: failures, now: now,
		activeAt: make(map[string]time.Time), idleSince: make(map[string]time.Time),
	}, nil
}

// Reconcile observes only durable executions bound to a Runtime-authenticated
// start/resume command. A terminal, expired, or demonstrably quiescent Provider
// execution is converted into PhaseInvocationFailed by the Controller; graph
// business recovery remains a later Task Manager decision.
func (m *productionPhaseExecutionMonitor) Reconcile(ctx context.Context) error {
	cleanupErr := m.cleanupRuntimeTerminalExecutions(ctx)
	reservedTargets, err := m.expiredReservedExecutions(ctx)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	reservedErr := m.cleanupExpiredReservedExecutions(ctx, reservedTargets)
	targets, err := m.activeExecutions(ctx)
	if err != nil {
		return errors.Join(cleanupErr, reservedErr, err)
	}
	now := m.now().UTC()
	reconcileErr := errors.Join(cleanupErr, reservedErr)
	for _, target := range targets {
		failed := !target.expiresAt.After(now)
		if !failed {
			// Probe activity first. ProductionClient uses this observation to
			// refresh the controller's LastActiveAt for every durable active slot;
			// probing TeamHarness terminal state first can consume the reconcile
			// deadline and let controller auto-sleep kill a live model turn.
			var activity agentteams.HostActivity
			activity, err = m.provider.ExecutionActivity(ctx, target.execution)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
				continue
			}
			failed = m.executionAbandoned(target, activity, now)
		}
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
			continue
		}
		if err := m.failures.FailInvocation(ctx, target.command); err != nil {
			reconcileErr = errors.Join(reconcileErr, err)
		} else {
			m.forgetExecution(target.execution.AgentTeamsTaskID)
		}
	}
	return reconcileErr
}

func (m *productionPhaseExecutionMonitor) cleanupExpiredReservedExecutions(ctx context.Context, targets []productionPhaseExecutionTarget) error {
	var cleanupErr error
	for _, target := range targets {
		err := m.failures.FailInvocation(ctx, target.command)
		if err != nil && !kernel.IsCode(err, kernel.CodeStaleCommand) {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if err := m.provider.Terminate(ctx, target.execution, string(agentteams.TerminateCancel)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		m.forgetExecution(target.execution.AgentTeamsTaskID)
	}
	return cleanupErr
}

// cleanupRuntimeTerminalExecutions is the second half of successful Phase
// completion. SubmitPhaseOutput synchronously fences only Threadmill authority
// so its own MCP response cannot be interrupted. Once TeamHarness reports the
// provider task terminal, this loop performs destructive MCP deletion and host
// release through Adapter.Terminate.
func (m *productionPhaseExecutionMonitor) cleanupRuntimeTerminalExecutions(ctx context.Context) error {
	targets, err := m.runtimeTerminalExecutions(ctx)
	if err != nil {
		return err
	}
	now := m.now().UTC()
	var cleanupErr error
	for _, target := range targets {
		if target.runtimeStatus == "completed" {
			if err := m.provider.FinalizeExecution(ctx, target.execution, "Threadmill accepted the authoritative phase result"); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
				continue
			}
		}
		providerTerminal, err := m.provider.ExecutionTerminal(ctx, target.execution)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		terminal := providerTerminal || !target.expiresAt.After(now)
		if !terminal {
			activity, err := m.provider.ExecutionActivity(ctx, target.execution)
			if err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
				continue
			}
			// Runtime authority is already terminal. Provider bookkeeping may
			// still say in_progress after a model/runtime error, so reclaim the
			// carrier once the invocation is observably quiescent.
			terminal = m.executionAbandoned(target, activity, now)
		}
		if !terminal {
			continue
		}
		if err := m.provider.Terminate(ctx, target.execution, string(agentteams.TerminateCancel)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		m.forgetExecution(target.execution.AgentTeamsTaskID)
	}
	return cleanupErr
}

func (m *productionPhaseExecutionMonitor) executionAbandoned(target productionPhaseExecutionTarget, activity agentteams.HostActivity, now time.Time) bool {
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

func productionPhaseActivityIsActive(activity agentteams.HostActivity) bool {
	return strings.EqualFold(strings.TrimSpace(activity.Status), "running") ||
		activity.RunningTaskCount > 0
}

func productionPhaseActivityObserved(activity agentteams.HostActivity, startedAt time.Time) bool {
	return productionPhaseActivityIsActive(activity) ||
		(!activity.LastRunAt.IsZero() && !activity.LastRunAt.UTC().Before(startedAt.UTC())) ||
		(!activity.LastFinishAt.IsZero() && !activity.LastFinishAt.UTC().Before(startedAt.UTC()))
}

func (m *productionPhaseExecutionMonitor) forgetExecution(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.activeAt, strings.TrimSpace(key))
	delete(m.idleSince, strings.TrimSpace(key))
}

func (m *productionPhaseExecutionMonitor) activeExecutions(ctx context.Context) ([]productionPhaseExecutionTarget, error) {
	rows, err := m.db.QueryContext(ctx, `
SELECT c.command_id, c.task_id, c.endpoint_id, c.generation, c.binding_ref, c.lease_ref, c.action, c.cause_ref,
       e.invocation_id, e.agentteams_task_id, e.host_ref, r.status, e.created_at, r.expires_at
FROM runtime_invocations r
JOIN coordination_phase_commands c
  ON c.project_id=r.project_id
 AND c.task_id=r.task_id
 AND c.endpoint_id=r.endpoint_id
 AND c.generation=r.generation
 AND c.binding_ref=r.binding_ref
 AND c.lease_ref=r.lease_id
JOIN phase_agentteams_host_states h
  ON h.invocation_id=r.invocation_id
JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.role IN ('planner','executor','verifier')
  AND r.status IN ('prepared','running','waiting')
  AND c.action IN ('start','resume')
  AND c.accepted_at IS NOT NULL
  AND e.state='dispatched'
ORDER BY e.created_at, e.agentteams_task_id`, m.projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make([]productionPhaseExecutionTarget, 0)
	for rows.Next() {
		var target productionPhaseExecutionTarget
		var endpointID kernel.EndpointID
		if err := rows.Scan(
			&target.command.ID, &target.command.Endpoint.TaskID, &endpointID,
			&target.command.Generation, &target.command.BindingRef, &target.command.LeaseRef,
			&target.command.Action, &target.command.CauseRef,
			&target.execution.InvocationID, &target.execution.AgentTeamsTaskID, &target.execution.HostRef,
			&target.runtimeStatus, &target.startedAt, &target.expiresAt,
		); err != nil {
			return nil, err
		}
		target.command.Endpoint.EndpointID = endpointID
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (m *productionPhaseExecutionMonitor) runtimeTerminalExecutions(ctx context.Context) ([]productionPhaseExecutionTarget, error) {
	rows, err := m.db.QueryContext(ctx, `
SELECT c.command_id, c.task_id, c.endpoint_id, c.generation, c.binding_ref, c.lease_ref, c.action, c.cause_ref,
       e.invocation_id, e.agentteams_task_id, e.host_ref, r.status, e.created_at, r.expires_at
FROM runtime_invocations r
JOIN coordination_phase_commands c
  ON c.project_id=r.project_id
 AND c.task_id=r.task_id
 AND c.endpoint_id=r.endpoint_id
 AND c.generation=r.generation
 AND c.binding_ref=r.binding_ref
 AND c.lease_ref=r.lease_id
JOIN phase_agentteams_host_states h ON h.invocation_id=r.invocation_id
JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.role IN ('planner','executor','verifier')
  AND r.status IN ('completed','failed','stopped')
  AND c.action IN ('start','resume')
  AND c.accepted_at IS NOT NULL
  AND e.state='dispatched'
ORDER BY e.created_at, e.agentteams_task_id`, m.projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make([]productionPhaseExecutionTarget, 0)
	for rows.Next() {
		var target productionPhaseExecutionTarget
		var endpointID kernel.EndpointID
		if err := rows.Scan(
			&target.command.ID, &target.command.Endpoint.TaskID, &endpointID,
			&target.command.Generation, &target.command.BindingRef, &target.command.LeaseRef,
			&target.command.Action, &target.command.CauseRef,
			&target.execution.InvocationID, &target.execution.AgentTeamsTaskID, &target.execution.HostRef,
			&target.runtimeStatus, &target.startedAt, &target.expiresAt,
		); err != nil {
			return nil, err
		}
		target.command.Endpoint.EndpointID = endpointID
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (m *productionPhaseExecutionMonitor) expiredReservedExecutions(ctx context.Context) ([]productionPhaseExecutionTarget, error) {
	rows, err := m.db.QueryContext(ctx, `
SELECT c.command_id, c.task_id, c.endpoint_id, c.generation, c.binding_ref, c.lease_ref, c.action, c.cause_ref,
       e.invocation_id, e.agentteams_task_id, e.host_ref, r.status, e.created_at, r.expires_at
FROM runtime_invocations r
JOIN coordination_phase_commands c
  ON c.project_id=r.project_id
 AND c.task_id=r.task_id
 AND c.endpoint_id=r.endpoint_id
 AND c.generation=r.generation
 AND c.binding_ref=r.binding_ref
 AND c.lease_ref=r.lease_id
JOIN phase_agentteams_host_states h ON h.invocation_id=r.invocation_id
JOIN agentteams_execution_refs e
  ON e.invocation_id=r.invocation_id
 AND e.invocation_ref=h.invocation_ref
 AND e.agentteams_task_id=h.agentteams_task_id
 AND e.host_ref=h.host_ref
WHERE r.project_id=$1
  AND r.role IN ('planner','executor','verifier')
  AND r.status IN ('prepared','running','waiting')
  AND r.expires_at <= $2
  AND c.action IN ('start','resume')
  AND c.accepted_at IS NOT NULL
  AND e.state='reserved'
ORDER BY e.created_at, e.agentteams_task_id`, m.projectID, m.now().UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	targets := make([]productionPhaseExecutionTarget, 0)
	for rows.Next() {
		var target productionPhaseExecutionTarget
		var endpointID kernel.EndpointID
		if err := rows.Scan(
			&target.command.ID, &target.command.Endpoint.TaskID, &endpointID,
			&target.command.Generation, &target.command.BindingRef, &target.command.LeaseRef,
			&target.command.Action, &target.command.CauseRef,
			&target.execution.InvocationID, &target.execution.AgentTeamsTaskID, &target.execution.HostRef,
			&target.runtimeStatus, &target.startedAt, &target.expiresAt,
		); err != nil {
			return nil, err
		}
		target.command.Endpoint.EndpointID = endpointID
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func productionPhaseExecutionAbandoned(activity agentteams.HostActivity, startedAt, now time.Time) bool {
	// QwenPaw may briefly report idle during cold start, so retain the full
	// quiescence grace. After that grace, reset timestamps plus zero running
	// tasks are durable evidence that the physical worker restarted and no
	// longer carries the persisted invocation; failing it lets Manager re-open
	// the phase instead of waiting for the hour-long invocation expiry.
	return productionExecutionAbandoned(activity, startedAt, now, productionPhaseExecutionQuiescenceGap)
}
