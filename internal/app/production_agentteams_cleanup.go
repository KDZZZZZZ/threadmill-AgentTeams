package app

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	productionTaskManagerCleanupTimeout         = 5 * time.Second
	productionTaskManagerTerminalProbeTimeout   = 1 * time.Second
	productionTaskManagerExecutionQuiescenceGap = 10 * time.Second
	productionTaskManagerBrokenContextGrace     = 2 * time.Minute
)

type productionAgentTeamsTerminator interface {
	FinalizeExecution(context.Context, agentteams.AgentTeamsExecutionRef, string) error
	Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error
	FenceExecution(context.Context, agentteams.AgentTeamsExecutionRef) error
	ExecutionTerminal(context.Context, agentteams.AgentTeamsExecutionRef) (bool, error)
}

type productionTaskManagerExecutionCleanup struct {
	db         *sql.DB
	terminator productionAgentTeamsTerminator
	contexts   productionInvocationContextEnder
	now        func() time.Time
}

type productionAgentTeamsActivityObserver interface {
	ExecutionActivity(context.Context, agentteams.AgentTeamsExecutionRef) (agentteams.HostActivity, error)
}

type productionTaskManagerExecutionCleanupTarget struct {
	execution         agentteams.AgentTeamsExecutionRef
	invocation        productionCleanupInvocationRecord
	mode              agentteams.TerminateMode
	providerEnded     bool
	preparationFailed bool
	expired           bool
	runtimeEnd        bool
	probe             bool
	abandoned         bool
	startedAt         time.Time
	failedContextAt   sql.NullTime
}

type productionInvocationContextEnder interface {
	EndInvocation(context.Context, auth.Principal, kernel.InvocationID) error
}

type productionCleanupInvocationRecord struct {
	ID               kernel.InvocationID
	ActorPrincipalID kernel.ActorPrincipalID
	ProjectID        kernel.ProjectID
	Role             auth.Role
	Status           string
}

func newProductionTaskManagerExecutionCleanup(db *sql.DB, terminator productionAgentTeamsTerminator, contexts productionInvocationContextEnder, now func() time.Time) (*productionTaskManagerExecutionCleanup, error) {
	if db == nil || terminator == nil || contexts == nil {
		return nil, kernel.InvalidArgument("production Task Manager execution cleanup requires database, terminator, and context lifecycle")
	}
	if now == nil {
		now = time.Now
	}
	return &productionTaskManagerExecutionCleanup{db: db, terminator: terminator, contexts: contexts, now: now}, nil
}

func (c *productionTaskManagerExecutionCleanup) CleanupTaskManagerInvocations(ctx context.Context) error {
	if c == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), productionTaskManagerCleanupTimeout)
	defer cancel()
	targets, err := c.completedExecutions(cleanupCtx)
	if err != nil {
		return err
	}
	var cleanupErr error
	for _, target := range targets {
		if target.providerEnded && !target.runtimeEnd {
			// The provider execution is already fenced and its host slot is gone,
			// so an active Runtime invocation can never make further progress.
			// Close the old invocation and let the durable input retry path create
			// a fresh invocation ID and one-lifetime MCP bearer.
			target.abandoned = true
		}
		if target.preparationFailed && !target.runtimeEnd {
			// PrepareHost can fail after minting and revoking the invocation's
			// one-lifetime bearer, either before or after the physical slot claim.
			// The execution is still reserved, so no provider task was delegated.
			// Keeping the Runtime invocation active would only retry an authority
			// that must never be reactivated.
			target.abandoned = true
		}
		if target.runtimeEnd && target.probe && !target.expired {
			if target.invocation.Status == "completed" {
				if err := c.terminator.FinalizeExecution(cleanupCtx, target.execution, "Threadmill accepted the Task Manager decision"); err != nil {
					cleanupErr = errors.Join(cleanupErr, err)
					continue
				}
			}
			terminal, err := c.executionTerminal(cleanupCtx, target.execution)
			if err != nil {
				// Runtime authority is already terminal. TeamHarness bookkeeping
				// can become unreadable after a worker restart or partial submit;
				// in that case an observably quiescent invocation is still safe to
				// fence and reclaim. Preserve the provider error only while the host
				// remains active or its activity cannot be established.
				observer, ok := c.terminator.(productionAgentTeamsActivityObserver)
				if !ok {
					cleanupErr = errors.Join(cleanupErr, err)
					continue
				}
				activity, activityErr := observer.ExecutionActivity(cleanupCtx, target.execution)
				if activityErr != nil || !productionTaskManagerExecutionAbandoned(activity, target.startedAt, c.now().UTC()) {
					cleanupErr = errors.Join(cleanupErr, err, activityErr)
					continue
				}
				target.abandoned = true
				terminal = true
			}
			if !terminal {
				observer, ok := c.terminator.(productionAgentTeamsActivityObserver)
				if ok {
					activity, activityErr := observer.ExecutionActivity(cleanupCtx, target.execution)
					if activityErr != nil {
						cleanupErr = errors.Join(cleanupErr, activityErr)
						continue
					}
					if productionTaskManagerExecutionAbandoned(activity, target.startedAt, c.now().UTC()) {
						target.abandoned = true
					} else {
						// The authoritative Runtime work is over, but provider SUCCESS
						// finalization has not become observable yet. Fence graph/tool
						// authority while retaining its carrier for the next retry.
						if err := c.terminator.FenceExecution(cleanupCtx, target.execution); err != nil {
							cleanupErr = errors.Join(cleanupErr, err)
							continue
						}
						principal := auth.Principal{
							ActorPrincipalID: target.invocation.ActorPrincipalID,
							Kind:             auth.PrincipalAgent,
							ProjectID:        target.invocation.ProjectID,
							Role:             target.invocation.Role,
							InvocationID:     target.invocation.ID,
						}
						if err := c.contexts.EndInvocation(cleanupCtx, principal, target.invocation.ID); err != nil {
							cleanupErr = errors.Join(cleanupErr, err)
						}
						continue
					}
				} else {
					// The authoritative Runtime status already fences graph/tool calls.
					// Keep the bearer, Provider task, and slot alive long enough
					// for Runtime-owned provider finalization to become observable.
					if err := c.terminator.FenceExecution(cleanupCtx, target.execution); err != nil {
						cleanupErr = errors.Join(cleanupErr, err)
						continue
					}
					principal := auth.Principal{
						ActorPrincipalID: target.invocation.ActorPrincipalID,
						Kind:             auth.PrincipalAgent,
						ProjectID:        target.invocation.ProjectID,
						Role:             target.invocation.Role,
						InvocationID:     target.invocation.ID,
					}
					if err := c.contexts.EndInvocation(cleanupCtx, principal, target.invocation.ID); err != nil {
						cleanupErr = errors.Join(cleanupErr, err)
					}
					continue
				}
			}
		}
		if !target.runtimeEnd && !target.expired && !target.providerEnded && target.mode != agentteams.TerminateReleaseWait && !target.preparationFailed {
			if !target.probe {
				continue
			}
			abandoned, err := c.activeDispatchedExecutionAbandoned(cleanupCtx, target)
			if err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
				continue
			}
			if !abandoned {
				continue
			}
			target.abandoned = true
		}
		if err := c.terminator.Terminate(cleanupCtx, target.execution, string(target.mode)); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		principal := auth.Principal{
			ActorPrincipalID: target.invocation.ActorPrincipalID,
			Kind:             auth.PrincipalAgent,
			ProjectID:        target.invocation.ProjectID,
			Role:             target.invocation.Role,
			InvocationID:     target.invocation.ID,
		}
		if err := c.contexts.EndInvocation(cleanupCtx, principal, target.invocation.ID); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if target.expired || target.abandoned {
			cleanupErr = errors.Join(cleanupErr, c.failAbandonedInvocation(cleanupCtx, target.invocation))
		}
	}
	return cleanupErr
}

func (c *productionTaskManagerExecutionCleanup) completedExecutions(ctx context.Context) ([]productionTaskManagerExecutionCleanupTarget, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT e.invocation_id, e.agentteams_task_id, e.host_ref, e.created_at,
	r.actor_principal_id, r.project_id, r.role, r.status,
	(SELECT max(c.updated_at)
	 FROM production_context_invocations c
	 WHERE c.project_id=r.project_id
	   AND c.consumer_invocation_id=r.invocation_id
	   AND c.state='failed') AS failed_context_at,
	(r.status IN ('completed', 'failed', 'stopped')) AS runtime_terminal,
	(r.expires_at <= $2 OR r.created_at <= $2 - $3::interval) AS expired,
	(e.state = 'terminated') AS provider_terminated,
	(e.state = 'reserved'
	 AND EXISTS (
	   SELECT 1
	   FROM agent_invocation_tokens t
	   WHERE t.invocation_id=e.invocation_id
	     AND t.revoked_at IS NOT NULL
	 )) AS preparation_failed,
	(e.state = 'dispatched') AS probe_provider,
  CASE
    WHEN e.state = 'terminated' AND e.termination_mode = 'release_wait' THEN 'release_wait'
    ELSE 'cancel'
  END AS cleanup_mode
FROM agentteams_execution_refs e
JOIN runtime_invocations r ON r.invocation_id = e.invocation_id
WHERE r.role = $1
  AND (
	(r.status IN ('completed', 'failed', 'stopped') AND e.state IN ('reserved', 'dispatched'))
	    OR (
      r.status IN ('prepared', 'running', 'waiting')
	  AND e.state IN ('reserved', 'dispatched', 'terminated')
    )
    OR (e.state = 'terminated' AND e.termination_mode = 'release_wait' AND e.host_slot_released_at IS NULL)
  )
ORDER BY e.created_at, e.agentteams_task_id`, auth.RoleTaskManager, c.now().UTC(), taskManagerInvocationTTL.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []productionTaskManagerExecutionCleanupTarget
	for rows.Next() {
		var target productionTaskManagerExecutionCleanupTarget
		var mode agentteams.TerminateMode
		if err := rows.Scan(
			&target.execution.InvocationID,
			&target.execution.AgentTeamsTaskID,
			&target.execution.HostRef,
			&target.startedAt,
			&target.invocation.ActorPrincipalID,
			&target.invocation.ProjectID,
			&target.invocation.Role,
			&target.invocation.Status,
			&target.failedContextAt,
			&target.runtimeEnd,
			&target.expired,
			&target.providerEnded,
			&target.preparationFailed,
			&target.probe,
			&mode,
		); err != nil {
			return nil, err
		}
		target.invocation.ID = target.execution.InvocationID
		target.mode = mode
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (c *productionTaskManagerExecutionCleanup) activeDispatchedExecutionAbandoned(ctx context.Context, target productionTaskManagerExecutionCleanupTarget) (bool, error) {
	if target.failedContextAt.Valid && !target.failedContextAt.Time.UTC().After(c.now().UTC().Add(-productionTaskManagerBrokenContextGrace)) {
		return true, nil
	}
	var activityErr error
	observer, ok := c.terminator.(productionAgentTeamsActivityObserver)
	if ok {
		var activity agentteams.HostActivity
		activity, activityErr = observer.ExecutionActivity(ctx, target.execution)
		if activityErr == nil && productionTaskManagerExecutionAbandoned(activity, target.startedAt, c.now().UTC()) {
			return true, nil
		}
	}
	terminal, terminalErr := c.executionTerminal(ctx, target.execution)
	if terminalErr != nil {
		return false, errors.Join(terminalErr, activityErr)
	}
	if terminal {
		return true, nil
	}
	return false, activityErr
}

func (c *productionTaskManagerExecutionCleanup) executionTerminal(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, productionTaskManagerTerminalProbeTimeout)
	defer cancel()
	return c.terminator.ExecutionTerminal(probeCtx, execution)
}

func productionTaskManagerExecutionAbandoned(activity agentteams.HostActivity, startedAt, now time.Time) bool {
	return productionExecutionAbandoned(activity, startedAt, now, productionTaskManagerExecutionQuiescenceGap)
}

func productionExecutionAbandoned(activity agentteams.HostActivity, startedAt, now time.Time, gap time.Duration) bool {
	if startedAt.IsZero() || now.IsZero() || gap <= 0 || activity.RunningTaskCount != 0 {
		return false
	}
	startedAt = startedAt.UTC()
	cutoff := now.UTC().Add(-gap)
	if startedAt.After(cutoff) {
		return false
	}
	if !activity.LastFinishAt.IsZero() {
		finishedAt := activity.LastFinishAt.UTC()
		return !finishedAt.Before(startedAt) && !finishedAt.After(cutoff)
	}
	if !strings.EqualFold(strings.TrimSpace(activity.Status), "idle") {
		return false
	}
	// QwenPaw resets last_run_at/last_finish_at when its worker restarts. An
	// idle host with no running tasks for the full quiescence gap therefore
	// proves that a persisted dispatched execution no longer has a carrier.
	return activity.LastRunAt.IsZero() || !activity.LastRunAt.UTC().After(cutoff)
}

func (c *productionTaskManagerExecutionCleanup) failAbandonedInvocation(ctx context.Context, invocation productionCleanupInvocationRecord) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
UPDATE runtime_invocations
SET status = 'failed'
WHERE invocation_id = $1
  AND status = $2
  AND status IN ('prepared', 'running', 'waiting')`, invocation.ID, invocation.Status)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE production_manager_inputs
SET status = 'failed', updated_at = $2
WHERE invocation_id = $1
  AND status <> 'completed'`, invocation.ID, c.now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

var _ productionTaskManagerExecutionCleaner = (*productionTaskManagerExecutionCleanup)(nil)
