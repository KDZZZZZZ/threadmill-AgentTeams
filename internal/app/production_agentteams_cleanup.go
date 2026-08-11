package app

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const productionTaskManagerCleanupTimeout = 5 * time.Second

type productionAgentTeamsTerminator interface {
	Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error
}

type productionTaskManagerExecutionCleanup struct {
	db         *sql.DB
	projectID  kernel.ProjectID
	terminator productionAgentTeamsTerminator
}

func newProductionTaskManagerExecutionCleanup(db *sql.DB, projectID kernel.ProjectID, terminator productionAgentTeamsTerminator) (*productionTaskManagerExecutionCleanup, error) {
	if db == nil || kernel.IsZeroID(projectID) || terminator == nil {
		return nil, kernel.InvalidArgument("production Task Manager execution cleanup requires database, project, and terminator")
	}
	return &productionTaskManagerExecutionCleanup{db: db, projectID: projectID, terminator: terminator}, nil
}

func (c *productionTaskManagerExecutionCleanup) CleanupCompletedTaskManagerInvocations(ctx context.Context) error {
	if c == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), productionTaskManagerCleanupTimeout)
	defer cancel()
	executions, err := c.completedExecutions(cleanupCtx)
	if err != nil {
		return err
	}
	var cleanupErr error
	for _, execution := range executions {
		cleanupErr = errors.Join(cleanupErr, c.terminator.Terminate(cleanupCtx, execution, string(agentteams.TerminateReleaseWait)))
	}
	return cleanupErr
}

func (c *productionTaskManagerExecutionCleanup) completedExecutions(ctx context.Context) ([]agentteams.AgentTeamsExecutionRef, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT e.invocation_id, e.agentteams_task_id, e.host_ref
FROM agentteams_execution_refs e
JOIN runtime_invocations r ON r.invocation_id = e.invocation_id
WHERE r.project_id = $1
  AND r.role = $2
  AND r.status = 'completed'
  AND e.host_slot_claimed_at IS NOT NULL
  AND e.host_slot_released_at IS NULL
  AND (
    e.state IN ('reserved', 'dispatched')
    OR (e.state = 'terminated' AND e.termination_mode = 'release_wait')
  )
ORDER BY e.created_at, e.agentteams_task_id`, c.projectID, auth.RoleTaskManager)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var executions []agentteams.AgentTeamsExecutionRef
	for rows.Next() {
		var execution agentteams.AgentTeamsExecutionRef
		if err := rows.Scan(&execution.InvocationID, &execution.AgentTeamsTaskID, &execution.HostRef); err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}
	return executions, rows.Err()
}

var _ productionTaskManagerExecutionCleaner = (*productionTaskManagerExecutionCleanup)(nil)
