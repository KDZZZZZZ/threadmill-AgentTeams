package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextagent"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
)

const (
	productionContextInvocationTTL = 10 * time.Minute
	productionContextPollInterval  = 250 * time.Millisecond
	productionContextWaitTimeout   = 5 * time.Minute
	productionContextRetrieveLimit = 3
)

// productionContextRuntime is the Agent Runtime boundary for Context Agent
// retrieve and review invocations. It deliberately does not implement graph
// search or mutation itself: the real AgentTeams-hosted Context Agent calls
// the same MCP tools as specified in docs/context-agent.md. The runtime only
// persists the invocation, captures its structured tool result, and binds a
// retrieve subscription back to the original consumer.
type productionContextRuntime struct {
	db          *sql.DB
	projectID   kernel.ProjectID
	roomID      string
	assembler   *runtimepkg.Assembler
	contexts    *contextgraph.PostgresStore
	invocations runtimepkg.InvocationStore
	dispatcher  agentteams.AgentTeamsHostAdapter
	now         func() time.Time
	pollEvery   time.Duration
	waitTimeout time.Duration
}

func newProductionContextRuntime(db *sql.DB, projectID kernel.ProjectID, roomID string, assembler *runtimepkg.Assembler, contexts *contextgraph.PostgresStore, now func() time.Time) (*productionContextRuntime, error) {
	if db == nil || kernel.IsZeroID(projectID) || strings.TrimSpace(roomID) == "" || assembler == nil || contexts == nil {
		return nil, kernel.InvalidArgument("production Context Runtime requires database, project, room, assembler, and Context Store")
	}
	if now == nil {
		now = time.Now
	}
	return &productionContextRuntime{
		db: db, projectID: projectID, roomID: roomID, assembler: assembler, contexts: contexts,
		invocations: runtimepkg.NewPostgresInvocationStoreFromSQL(db), now: now,
		pollEvery: productionContextPollInterval, waitTimeout: productionContextWaitTimeout,
	}, nil
}

func (r *productionContextRuntime) setDispatcher(dispatcher agentteams.AgentTeamsHostAdapter) error {
	if dispatcher == nil {
		return kernel.InvalidArgument("production Context Runtime requires AgentTeams dispatcher")
	}
	r.dispatcher = dispatcher
	return nil
}

func (r *productionContextRuntime) LoadPreparedInvocation(ctx context.Context, invocationRef string) (agentteams.PreparedInvocation, error) {
	var prepared agentteams.PreparedInvocation
	var capabilities []byte
	err := r.db.QueryRowContext(ctx, `
SELECT c.invocation_id, c.project_id, i.role, i.operation, c.room_id, c.spec,
       c.runtime_config_ref, c.envelope_ref, c.required_capabilities
FROM production_context_invocations c
JOIN runtime_invocations i ON i.invocation_id=c.invocation_id
WHERE c.project_id=$1 AND c.invocation_id=$2`, r.projectID, invocationRef).Scan(
		&prepared.InvocationID, &prepared.ProjectID, &prepared.Role, &prepared.Operation,
		&prepared.RoomID, &prepared.Spec, &prepared.RuntimeConfigRef, &prepared.EnvelopeRef, &capabilities,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return agentteams.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "prepared Context Agent invocation not found", Recoverable: false}
	}
	if err != nil {
		return agentteams.PreparedInvocation{}, err
	}
	if err := json.Unmarshal(capabilities, &prepared.RequiredCapabilities); err != nil {
		return agentteams.PreparedInvocation{}, fmt.Errorf("decode Context Agent capabilities: %w", err)
	}
	return prepared, nil
}

func (r *productionContextRuntime) RetrieveForConsumer(ctx context.Context, caller auth.Principal, req contextagent.ContextRetrieveRequest) (contextagent.ContextRetrieveResult, error) {
	if err := contextagent.ValidateRetrieveRequest(req); err != nil {
		return contextagent.ContextRetrieveResult{}, err
	}
	if err := r.requireActiveConsumer(ctx, caller); err != nil {
		return contextagent.ContextRetrieveResult{}, err
	}
	boundary := struct {
		Operation string `json:"operation"`
		Query     string `json:"query"`
	}{Operation: "retrieve", Query: strings.TrimSpace(req.Query)}
	requestKey := stableProductionSuffix(caller.InvocationID, boundary.Query)
	invocationID, err := r.ensureInvocation(ctx, "retrieve", requestKey, caller.TaskID, caller, boundary)
	if err != nil {
		return contextagent.ContextRetrieveResult{}, err
	}
	if result, ok, err := r.retrieveResult(ctx, invocationID); err != nil {
		return contextagent.ContextRetrieveResult{}, err
	} else if ok {
		_ = r.completeInvocation(ctx, invocationID)
		return result, nil
	}
	if r.dispatcher == nil {
		return contextagent.ContextRetrieveResult{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent dispatcher is not configured", Recoverable: true}
	}
	if _, err := r.dispatch(ctx, invocationID); err != nil && !productionTerminalDeliveryWaitsForCapacity(err) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return contextagent.ContextRetrieveResult{}, errors.Join(err, r.abandonInvocation(cleanupCtx, invocationID, "context retrieve dispatch failed"))
	}
	waitCtx, cancel := context.WithTimeout(ctx, r.waitTimeout)
	defer cancel()
	ticker := time.NewTicker(r.pollEvery)
	defer ticker.Stop()
	for {
		result, ok, err := r.retrieveResult(waitCtx, invocationID)
		if err != nil {
			return contextagent.ContextRetrieveResult{}, err
		}
		if ok {
			if err := r.completeInvocation(waitCtx, invocationID); err != nil {
				return contextagent.ContextRetrieveResult{}, err
			}
			return result, nil
		}
		select {
		case <-waitCtx.Done():
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			timeoutErr := kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent retrieve did not submit a structured search result before its deadline", Recoverable: true}
			return contextagent.ContextRetrieveResult{}, errors.Join(timeoutErr, r.abandonInvocation(cleanupCtx, invocationID, "context_retrieve_timeout"))
		case <-ticker.C:
			// Capacity exhaustion is a waiting state for this synchronous tool call,
			// not a failed result. Keeping the caller blocked prevents it from
			// issuing a differently-worded retry while Reconcile later dispatches
			// this durable prepared invocation as a ghost execution.
			if _, err := r.dispatch(waitCtx, invocationID); err != nil && !productionTerminalDeliveryWaitsForCapacity(err) {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				return contextagent.ContextRetrieveResult{}, errors.Join(err, r.abandonInvocation(cleanupCtx, invocationID, "context retrieve dispatch retry failed"))
			}
		}
	}
}

// FinalizeTaskMemory freezes the authoritative task buffer, persists one real
// Context Agent review invocation, and schedules it. Candidate bodies are not
// returned to Task Manager: only the task identity crosses the Agent-facing
// seam. The background reconciler retries capacity-limited dispatches.
func (r *productionContextRuntime) FinalizeTaskMemory(ctx context.Context, principal auth.Principal, taskID kernel.TaskID) (contextgraph.FrozenCandidateBatch, error) {
	batch, err := r.contexts.FinalizeTaskMemory(ctx, principal, taskID)
	if err != nil {
		return contextgraph.FrozenCandidateBatch{}, err
	}
	boundary := struct {
		Operation string                            `json:"operation"`
		Batch     contextgraph.FrozenCandidateBatch `json:"frozen_candidate_batch"`
	}{Operation: "review", Batch: batch}
	runtimeCaller := auth.Principal{ProjectID: r.projectID, TaskID: kernel.TaskID(batch.TaskID), Role: auth.RoleTaskManager}
	requestKey := stableProductionSuffix(batch.TaskID, hashProductionJSON(batch))
	invocationID, err := r.ensureInvocation(ctx, "review", requestKey, kernel.TaskID(batch.TaskID), runtimeCaller, boundary)
	if err != nil {
		return contextgraph.FrozenCandidateBatch{}, err
	}
	if _, err := r.dispatch(ctx, invocationID); err != nil && !productionTerminalDeliveryWaitsForCapacity(err) {
		return contextgraph.FrozenCandidateBatch{}, err
	}
	return contextgraph.FrozenCandidateBatch{TaskID: batch.TaskID, Candidates: []contextgraph.TaskMemoryCandidateView{}}, nil
}

// CaptureSearch is called only by the Context Agent's context.search adapter.
// The result is still produced by Context Service; this method only saves the
// exact structured request/result so the blocked original caller can receive
// it without trusting free-form AgentTeams output.
func (r *productionContextRuntime) CaptureSearch(ctx context.Context, principal auth.Principal, req contextgraph.SearchRequest, result contextgraph.ContextSearchResult) error {
	if principal.ProjectID != r.projectID || principal.Role != auth.RoleContext || principal.Operation != "retrieve" || principal.InvocationID == "" {
		return kernel.Forbidden("search capture requires a production Context Agent retrieve invocation")
	}
	if len(req.Keywords) == 0 && len(req.Scope) == 0 && len(req.AnchorRefs) == 0 {
		return kernel.InvalidArgument("Context Agent retrieve must submit a bounded search request")
	}
	requestJSON, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	resultHash := hashProductionBytes(resultJSON)
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var operation, state string
	var consumerInvocation sql.NullString
	if err := tx.QueryRowContext(ctx, `
SELECT operation, state, consumer_invocation_id
FROM production_context_invocations
WHERE project_id=$1 AND invocation_id=$2
FOR UPDATE`, r.projectID, principal.InvocationID).Scan(&operation, &state, &consumerInvocation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return kernel.Forbidden("Context Agent invocation is not registered by Runtime")
		}
		return err
	}
	if operation != "retrieve" || !consumerInvocation.Valid || kernel.InvocationID(consumerInvocation.String) != principal.ConsumerInvocationID {
		return kernel.Forbidden("Context Agent retrieve consumer binding mismatch")
	}
	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT result_hash FROM production_context_retrieve_results WHERE project_id=$1 AND invocation_id=$2`, r.projectID, principal.InvocationID).Scan(&existingHash)
	if err == nil {
		if existingHash != resultHash {
			return kernel.IdempotencyConflict()
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if state != "prepared" && state != "dispatched" && state != "result_captured" {
		return kernel.TransitionRejected("Context Agent retrieve invocation no longer accepts a result")
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO production_context_retrieve_results(project_id, invocation_id, search_request, result, result_hash, created_at)
VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6)`, r.projectID, principal.InvocationID, string(requestJSON), string(resultJSON), resultHash, r.now().UTC()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_context_invocations SET state='result_captured', updated_at=$3 WHERE project_id=$1 AND invocation_id=$2`, r.projectID, principal.InvocationID, r.now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *productionContextRuntime) Reconcile(ctx context.Context) error {
	if r.dispatcher == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent dispatcher is not configured", Recoverable: true}
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT invocation_id, operation, state, COALESCE(task_id, '')
FROM production_context_invocations
WHERE project_id=$1 AND state IN ('prepared','dispatched','result_captured')
ORDER BY CASE WHEN operation='retrieve' THEN 0 ELSE 1 END, updated_at, invocation_id
LIMIT 32`, r.projectID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type pending struct {
		invocationID kernel.InvocationID
		operation    string
		state        string
		taskID       kernel.TaskID
	}
	var pendingInvocations []pending
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.invocationID, &item.operation, &item.state, &item.taskID); err != nil {
			return err
		}
		pendingInvocations = append(pendingInvocations, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var reconcileErr error
	for _, item := range pendingInvocations {
		switch {
		case item.operation == "retrieve" && (item.state == "prepared" || item.state == "dispatched"):
			active, err := r.retrieveConsumerActive(ctx, item.invocationID)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
				continue
			}
			if !active {
				reconcileErr = errors.Join(reconcileErr, r.abandonInvocation(ctx, item.invocationID, "context retrieve consumer is no longer active or invocation expired"))
				continue
			}
			if item.state != "prepared" {
				continue
			}
			if _, err := r.dispatch(ctx, item.invocationID); err != nil && !productionTerminalDeliveryWaitsForCapacity(err) {
				reconcileErr = errors.Join(reconcileErr, err)
			}
		case item.operation == "retrieve" && item.state == "result_captured":
			if err := r.completeInvocation(ctx, item.invocationID); err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
			}
		case item.operation == "review" && item.state == "prepared":
			active, err := r.contextInvocationActive(ctx, item.invocationID)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
			} else if !active {
				reconcileErr = errors.Join(reconcileErr, r.abandonInvocation(ctx, item.invocationID, "context review invocation expired before dispatch"))
			} else if _, err := r.dispatch(ctx, item.invocationID); err != nil && !productionTerminalDeliveryWaitsForCapacity(err) {
				reconcileErr = errors.Join(reconcileErr, err)
			}
		case item.operation == "review" && item.state == "dispatched":
			reviewed, err := r.taskMemoryReviewed(ctx, item.taskID)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
			} else if reviewed {
				reconcileErr = errors.Join(reconcileErr, r.completeInvocation(ctx, item.invocationID))
			} else if active, activeErr := r.contextInvocationActive(ctx, item.invocationID); activeErr != nil {
				reconcileErr = errors.Join(reconcileErr, activeErr)
			} else if !active {
				reconcileErr = errors.Join(reconcileErr, r.abandonInvocation(ctx, item.invocationID, "context review invocation expired before submitting a review"))
			}
		}
	}
	return reconcileErr
}

func (r *productionContextRuntime) contextInvocationActive(ctx context.Context, invocationID kernel.InvocationID) (bool, error) {
	var status string
	var expiresAt time.Time
	err := r.db.QueryRowContext(ctx, `
SELECT status, expires_at
FROM runtime_invocations
WHERE project_id=$1 AND invocation_id=$2`, r.projectID, invocationID).Scan(&status, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	active := status == string(runtimepkg.InvocationPrepared) || status == string(runtimepkg.InvocationRunning) || status == string(runtimepkg.InvocationWaiting)
	return active && expiresAt.After(r.now()), nil
}

func (r *productionContextRuntime) retrieveConsumerActive(ctx context.Context, invocationID kernel.InvocationID) (bool, error) {
	var consumerStatus, invocationStatus string
	var consumerExpiresAt, invocationExpiresAt time.Time
	err := r.db.QueryRowContext(ctx, `
SELECT consumer.status, consumer.expires_at, current_invocation.status, current_invocation.expires_at
FROM production_context_invocations c
JOIN runtime_invocations consumer ON consumer.invocation_id=c.consumer_invocation_id
JOIN runtime_invocations current_invocation ON current_invocation.invocation_id=c.invocation_id
WHERE c.project_id=$1 AND c.invocation_id=$2 AND c.operation='retrieve'`, r.projectID, invocationID).Scan(
		&consumerStatus, &consumerExpiresAt, &invocationStatus, &invocationExpiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	now := r.now()
	consumerActive := consumerStatus == string(runtimepkg.InvocationRunning) || consumerStatus == string(runtimepkg.InvocationWaiting)
	invocationActive := invocationStatus == string(runtimepkg.InvocationPrepared) || invocationStatus == string(runtimepkg.InvocationRunning) || invocationStatus == string(runtimepkg.InvocationWaiting)
	return consumerActive && invocationActive && consumerExpiresAt.After(now) && invocationExpiresAt.After(now), nil
}

func (r *productionContextRuntime) abandonInvocation(ctx context.Context, invocationID kernel.InvocationID, reason string) error {
	var execution agentteams.AgentTeamsExecutionRef
	err := r.db.QueryRowContext(ctx, `
SELECT invocation_id, agentteams_task_id, host_ref
FROM agentteams_execution_refs
WHERE invocation_ref=$1 AND state IN ('reserved','dispatched')
ORDER BY attempt DESC
LIMIT 1`, invocationID).Scan(&execution.InvocationID, &execution.AgentTeamsTaskID, &execution.HostRef)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if execution.InvocationID != invocationID {
			return kernel.IdempotencyConflict()
		}
		if r.dispatcher == nil {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent dispatcher is not configured", Recoverable: true}
		}
		if err := r.dispatcher.Terminate(ctx, execution, string(agentteams.TerminateCancel)); err != nil {
			return err
		}
	}
	return r.failInvocation(ctx, invocationID, reason)
}

func (r *productionContextRuntime) requireActiveConsumer(ctx context.Context, caller auth.Principal) error {
	if caller.ProjectID != r.projectID || caller.InvocationID == "" || (caller.Role != auth.RoleTaskManager && !caller.Role.IsPhase()) {
		return kernel.Forbidden("context retrieve requires an active Task Manager or Phase consumer")
	}
	invocation, ok, err := r.invocations.Get(ctx, caller.InvocationID)
	if err != nil {
		return err
	}
	if !ok || invocation.ProjectID != caller.ProjectID || invocation.TaskID != caller.TaskID || invocation.Role != caller.Role {
		return kernel.Forbidden("context retrieve caller does not match its persisted invocation")
	}
	if invocation.Status != runtimepkg.InvocationRunning && invocation.Status != runtimepkg.InvocationWaiting {
		return kernel.TransitionRejected("context retrieve consumer invocation is not active")
	}
	if !invocation.ExpiresAt.After(r.now()) {
		return kernel.TransitionRejected("context retrieve consumer invocation expired")
	}
	return nil
}

func (r *productionContextRuntime) ensureInvocation(ctx context.Context, operation, requestKey string, taskID kernel.TaskID, caller auth.Principal, boundary any) (kernel.InvocationID, error) {
	boundaryJSON, err := json.Marshal(boundary)
	if err != nil {
		return "", err
	}
	requestHash := hashProductionBytes(boundaryJSON)
	invocationID := kernel.InvocationID("context-invocation:" + stableProductionSuffix(r.projectID, operation, requestKey))
	now := r.now().UTC()
	invocation := runtimepkg.Invocation{
		ID: invocationID, ActorPrincipalID: kernel.ActorPrincipalID("context-agent:" + string(r.projectID)),
		ProjectID: r.projectID, TaskID: taskID, Role: auth.RoleContext, Operation: operation,
		Status: runtimepkg.InvocationPrepared, CreatedAt: now, ExpiresAt: now.Add(productionContextInvocationTTL),
	}
	if operation == "retrieve" {
		invocation.ConsumerInvocationID = caller.InvocationID
		invocation.ConsumerTaskID = caller.TaskID
		invocation.ConsumerRole = caller.Role
	}
	assembly, err := r.assembler.Assemble(invocation, promptcatalog.RenderData{BoundaryInput: string(boundaryJSON)})
	if err != nil {
		return "", err
	}
	envelope, err := runtimepkg.EnvelopeFromInvocation(assembly.Invocation).JSON()
	if err != nil {
		return "", err
	}
	assembly, err = r.assembler.Assemble(assembly.Invocation, promptcatalog.RenderData{RuntimeEnvelope: envelope, BoundaryInput: string(boundaryJSON)})
	if err != nil {
		return "", err
	}
	prepared := agentteams.PreparedInvocation{
		InvocationID: invocationID, ProjectID: r.projectID, Role: auth.RoleContext, Operation: operation,
		RoomID: r.roomID, Spec: assembly.Prompt.Text,
		RuntimeConfigRef: "runtime-config:" + string(invocationID), EnvelopeRef: "runtime-envelope:" + string(invocationID),
		RequiredCapabilities: []string{agentteams.CapabilityContextAgent},
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if operation == "retrieve" {
		// A Phase/Task Manager invocation has a deliberately small research
		// budget. Serialize all unique retrieve requests for the consumer so
		// concurrent tool calls cannot race past the limit. The request-specific
		// lookup below still makes an identical query an idempotent retry.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(r.projectID)+"/context/retrieve-consumer/"+string(caller.InvocationID)); err != nil {
			return "", err
		}
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(r.projectID)+"/context/"+operation+"/"+requestKey); err != nil {
		return "", err
	}
	var existingID kernel.InvocationID
	var existingHash string
	err = tx.QueryRowContext(ctx, `
SELECT invocation_id, request_hash
FROM production_context_invocations
WHERE project_id=$1 AND operation=$2 AND request_key=$3`, r.projectID, operation, requestKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != requestHash || existingID != invocationID {
			return "", kernel.IdempotencyConflict()
		}
		return existingID, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if operation == "retrieve" {
		var retrieveCount int
		if err := tx.QueryRowContext(ctx, `
SELECT count(*)
FROM production_context_invocations
WHERE project_id=$1 AND operation='retrieve' AND consumer_invocation_id=$2`, r.projectID, caller.InvocationID).Scan(&retrieveCount); err != nil {
			return "", err
		}
		if retrieveCount >= productionContextRetrieveLimit {
			return "", kernel.InvalidArgument("Context Agent retrieve budget exhausted for this consumer invocation")
		}
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(tx).Create(ctx, assembly.Invocation); err != nil {
		return "", err
	}
	capabilities, _ := json.Marshal(prepared.RequiredCapabilities)
	var taskValue, consumerValue any
	if taskID != "" {
		taskValue = taskID
	}
	if invocation.ConsumerInvocationID != "" {
		consumerValue = invocation.ConsumerInvocationID
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO production_context_invocations(
  project_id, invocation_id, operation, request_key, request_hash, task_id, consumer_invocation_id,
  room_id, spec, runtime_config_ref, envelope_ref, required_capabilities, state, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,'prepared',$13,$13)`,
		r.projectID, invocationID, operation, requestKey, requestHash, taskValue, consumerValue,
		prepared.RoomID, prepared.Spec, prepared.RuntimeConfigRef, prepared.EnvelopeRef, string(capabilities), now); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return invocationID, nil
}

func (r *productionContextRuntime) dispatch(ctx context.Context, invocationID kernel.InvocationID) (agentteams.AgentTeamsExecutionRef, error) {
	if r.dispatcher == nil {
		return agentteams.AgentTeamsExecutionRef{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent dispatcher is not configured", Recoverable: true}
	}
	execution, err := r.dispatcher.Dispatch(ctx, string(invocationID))
	if err != nil {
		return agentteams.AgentTeamsExecutionRef{}, err
	}
	now := r.now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return agentteams.AgentTeamsExecutionRef{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='running' WHERE invocation_id=$1 AND status='prepared'`, invocationID); err != nil {
		return agentteams.AgentTeamsExecutionRef{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE production_context_invocations
SET state=CASE WHEN state='prepared' THEN 'dispatched' ELSE state END,
    agentteams_task_id=$3, host_ref=$4, updated_at=$5
WHERE project_id=$1 AND invocation_id=$2 AND state IN ('prepared','dispatched','result_captured')`,
		r.projectID, invocationID, execution.AgentTeamsTaskID, execution.HostRef, now); err != nil {
		return agentteams.AgentTeamsExecutionRef{}, err
	}
	if err := tx.Commit(); err != nil {
		return agentteams.AgentTeamsExecutionRef{}, err
	}
	return execution, nil
}

func (r *productionContextRuntime) retrieveResult(ctx context.Context, invocationID kernel.InvocationID) (contextagent.ContextRetrieveResult, bool, error) {
	var requestJSON, resultJSON []byte
	err := r.db.QueryRowContext(ctx, `
SELECT search_request, result
FROM production_context_retrieve_results
WHERE project_id=$1 AND invocation_id=$2`, r.projectID, invocationID).Scan(&requestJSON, &resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return contextagent.ContextRetrieveResult{}, false, nil
	}
	if err != nil {
		return contextagent.ContextRetrieveResult{}, false, err
	}
	var request contextgraph.SearchRequest
	var result contextgraph.ContextSearchResult
	if err := json.Unmarshal(requestJSON, &request); err != nil {
		return contextagent.ContextRetrieveResult{}, false, err
	}
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		return contextagent.ContextRetrieveResult{}, false, err
	}
	return contextagent.ContextRetrieveResult{
		Slice: result.Slice, SubscriptionIDs: append([]string(nil), result.SubscriptionIDs...),
		Explanation: explainProductionContextSearch(request),
	}, true, nil
}

func explainProductionContextSearch(req contextgraph.SearchRequest) string {
	parts := []string{"Context Agent submitted bounded context.search"}
	if len(req.Keywords) > 0 {
		parts = append(parts, "keywords="+strings.Join(req.Keywords, ","))
	}
	if len(req.Scope) > 0 {
		parts = append(parts, "scope="+strings.Join(req.Scope, ","))
	}
	if len(req.AnchorRefs) > 0 {
		parts = append(parts, "anchors="+strings.Join(req.AnchorRefs, ","))
	}
	return strings.Join(parts, "; ")
}

func (r *productionContextRuntime) completeInvocation(ctx context.Context, invocationID kernel.InvocationID) error {
	if r.dispatcher == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent dispatcher is not configured", Recoverable: true}
	}
	var execution agentteams.AgentTeamsExecutionRef
	execution.InvocationID = invocationID
	if err := r.db.QueryRowContext(ctx, `
SELECT agentteams_task_id, host_ref
FROM production_context_invocations
WHERE project_id=$1 AND invocation_id=$2`, r.projectID, invocationID).Scan(&execution.AgentTeamsTaskID, &execution.HostRef); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return kernel.Error{Code: kernel.CodeNotFound, Message: "Context Agent invocation is not registered", Recoverable: false}
		}
		return err
	}
	if strings.TrimSpace(execution.AgentTeamsTaskID) == "" || strings.TrimSpace(execution.HostRef) == "" {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "Context Agent execution reference is not available", Recoverable: true}
	}
	if err := r.dispatcher.Terminate(ctx, execution, string(agentteams.TerminateCancel)); err != nil {
		return err
	}
	now := r.now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='completed' WHERE invocation_id=$1 AND status IN ('prepared','running','waiting')`, invocationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_context_invocations SET state='completed', updated_at=$3 WHERE project_id=$1 AND invocation_id=$2 AND state <> 'failed'`, r.projectID, invocationID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *productionContextRuntime) failInvocation(ctx context.Context, invocationID kernel.InvocationID, reason string) error {
	now := r.now().UTC()
	_, err := r.db.ExecContext(ctx, `
UPDATE production_context_invocations SET state='failed', last_error=$3, updated_at=$4
WHERE project_id=$1 AND invocation_id=$2 AND state <> 'completed'`, r.projectID, invocationID, reason, now)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1 AND status IN ('prepared','running','waiting')`, invocationID)
	return err
}

func (r *productionContextRuntime) taskMemoryReviewed(ctx context.Context, taskID kernel.TaskID) (bool, error) {
	var state string
	err := r.db.QueryRowContext(ctx, `SELECT state FROM context_task_memory_reviews WHERE project_id=$1 AND task_id=$2`, r.projectID, taskID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return state == string(contextgraph.TaskMemoryReviewed), err
}

func hashProductionJSON(value any) string {
	raw, _ := json.Marshal(value)
	return hashProductionBytes(raw)
}

type productionContextSearcher struct {
	searcher contextgraph.ContextGraphSearcher
	runtime  *productionContextRuntime
}

func (s productionContextSearcher) Search(ctx context.Context, principal auth.Principal, req contextgraph.SearchRequest) (contextgraph.ContextSearchResult, error) {
	result, err := s.searcher.Search(ctx, principal, req)
	if err != nil {
		return contextgraph.ContextSearchResult{}, err
	}
	if principal.Role == auth.RoleContext && principal.Operation == "retrieve" {
		if err := s.runtime.CaptureSearch(ctx, principal, req, result); err != nil {
			return contextgraph.ContextSearchResult{}, err
		}
	}
	return result, nil
}

var _ agentteams.InvocationSource = (*productionContextRuntime)(nil)
var _ contextgraph.TaskMemoryFinalizer = (*productionContextRuntime)(nil)
var _ contextgraph.ContextGraphSearcher = productionContextSearcher{}
