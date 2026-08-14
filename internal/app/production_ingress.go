package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/promptcatalog"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
)

// A Task Manager invocation is a bounded control-plane computation, not a
// durable conversation session. Conversation continuity is persisted in the
// input/decision log; the execution token and AgentTeams slot are short lived.
const taskManagerInvocationTTL = 30 * time.Minute
const productionTaskManagerMaxAttempts = 3

type productionInvocationDispatcher interface {
	Dispatch(context.Context, string) (agentteams.AgentTeamsExecutionRef, error)
}

type productionTaskManagerExecutionCleaner interface {
	CleanupTaskManagerInvocations(context.Context) error
}

type productionPersistedDecisionRecoverer interface {
	RecoverPersistedTaskManagerDecision(context.Context, kernel.InvocationID) (bool, error)
}

// productionIngress is the only production HTTP write boundary for manager
// input. It commits the immutable input and its Task Manager invocation before
// asking AgentTeams to perform any external work.
type productionIngress struct {
	db         *sql.DB
	projectID  kernel.ProjectID
	roomID     string
	graph      *coordination.PostgresStore
	assembler  *runtimepkg.Assembler
	dispatcher productionInvocationDispatcher
	cleaner    productionTaskManagerExecutionCleaner
	recoverer  productionPersistedDecisionRecoverer
	now        func() time.Time
}

type productionInput struct {
	Kind             string
	RequestID        string
	ConversationID   string
	Body             string
	Payload          []byte
	SeenRevision     kernel.Revision
	SelectedEndpoint *coordination.PhaseEndpointRef
	TargetKind       string
	TargetRef        string
}

type persistedProductionInput struct {
	InputRef       string
	InvocationID   kernel.InvocationID
	ConversationID string
	Status         string
}

func newProductionIngress(db *sql.DB, projectID kernel.ProjectID, roomID string, assembler *runtimepkg.Assembler, graph *coordination.PostgresStore, now func() time.Time) (*productionIngress, error) {
	if db == nil || assembler == nil || graph == nil || strings.TrimSpace(roomID) == "" || kernel.IsZeroID(projectID) {
		return nil, kernel.InvalidArgument("production ingress database, project, room, assembler, and graph are required")
	}
	if now == nil {
		now = time.Now
	}
	return &productionIngress{db: db, projectID: projectID, roomID: roomID, graph: graph, assembler: assembler, now: now}, nil
}

func (p *productionIngress) setDispatcher(dispatcher productionInvocationDispatcher) error {
	if dispatcher == nil {
		return kernel.InvalidArgument("production ingress dispatcher is required")
	}
	p.dispatcher = dispatcher
	return nil
}

func (p *productionIngress) setTaskManagerExecutionCleaner(cleaner productionTaskManagerExecutionCleaner) error {
	if cleaner == nil {
		return kernel.InvalidArgument("production Task Manager execution cleaner is required")
	}
	p.cleaner = cleaner
	return nil
}

func (p *productionIngress) setPersistedDecisionRecoverer(recoverer productionPersistedDecisionRecoverer) error {
	if recoverer == nil {
		return kernel.InvalidArgument("production Task Manager decision recoverer is required")
	}
	p.recoverer = recoverer
	return nil
}

func (p *productionIngress) SubmitRequirement(ctx context.Context, principal auth.Principal, req httpapi.RequirementCreateRequest) (httpapi.RequirementCreateResponse, error) {
	if err := p.requireProject(principal, req.ProjectID); err != nil {
		return httpapi.RequirementCreateResponse{}, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return httpapi.RequirementCreateResponse{}, kernel.InvalidArgument("requirement input cannot be encoded")
	}
	conversationID := strings.TrimSpace(req.ConversationID)
	if conversationID == "" {
		conversationID = "requirement:" + req.RequestID
	}
	stored, err := p.persistAndDispatch(ctx, productionInput{Kind: "requirement", RequestID: req.RequestID, ConversationID: conversationID, Body: req.Body, Payload: payload})
	if err != nil && !productionInputDurablyQueued(stored, err) {
		return httpapi.RequirementCreateResponse{}, err
	}
	return httpapi.RequirementCreateResponse{RequirementID: "requirement:" + stableProductionSuffix(p.projectID, "requirement", req.RequestID), ManagerInputRef: stored.InputRef, InvocationRef: stored.InvocationID, ConversationID: stored.ConversationID, Status: "accepted"}, nil
}

func (p *productionIngress) SubmitManagerMessage(ctx context.Context, principal auth.Principal, req httpapi.ManagerMessageRequest) (httpapi.ManagerMessageResponse, error) {
	if err := p.requireProject(principal, req.ProjectID); err != nil {
		return httpapi.ManagerMessageResponse{}, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return httpapi.ManagerMessageResponse{}, kernel.InvalidArgument("manager input cannot be encoded")
	}
	input := productionInput{Kind: "manager", RequestID: req.RequestID, ConversationID: req.ConversationID, Body: req.Body, Payload: payload, SelectedEndpoint: req.SelectedEndpoint}
	if req.ObservedGraphRevision != nil {
		input.SeenRevision = *req.ObservedGraphRevision
	}
	stored, err := p.persistAndDispatch(ctx, input)
	if err != nil && !productionInputDurablyQueued(stored, err) {
		return httpapi.ManagerMessageResponse{}, err
	}
	return httpapi.ManagerMessageResponse{ManagerInputRef: stored.InputRef, InvocationRef: stored.InvocationID, ConversationID: stored.ConversationID, Status: "accepted"}, nil
}

func (p *productionIngress) SubmitHumanDecision(ctx context.Context, principal auth.Principal, req httpapi.HumanDecisionRequest) (httpapi.HumanDecisionResponse, error) {
	if err := p.requireProject(principal, req.ProjectID); err != nil {
		return httpapi.HumanDecisionResponse{}, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return httpapi.HumanDecisionResponse{}, kernel.InvalidArgument("human decision cannot be encoded")
	}
	input := productionInput{Kind: "human", RequestID: req.RequestID, ConversationID: "human:" + req.Target.Kind + ":" + req.Target.Ref, Body: req.Decision + ": " + req.Reason, Payload: payload, TargetKind: req.Target.Kind, TargetRef: req.Target.Ref}
	if req.ExpectedGraphRevision != nil {
		input.SeenRevision = *req.ExpectedGraphRevision
	}
	stored, err := p.persistAndDispatch(ctx, input)
	if err != nil && !productionInputDurablyQueued(stored, err) {
		return httpapi.HumanDecisionResponse{}, err
	}
	return httpapi.HumanDecisionResponse{HumanDecisionRef: "human-decision:" + stableProductionSuffix(p.projectID, "human", req.RequestID), ManagerInputRef: stored.InputRef, InvocationRef: stored.InvocationID, Status: "accepted"}, nil
}

// A control-plane input is accepted once its immutable input, invocation, and
// conversation entry are committed. Manager host capacity only determines
// when that durable input starts running; the Runtime retry loop owns that
// transition. Returning 503 after persistence makes clients retry a write that
// already succeeded and contradicts the pending status shown by the GUI.
func productionInputDurablyQueued(stored persistedProductionInput, err error) bool {
	return stored.InputRef != "" && stored.InvocationID != "" && productionTerminalDeliveryWaitsForCapacity(err)
}

// SubmitRequirement is the Agent-facing requirement port. Its request ID is
// derived from the authenticated invocation and normalized payload; the Agent
// cannot choose another invocation's idempotency namespace.
func (p *productionIngress) SubmitRequirementFromAgent(ctx context.Context, principal auth.Principal, requirement taskmanager.Requirement) (any, error) {
	payload, err := json.Marshal(requirement)
	if err != nil {
		return nil, kernel.InvalidArgument("agent requirement cannot be encoded")
	}
	requestID := stableProductionSuffix(principal.InvocationID, string(payload))
	return p.SubmitRequirement(ctx, principal, httpapi.RequirementCreateRequest{
		RequestID: requestID, ProjectID: principal.ProjectID,
		ConversationID: "agent:" + string(principal.InvocationID), Body: requirement.Text,
		Motivation: requirement.Goal, Constraints: requirement.Constraints,
	})
}

type productionRequirementSubmitter struct{ ingress *productionIngress }

func (s productionRequirementSubmitter) SubmitRequirement(ctx context.Context, principal auth.Principal, requirement taskmanager.Requirement) (any, error) {
	return s.ingress.SubmitRequirementFromAgent(ctx, principal, requirement)
}

func (p *productionIngress) Conversation(ctx context.Context, principal auth.Principal, conversationID, after string) (httpapi.ManagerConversation, error) {
	if err := p.requireProject(principal, principal.ProjectID); err != nil {
		return httpapi.ManagerConversation{}, err
	}
	if strings.TrimSpace(conversationID) == "" {
		return httpapi.ManagerConversation{}, kernel.InvalidArgument("conversation_id is required")
	}
	cursor := int64(0)
	if after != "" {
		parsed, err := strconv.ParseInt(after, 10, 64)
		if err != nil || parsed < 0 {
			return httpapi.ManagerConversation{}, kernel.InvalidArgument("conversation cursor is invalid")
		}
		cursor = parsed
	}
	rows, err := p.db.QueryContext(ctx, `SELECT sequence, entry_id, entry_kind, created_at, COALESCE(manager_input_ref, ''), COALESCE(decision_ref, ''), COALESCE(graph_revision, 0), COALESCE(body, ''), COALESCE(disposition, '')
FROM production_conversation_entries
WHERE project_id = $1 AND conversation_id = $2 AND sequence > $3
ORDER BY sequence LIMIT 200`, p.projectID, conversationID, cursor)
	if err != nil {
		return httpapi.ManagerConversation{}, err
	}
	defer rows.Close()
	conversation := httpapi.ManagerConversation{ConversationID: conversationID, ProjectID: p.projectID, Messages: []httpapi.ManagerConversationEntry{}, Cursor: strconv.FormatInt(cursor, 10)}
	for rows.Next() {
		var sequence int64
		var entry httpapi.ManagerConversationEntry
		if err := rows.Scan(&sequence, &entry.EntryID, &entry.Kind, &entry.CreatedAt, &entry.ManagerInputRef, &entry.DecisionRef, &entry.GraphRevision, &entry.Body, &entry.Disposition); err != nil {
			return httpapi.ManagerConversation{}, err
		}
		conversation.Messages = append(conversation.Messages, entry)
		conversation.Cursor = strconv.FormatInt(sequence, 10)
	}
	return conversation, rows.Err()
}

func (p *productionIngress) persistAndDispatch(ctx context.Context, input productionInput) (persistedProductionInput, error) {
	if p.dispatcher == nil {
		return persistedProductionInput{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams dispatcher is not configured", Recoverable: true}
	}
	if input.SeenRevision <= 0 {
		snapshot, err := p.graph.Latest(ctx, p.projectID)
		if err != nil {
			return persistedProductionInput{}, err
		}
		input.SeenRevision = snapshot.Revision
	}
	prepared, invocation, payloadHash, err := p.prepare(input)
	if err != nil {
		return persistedProductionInput{}, err
	}
	stored, alreadyDispatched, err := p.persist(ctx, input, payloadHash, prepared, invocation)
	if err != nil {
		return persistedProductionInput{}, err
	}
	completed, err := p.completeAlreadyAppliedPhaseEvaluation(ctx, stored.InputRef, stored.InvocationID)
	if err != nil {
		return persistedProductionInput{}, err
	}
	if completed {
		stored.Status = "completed"
		return stored, nil
	}
	if alreadyDispatched {
		if stored.Status == "failed" {
			return persistedProductionInput{}, kernel.Error{
				Code:        kernel.CodeExecutorUnavailable,
				Message:     "the bounded Task Manager invocation failed; retry with a new request_id",
				Recoverable: true,
			}
		}
		return stored, nil
	}
	if p.cleaner != nil {
		if err := p.cleaner.CleanupTaskManagerInvocations(ctx); err != nil {
			return persistedProductionInput{}, err
		}
	}
	if _, err := p.dispatcher.Dispatch(ctx, string(stored.InvocationID)); err != nil {
		// The input and invocation are already durable. Returning their pending
		// identity lets an internal caller distinguish a capacity wait (which the
		// reconcile loop can retry) from a failure that happened before persistence.
		return stored, err
	}
	if err := p.markDispatched(ctx, stored.InvocationID); err != nil {
		return persistedProductionInput{}, err
	}
	stored.Status = "dispatched"
	return stored, nil
}

// RetryFailedTaskManagerInputs preserves the logical inputRef while replacing
// only the bounded Invocation and AgentTeams execution. This is the recovery
// path for provider-terminal or quiescent executions that never submitted a
// decision. It is intentionally internal to Runtime; callers cannot request an
// attempt number or forge a replacement Invocation.
func (p *productionIngress) RetryFailedTaskManagerInputs(ctx context.Context) error {
	if p == nil || p.dispatcher == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams dispatcher is not configured", Recoverable: true}
	}
	rows, err := p.db.QueryContext(ctx, `
SELECT i.input_ref, i.invocation_id, i.status, i.dispatch_attempt, (r.expires_at <= $3)
FROM production_manager_inputs i
JOIN runtime_invocations r ON r.invocation_id=i.invocation_id
WHERE i.project_id=$1 AND i.status IN ('pending','failed') AND i.dispatch_attempt <= $2
ORDER BY i.updated_at, i.input_ref
LIMIT 32`, p.projectID, productionTaskManagerMaxAttempts, p.now().UTC())
	if err != nil {
		return err
	}
	defer rows.Close()
	type target struct {
		inputRef   string
		invocation kernel.InvocationID
		status     string
		attempt    int
		expired    bool
	}
	var targets []target
	for rows.Next() {
		var item target
		if err := rows.Scan(&item.inputRef, &item.invocation, &item.status, &item.attempt, &item.expired); err != nil {
			return err
		}
		targets = append(targets, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var retryErr error
	for _, item := range targets {
		completed, err := p.completeAlreadyAppliedPhaseEvaluation(ctx, item.inputRef, item.invocation)
		if err != nil {
			retryErr = errors.Join(retryErr, err)
			continue
		}
		if completed {
			continue
		}
		invocationID := item.invocation
		if item.status == "pending" && item.expired {
			failed, err := p.failExpiredPendingTaskManagerInvocation(ctx, item.inputRef, item.invocation)
			if err != nil {
				retryErr = errors.Join(retryErr, err)
				continue
			}
			if !failed {
				continue
			}
			item.status = "failed"
		}
		if item.status == "failed" {
			if p.recoverer != nil {
				recovered, err := p.recoverer.RecoverPersistedTaskManagerDecision(ctx, item.invocation)
				if recovered {
					// A revision conflict means the decision was made against an old
					// graph. A transition rejection means the immutable decision was
					// never applicable to the trusted target state (for example a
					// targeted-verify proposal chose reopen_round while verify is still
					// pending). Replaying either decision cannot make progress. Preserve
					// it as audit evidence and give a fresh Task Manager invocation the
					// same trusted boundary at the current graph revision.
					if kernel.IsCode(err, kernel.CodeRevisionConflict) || kernel.IsCode(err, kernel.CodeTransitionRejected) {
						err = p.rebaseFailedTaskManagerInput(ctx, item.inputRef)
					}
					retryErr = errors.Join(retryErr, err)
					continue
				}
				if err != nil {
					retryErr = errors.Join(retryErr, err)
					continue
				}
			}
			if item.attempt >= productionTaskManagerMaxAttempts {
				continue
			}
			rotated, ok, err := p.rotateFailedTaskManagerInvocation(ctx, item.inputRef, item.invocation, item.attempt)
			if err != nil {
				retryErr = errors.Join(retryErr, err)
				continue
			}
			if !ok {
				continue
			}
			invocationID = rotated
		}
		if _, err := p.dispatcher.Dispatch(ctx, string(invocationID)); err != nil {
			if productionTerminalDeliveryWaitsForCapacity(err) {
				continue
			}
			retryErr = errors.Join(retryErr, err)
			continue
		}
		if err := p.markDispatched(ctx, invocationID); err != nil {
			retryErr = errors.Join(retryErr, err)
		}
	}
	return retryErr
}

// completeAlreadyAppliedPhaseEvaluation closes an idempotency gap between the
// durable phase-evaluation input and the Coordination Graph. A previous Task
// Manager attempt may have committed the exact terminal result and then lost
// its completion acknowledgement. In that case another model call cannot add
// authority or information: it can only repeat a transition that the graph
// correctly rejects because the endpoint is no longer submitted.
//
// The match is intentionally exact and narrow. Runtime requires the persisted
// endpoint, generation, binding, output, terminal state, and terminal verdict
// to agree in one serializable transaction. A different result or a reopened
// generation remains pending for the Task Manager to judge.
func (p *productionIngress) completeAlreadyAppliedPhaseEvaluation(ctx context.Context, inputRef string, invocationID kernel.InvocationID) (bool, error) {
	if p == nil || p.db == nil || inputRef == "" || invocationID == "" {
		return false, nil
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(p.projectID)+"/"+inputRef); err != nil {
		return false, err
	}
	var inputKind, targetKind, targetRef, status, payloadText string
	var selectedTaskID, selectedEndpointID sql.NullString
	err = tx.QueryRowContext(ctx, `
SELECT input_kind, COALESCE(target_kind,''), COALESCE(target_ref,''), status,
       selected_task_id, selected_endpoint_id, payload::text
FROM production_manager_inputs
WHERE project_id=$1 AND input_ref=$2 AND invocation_id=$3
FOR UPDATE`, p.projectID, inputRef, invocationID).Scan(
		&inputKind, &targetKind, &targetRef, &status,
		&selectedTaskID, &selectedEndpointID, &payloadText,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if inputKind != "phase_orchestration" || targetKind != "phase_evaluation" ||
		(status != "pending" && status != "failed") {
		return false, tx.Commit()
	}
	var evaluation productionPhaseEvaluationBoundary
	if err := json.Unmarshal([]byte(payloadText), &evaluation); err != nil {
		return false, trustedBoundaryError("persisted phase_evaluation payload is invalid")
	}
	if !selectedTaskID.Valid || !selectedEndpointID.Valid || targetRef == "" ||
		evaluation.Endpoint.TaskID != kernel.TaskID(selectedTaskID.String) ||
		evaluation.Endpoint.EndpointID != coordination.EndpointID(selectedEndpointID.String) ||
		evaluation.Output.OutputRef != targetRef || evaluation.Output.Receipt.Endpoint != evaluation.Endpoint ||
		evaluation.Generation <= 0 || evaluation.BindingRef == "" ||
		evaluation.Output.Receipt.Generation != evaluation.Generation ||
		evaluation.Output.Receipt.BindingRef != evaluation.BindingRef {
		return false, trustedBoundaryError("persisted phase_evaluation identity is inconsistent")
	}
	var applied bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM coordination_endpoints e
  JOIN coordination_phase_results r
    ON r.project_id=e.project_id
   AND r.task_id=e.task_id
   AND r.endpoint_id=e.endpoint_id
   AND r.binding_ref=e.binding_ref
  WHERE e.project_id=$1 AND e.task_id=$2 AND e.endpoint_id=$3
    AND e.generation=$4 AND e.binding_ref=$5
    AND r.output_ref=$6
    AND ((e.state='satisfied' AND r.verdict='satisfied')
      OR (e.state='rejected' AND r.verdict='rejected'))
)`, p.projectID, evaluation.Endpoint.TaskID, evaluation.Endpoint.EndpointID,
		evaluation.Generation, evaluation.BindingRef, evaluation.Output.OutputRef).Scan(&applied); err != nil {
		return false, err
	}
	if !applied {
		return false, tx.Commit()
	}
	now := p.now().UTC()
	result, err := tx.ExecContext(ctx, `
UPDATE production_manager_inputs
SET status='completed', updated_at=$4
WHERE project_id=$1 AND input_ref=$2 AND invocation_id=$3 AND status IN ('pending','failed')`,
		p.projectID, inputRef, invocationID, now)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, err
	} else if affected != 1 {
		return false, tx.Commit()
	}
	// A failed attempt remains failed as execution history. Only a bounded
	// invocation that never reached a terminal state is closed as completed.
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_invocations
SET status='completed'
WHERE invocation_id=$1 AND status IN ('prepared','running','waiting')`, invocationID); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE production_taskmanager_bindings
SET completed_at=COALESCE(completed_at,$2)
WHERE invocation_id=$1`, invocationID, now); err != nil {
		return false, err
	}
	entryID := "runtime:phase-evaluation-already-applied:" + stableProductionSuffix(p.projectID, inputRef, invocationID, targetRef)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO production_conversation_entries(
  project_id, conversation_id, entry_id, entry_kind, manager_input_ref,
  graph_revision, body, disposition, created_at
)
SELECT i.project_id, i.conversation_id, $1, 'runtime', i.input_ref,
       g.revision, $2, 'already_applied', $3
FROM production_manager_inputs i
CROSS JOIN LATERAL (
  SELECT revision FROM coordination_graph_revisions
  WHERE project_id=i.project_id ORDER BY revision DESC LIMIT 1
) g
WHERE i.project_id=$4 AND i.input_ref=$5
ON CONFLICT (project_id, conversation_id, entry_id) DO NOTHING`,
		entryID, "phase evaluation already reflected by exact graph result "+targetRef, now, p.projectID, inputRef); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// failExpiredPendingTaskManagerInvocation closes a durable input attempt that
// expired before AgentTeams created an execution reference. The input remains
// retryable, but only after rotateFailedTaskManagerInvocation assigns a fresh
// invocation ID, expiry, and one-lifetime MCP bearer.
func (p *productionIngress) failExpiredPendingTaskManagerInvocation(ctx context.Context, inputRef string, invocationID kernel.InvocationID) (bool, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(p.projectID)+"/"+inputRef); err != nil {
		return false, err
	}
	var inputStatus, invocationStatus string
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx, `
SELECT i.status, r.status, r.expires_at
FROM production_manager_inputs i
JOIN runtime_invocations r ON r.invocation_id=i.invocation_id
WHERE i.project_id=$1 AND i.input_ref=$2 AND i.invocation_id=$3
FOR UPDATE OF i, r`, p.projectID, inputRef, invocationID).Scan(&inputStatus, &invocationStatus, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if inputStatus != "pending" || expiresAt.After(p.now().UTC()) ||
		(invocationStatus != string(runtimepkg.InvocationPrepared) && invocationStatus != string(runtimepkg.InvocationRunning) && invocationStatus != string(runtimepkg.InvocationWaiting)) {
		return false, tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1 AND status=$2`, invocationID, invocationStatus)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, err
	} else if affected != 1 {
		return false, kernel.Error{Code: kernel.CodeRevisionConflict, Message: "expired Task Manager invocation changed concurrently", Recoverable: true}
	}
	result, err = tx.ExecContext(ctx, `UPDATE production_manager_inputs SET status='failed', updated_at=$4 WHERE project_id=$1 AND input_ref=$2 AND invocation_id=$3 AND status='pending'`, p.projectID, inputRef, invocationID, p.now().UTC())
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, err
	} else if affected != 1 {
		return false, kernel.Error{Code: kernel.CodeRevisionConflict, Message: "expired Task Manager input changed concurrently", Recoverable: true}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// rebaseFailedTaskManagerInput preserves the authoritative boundary payload
// but gives a stale, unapplied decision a new inputRef at the current graph
// revision. The old DecisionRef remains immutable audit evidence; it is never
// rewritten or applied against a different revision.
func (p *productionIngress) rebaseFailedTaskManagerInput(ctx context.Context, inputRef string) error {
	snapshot, err := p.graph.Latest(ctx, p.projectID)
	if err != nil {
		return err
	}
	var input productionInput
	var selectedTaskID, selectedEndpointID, targetKind, targetRef sql.NullString
	var payloadText, status string
	err = p.db.QueryRowContext(ctx, `
SELECT input_kind, request_id, conversation_id,
       COALESCE((SELECT e.body FROM production_conversation_entries e
                 WHERE e.project_id=i.project_id AND e.manager_input_ref=i.input_ref
                 ORDER BY e.sequence LIMIT 1), ''),
       payload::text, selected_task_id, selected_endpoint_id, target_kind, target_ref, status
FROM production_manager_inputs i
WHERE project_id=$1 AND input_ref=$2`, p.projectID, inputRef).Scan(
		&input.Kind, &input.RequestID, &input.ConversationID, &input.Body, &payloadText,
		&selectedTaskID, &selectedEndpointID, &targetKind, &targetRef, &status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if status != "failed" {
		return nil
	}
	input.Payload = json.RawMessage(payloadText)
	input.TargetKind, input.TargetRef = targetKind.String, targetRef.String
	if selectedTaskID.Valid || selectedEndpointID.Valid {
		if !selectedTaskID.Valid || !selectedEndpointID.Valid {
			return kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager endpoint binding is incomplete", Recoverable: false}
		}
		ref := coordination.PhaseEndpointRef{TaskID: kernel.TaskID(selectedTaskID.String), EndpointID: coordination.EndpointID(selectedEndpointID.String)}
		input.SelectedEndpoint = &ref
	}
	input.RequestID = stableProductionSuffix(p.projectID, "revision-rebase", inputRef, snapshot.Revision)
	input.SeenRevision = snapshot.Revision
	stored, dispatchErr := p.persistAndDispatch(ctx, input)
	if dispatchErr != nil && !(stored.InputRef != "" && productionTerminalDeliveryWaitsForCapacity(dispatchErr)) {
		return dispatchErr
	}
	if stored.InputRef == "" {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "rebased Task Manager input was not persisted", Recoverable: true}
	}
	now := p.now().UTC()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE production_manager_inputs
SET status='completed', updated_at=$3
WHERE project_id=$1 AND input_ref=$2 AND status='failed'`, p.projectID, inputRef, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_conversation_entries(
project_id, conversation_id, entry_id, entry_kind, manager_input_ref, graph_revision, body, disposition, created_at)
VALUES ($1,$2,$3,'runtime',$4,$5,$6,'revision_rebased',$7)
ON CONFLICT (project_id, conversation_id, entry_id) DO NOTHING`,
		p.projectID, input.ConversationID, "runtime:rebase:"+stored.InputRef, inputRef, snapshot.Revision,
		"stale decision rebased as "+stored.InputRef, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *productionIngress) rotateFailedTaskManagerInvocation(ctx context.Context, inputRef string, previousInvocation kernel.InvocationID, previousAttempt int) (kernel.InvocationID, bool, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(p.projectID)+"/"+inputRef); err != nil {
		return "", false, err
	}
	var input productionInput
	var selectedTaskID, selectedEndpointID, targetKind, targetRef sql.NullString
	var payloadText, status string
	var currentInvocation kernel.InvocationID
	var attempt int
	err = tx.QueryRowContext(ctx, `
SELECT i.input_kind, i.request_id, i.conversation_id,
       COALESCE((SELECT e.body FROM production_conversation_entries e
                 WHERE e.project_id=i.project_id AND e.manager_input_ref=i.input_ref
                 ORDER BY e.sequence LIMIT 1), ''),
       i.payload::text, i.observed_graph_revision, i.selected_task_id, i.selected_endpoint_id,
       i.target_kind, i.target_ref, i.status, i.dispatch_attempt, i.invocation_id
FROM production_manager_inputs i
WHERE i.project_id=$1 AND i.input_ref=$2
FOR UPDATE`, p.projectID, inputRef).Scan(
		&input.Kind, &input.RequestID, &input.ConversationID, &input.Body, &payloadText,
		&input.SeenRevision, &selectedTaskID, &selectedEndpointID, &targetKind, &targetRef,
		&status, &attempt, &currentInvocation,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, tx.Commit()
	}
	if err != nil {
		return "", false, err
	}
	if status != "failed" || currentInvocation != previousInvocation || attempt != previousAttempt || attempt >= productionTaskManagerMaxAttempts {
		return "", false, tx.Commit()
	}
	input.Payload = []byte(payloadText)
	input.TargetKind = targetKind.String
	input.TargetRef = targetRef.String
	if selectedTaskID.Valid || selectedEndpointID.Valid {
		if !selectedTaskID.Valid || !selectedEndpointID.Valid {
			return "", false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager endpoint binding is incomplete", Recoverable: false}
		}
		endpoint := coordination.PhaseEndpointRef{TaskID: kernel.TaskID(selectedTaskID.String), EndpointID: coordination.EndpointID(selectedEndpointID.String)}
		input.SelectedEndpoint = &endpoint
	}
	nextAttempt := attempt + 1
	prepared, invocation, _, err := p.prepareAttempt(input, nextAttempt)
	if err != nil {
		return "", false, err
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(tx).Create(ctx, invocation); err != nil {
		return "", false, err
	}
	capabilities, _ := json.Marshal(prepared.RequiredCapabilities)
	result, err := tx.ExecContext(ctx, `
INSERT INTO production_taskmanager_bindings(
  project_id, invocation_id, input_ref, room_id, spec, runtime_config_ref, envelope_ref,
  required_capabilities, decision_ref, decision_kind, decision_action, mutation_applied, applied_graph_revision
)
SELECT project_id, $1, input_ref, $2, $3, $4, $5, $6::jsonb,
       decision_ref, decision_kind, decision_action, mutation_applied, applied_graph_revision
FROM production_taskmanager_bindings
WHERE project_id=$7 AND invocation_id=$8`, invocation.ID, prepared.RoomID, prepared.Spec,
		prepared.RuntimeConfigRef, prepared.EnvelopeRef, string(capabilities), p.projectID, previousInvocation)
	if err != nil {
		return "", false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return "", false, err
	} else if affected != 1 {
		return "", false, kernel.Error{Code: kernel.CodeInternalError, Message: "previous Task Manager binding is missing", Recoverable: false}
	}
	result, err = tx.ExecContext(ctx, `
UPDATE production_manager_inputs
SET invocation_id=$1, status='pending', dispatch_attempt=$2, updated_at=$3
WHERE project_id=$4 AND input_ref=$5 AND invocation_id=$6 AND status='failed' AND dispatch_attempt=$7`,
		invocation.ID, nextAttempt, p.now().UTC(), p.projectID, inputRef, previousInvocation, previousAttempt)
	if err != nil {
		return "", false, err
	}
	if affected, err := result.RowsAffected(); err != nil {
		return "", false, err
	} else if affected != 1 {
		return "", false, kernel.Error{Code: kernel.CodeRevisionConflict, Message: "Task Manager retry changed concurrently", Recoverable: true}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return invocation.ID, true, nil
}

func (p *productionIngress) DispatchTaskManagerFollowup(ctx context.Context, input productionInput) (persistedProductionInput, error) {
	if input.Kind != "phase_orchestration" ||
		(input.TargetKind != "phase_evaluation" && input.TargetKind != "stop_release" && input.TargetKind != "task_completion") {
		return persistedProductionInput{}, kernel.InvalidArgument("Task Manager follow-up must be phase_evaluation, stop_release, or task_completion")
	}
	if input.RequestID == "" || input.ConversationID == "" || input.SeenRevision <= 0 || input.SelectedEndpoint == nil || input.TargetRef == "" || len(input.Payload) == 0 {
		return persistedProductionInput{}, kernel.InvalidArgument("Task Manager follow-up identity and payload are required")
	}
	return p.persistAndDispatch(ctx, input)
}

func (p *productionIngress) prepare(input productionInput) (agentteams.PreparedInvocation, runtimepkg.Invocation, string, error) {
	return p.prepareAttempt(input, 1)
}

func (p *productionIngress) prepareAttempt(input productionInput, attempt int) (agentteams.PreparedInvocation, runtimepkg.Invocation, string, error) {
	if attempt <= 0 {
		return agentteams.PreparedInvocation{}, runtimepkg.Invocation{}, "", kernel.InvalidArgument("Task Manager dispatch attempt must be positive")
	}
	payloadHash := hashProductionBytes(input.Payload)
	suffix := stableProductionSuffix(p.projectID, input.Kind, input.RequestID)
	inputRef := "manager-input:" + suffix
	invocationSuffix := suffix
	if attempt > 1 {
		invocationSuffix = stableProductionSuffix(suffix, "attempt", attempt)
	}
	invocationID := kernel.InvocationID("tm-invocation:" + invocationSuffix)
	boundary, err := json.Marshal(struct {
		InputRef         string                         `json:"input_ref"`
		InputKind        string                         `json:"input_kind"`
		ConversationID   string                         `json:"conversation_id"`
		Body             string                         `json:"body"`
		SeenRevision     kernel.Revision                `json:"seen_graph_revision"`
		SelectedEndpoint *coordination.PhaseEndpointRef `json:"selected_endpoint,omitempty"`
		TargetKind       string                         `json:"target_kind,omitempty"`
		TargetRef        string                         `json:"target_ref,omitempty"`
		Payload          json.RawMessage                `json:"payload"`
	}{inputRef, input.Kind, input.ConversationID, input.Body, input.SeenRevision, input.SelectedEndpoint, input.TargetKind, input.TargetRef, json.RawMessage(input.Payload)})
	if err != nil {
		return agentteams.PreparedInvocation{}, runtimepkg.Invocation{}, "", err
	}
	now := p.now().UTC()
	invocation := runtimepkg.Invocation{ID: invocationID, ActorPrincipalID: kernel.ActorPrincipalID("task-manager:" + string(p.projectID)), ProjectID: p.projectID, Role: auth.RoleTaskManager, Status: runtimepkg.InvocationPrepared, CreatedAt: now, ExpiresAt: now.Add(taskManagerInvocationTTL)}
	assembly, err := p.assembler.Assemble(invocation, promptcatalog.RenderData{BoundaryInput: string(boundary)})
	if err != nil {
		return agentteams.PreparedInvocation{}, runtimepkg.Invocation{}, "", err
	}
	envelope, err := runtimepkg.EnvelopeFromInvocation(assembly.Invocation).JSON()
	if err != nil {
		return agentteams.PreparedInvocation{}, runtimepkg.Invocation{}, "", err
	}
	assembly, err = p.assembler.Assemble(assembly.Invocation, promptcatalog.RenderData{RuntimeEnvelope: envelope, BoundaryInput: string(boundary)})
	if err != nil {
		return agentteams.PreparedInvocation{}, runtimepkg.Invocation{}, "", err
	}
	prepared := agentteams.PreparedInvocation{InvocationID: invocationID, ProjectID: p.projectID, Role: auth.RoleTaskManager, RoomID: p.roomID, Spec: assembly.Prompt.Text, RuntimeConfigRef: "runtime-config:" + string(invocationID), EnvelopeRef: "runtime-envelope:" + string(invocationID), RequiredCapabilities: []string{agentteams.CapabilityTaskManager}}
	return prepared, assembly.Invocation, payloadHash, nil
}

func (p *productionIngress) persist(ctx context.Context, input productionInput, payloadHash string, prepared agentteams.PreparedInvocation, invocation runtimepkg.Invocation) (persistedProductionInput, bool, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return persistedProductionInput{}, false, err
	}
	defer tx.Rollback()
	lockKey := string(p.projectID) + "/" + input.Kind + "/" + input.RequestID
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return persistedProductionInput{}, false, err
	}
	existing, found, err := loadPersistedProductionInput(ctx, tx, p.projectID, input.Kind, input.RequestID)
	if err != nil {
		return persistedProductionInput{}, false, err
	}
	if found {
		if existing.payloadHash != payloadHash {
			return persistedProductionInput{}, false, kernel.IdempotencyConflict()
		}
		if err := tx.Commit(); err != nil {
			return persistedProductionInput{}, false, err
		}
		return existing.persistedProductionInput, existing.Status == "dispatched" || existing.Status == "completed" || existing.Status == "failed", nil
	}
	if err := runtimepkg.NewPostgresInvocationStoreFromSQL(tx).Create(ctx, invocation); err != nil {
		return persistedProductionInput{}, false, err
	}
	inputRef := "manager-input:" + stableProductionSuffix(p.projectID, input.Kind, input.RequestID)
	var taskID, endpointID any
	if input.SelectedEndpoint != nil {
		taskID, endpointID = input.SelectedEndpoint.TaskID, input.SelectedEndpoint.EndpointID
	}
	now := invocation.CreatedAt
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_manager_inputs(project_id, input_ref, request_id, input_kind, conversation_id, payload, payload_hash, observed_graph_revision, selected_task_id, selected_endpoint_id, target_kind, target_ref, invocation_id, status, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,NULLIF($11,''),NULLIF($12,''),$13,'pending',$14,$14)`, p.projectID, inputRef, input.RequestID, input.Kind, input.ConversationID, string(input.Payload), payloadHash, input.SeenRevision, taskID, endpointID, input.TargetKind, input.TargetRef, invocation.ID, now); err != nil {
		return persistedProductionInput{}, false, err
	}
	capabilities, _ := json.Marshal(prepared.RequiredCapabilities)
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_taskmanager_bindings(project_id, invocation_id, input_ref, room_id, spec, runtime_config_ref, envelope_ref, required_capabilities)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`, p.projectID, invocation.ID, inputRef, prepared.RoomID, prepared.Spec, prepared.RuntimeConfigRef, prepared.EnvelopeRef, string(capabilities)); err != nil {
		return persistedProductionInput{}, false, err
	}
	entryID := "input:" + stableProductionSuffix(p.projectID, input.Kind, input.RequestID)
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_conversation_entries(project_id, conversation_id, entry_id, entry_kind, manager_input_ref, graph_revision, body, disposition, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,'accepted',$8)`, p.projectID, input.ConversationID, entryID, input.Kind, inputRef, input.SeenRevision, input.Body, now); err != nil {
		return persistedProductionInput{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return persistedProductionInput{}, false, err
	}
	return persistedProductionInput{InputRef: inputRef, InvocationID: invocation.ID, ConversationID: input.ConversationID, Status: "pending"}, false, nil
}

type loadedProductionInput struct {
	persistedProductionInput
	payloadHash string
}

func loadPersistedProductionInput(ctx context.Context, tx *sql.Tx, projectID kernel.ProjectID, kind, requestID string) (loadedProductionInput, bool, error) {
	var loaded loadedProductionInput
	err := tx.QueryRowContext(ctx, `SELECT input_ref, invocation_id, conversation_id, status, payload_hash FROM production_manager_inputs WHERE project_id=$1 AND input_kind=$2 AND request_id=$3`, projectID, kind, requestID).
		Scan(&loaded.InputRef, &loaded.InvocationID, &loaded.ConversationID, &loaded.Status, &loaded.payloadHash)
	if errors.Is(err, sql.ErrNoRows) {
		return loadedProductionInput{}, false, nil
	}
	return loaded, err == nil, err
}

func (p *productionIngress) markDispatched(ctx context.Context, invocationID kernel.InvocationID) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := p.now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='running' WHERE invocation_id=$1 AND status='prepared'`, invocationID); err != nil {
		return err
	}
	if result, err := tx.ExecContext(ctx, `UPDATE production_manager_inputs SET status='dispatched', updated_at=$2 WHERE invocation_id=$1 AND status='pending'`, invocationID, now); err != nil {
		return err
	} else if affected, _ := result.RowsAffected(); affected == 0 {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, invocationID).Scan(&status); err != nil || (status != "dispatched" && status != "completed") {
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "manager input dispatch state changed", Recoverable: true}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_taskmanager_bindings SET dispatched_at=COALESCE(dispatched_at,$2) WHERE invocation_id=$1`, invocationID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *productionIngress) LoadPreparedInvocation(ctx context.Context, invocationRef string) (agentteams.PreparedInvocation, error) {
	var prepared agentteams.PreparedInvocation
	var capabilities []byte
	err := p.db.QueryRowContext(ctx, `SELECT b.invocation_id, b.project_id, i.role, COALESCE(i.operation,''), b.room_id, b.spec, b.runtime_config_ref, b.envelope_ref, b.required_capabilities
FROM production_taskmanager_bindings b JOIN runtime_invocations i ON i.invocation_id=b.invocation_id
WHERE b.invocation_id=$1`, invocationRef).Scan(&prepared.InvocationID, &prepared.ProjectID, &prepared.Role, &prepared.Operation, &prepared.RoomID, &prepared.Spec, &prepared.RuntimeConfigRef, &prepared.EnvelopeRef, &capabilities)
	if errors.Is(err, sql.ErrNoRows) {
		return agentteams.PreparedInvocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "prepared Task Manager invocation not found", Recoverable: false}
	}
	if err != nil {
		return agentteams.PreparedInvocation{}, err
	}
	if err := json.Unmarshal(capabilities, &prepared.RequiredCapabilities); err != nil {
		return agentteams.PreparedInvocation{}, fmt.Errorf("decode prepared invocation capabilities: %w", err)
	}
	return prepared, nil
}

func (p *productionIngress) requireProject(principal auth.Principal, projectID kernel.ProjectID) error {
	if projectID != p.projectID || principal.ProjectID != p.projectID {
		return kernel.Forbidden("production ingress project mismatch")
	}
	return nil
}

func stableProductionSuffix(parts ...any) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(fmt.Sprint(part)))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func hashProductionBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

var _ httpapi.RequirementCommandPort = (*productionIngress)(nil)
var _ httpapi.HumanDecisionPort = (*productionIngress)(nil)
var _ httpapi.ManagerPort = (*productionIngress)(nil)
var _ agentteams.InvocationSource = (*productionIngress)(nil)
var _ productionTaskManagerFollowupDispatcher = (*productionIngress)(nil)
