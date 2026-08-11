package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
)

// productionTaskManagerRuntime binds every MCP mutation to the immutable input
// row selected when the invocation was created. InputRef, graph revision,
// endpoint and decision reference never come from Agent JSON.
type productionTaskManagerRuntime struct {
	db          *sql.DB
	projectID   kernel.ProjectID
	graphStore  *coordination.PostgresStore
	decisions   *taskmanager.PostgresStore
	idempotency kernel.IdempotencyStore
	now         func() time.Time
}

type productionTaskManagerBinding struct {
	InvocationID     kernel.InvocationID
	InputRef         string
	ConversationID   string
	SeenRevision     kernel.Revision
	SelectedTaskID   kernel.TaskID
	SelectedEndpoint coordination.EndpointID
	TargetKind       string
	TargetRef        string
	DecisionRef      string
	DecisionKind     taskmanager.DecisionKind
	DecisionAction   string
	MutationApplied  bool
	AppliedRevision  kernel.Revision
}

func newProductionTaskManagerRuntime(db *sql.DB, projectID kernel.ProjectID, graphStore *coordination.PostgresStore, now func() time.Time) (*productionTaskManagerRuntime, error) {
	if db == nil || graphStore == nil || kernel.IsZeroID(projectID) {
		return nil, kernel.InvalidArgument("production Task Manager database, project, and graph are required")
	}
	if now == nil {
		now = time.Now
	}
	return &productionTaskManagerRuntime{db: db, projectID: projectID, graphStore: graphStore, decisions: taskmanager.NewPostgresStore(db, projectID, graphStore), idempotency: kernel.NewMemoryIdempotencyStore(), now: now}, nil
}

func (p *productionTaskManagerRuntime) Snapshot(ctx context.Context, caller auth.Principal, scope auth.BoundScope, revision kernel.Revision) (coordination.GraphSnapshot, error) {
	if _, err := p.binding(ctx, caller, scope); err != nil {
		return coordination.GraphSnapshot{}, err
	}
	return p.graph(caller).Snapshot(ctx, revision)
}

func (p *productionTaskManagerRuntime) SubmitTaskManagerDecision(ctx context.Context, caller auth.Principal, scope auth.BoundScope, decision taskmanager.TaskManagerDecision) (string, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return "", err
	}
	if binding.DecisionRef != "" {
		matches, err := p.decisionMatches(ctx, binding, decision)
		if err != nil {
			return "", err
		}
		if !matches {
			return "", kernel.IdempotencyConflict()
		}
		if binding.DecisionKind == taskmanager.DecisionKindTerminal && !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, binding.SeenRevision); err != nil {
				return "", err
			}
		}
		return binding.DecisionRef, nil
	}
	snapshot, err := p.graph(caller).Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		return "", err
	}
	if snapshot.Revision != binding.SeenRevision {
		return "", kernel.RevisionConflict(binding.SeenRevision, snapshot.Revision)
	}
	kind, transition, err := trustedDecisionMutation(binding, snapshot, decision)
	if err != nil {
		return "", err
	}
	decisionRef, err := p.decisions.SubmitDecision(ctx, taskmanager.DecisionSubmission{ProjectID: p.projectID, InputRef: binding.InputRef, ExpectedRevision: binding.SeenRevision, Decision: decision, Kind: kind, Transition: transition})
	if err != nil {
		return "", err
	}
	if err := p.persistDecisionAcceptance(ctx, binding, decisionRef, kind, decision, snapshot.Revision); err != nil {
		return "", err
	}
	if kind == taskmanager.DecisionKindTerminal {
		if err := p.complete(ctx, caller.InvocationID, decisionRef, snapshot.Revision); err != nil {
			return "", err
		}
	}
	return decisionRef, nil
}

func (p *productionTaskManagerRuntime) ReplacePending(ctx context.Context, caller auth.Principal, scope auth.BoundScope, intent mcpapi.PendingSubgraphIntent) (kernel.Revision, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return 0, err
	}
	if binding.DecisionKind != taskmanager.DecisionKindReplacePending || binding.DecisionRef == "" || binding.DecisionAction != "replace_pending" {
		return 0, kernel.Forbidden("replacePending requires this invocation's persisted replace_pending decision")
	}
	if revision, found, err := p.recoverAppliedRevision(ctx, binding); err != nil {
		return 0, err
	} else if found {
		if !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, revision); err != nil {
				return 0, err
			}
		}
		return revision, nil
	}
	next := coordination.PendingSubgraph{RequestID: kernel.IdempotencyKey(binding.DecisionRef), BaseRevision: binding.SeenRevision, Tasks: intent.Tasks, Endpoints: intent.Endpoints, Edges: intent.Edges, Blockers: intent.Blockers}
	revision, err := p.graph(caller).ReplacePending(ctx, next)
	if err != nil {
		return 0, err
	}
	if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, revision); err != nil {
		return 0, err
	}
	return revision, nil
}

func (p *productionTaskManagerRuntime) Transition(ctx context.Context, caller auth.Principal, scope auth.BoundScope) (kernel.Revision, error) {
	binding, err := p.binding(ctx, caller, scope)
	if err != nil {
		return 0, err
	}
	if binding.DecisionKind != taskmanager.DecisionKindTransition || binding.DecisionRef == "" {
		return 0, kernel.Forbidden("transition requires this invocation's persisted transition decision")
	}
	if revision, found, err := p.recoverAppliedRevision(ctx, binding); err != nil {
		return 0, err
	} else if found {
		if !binding.MutationApplied {
			if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, revision); err != nil {
				return 0, err
			}
		}
		return revision, nil
	}
	revision, err := p.graph(caller).Transition(ctx, binding.SeenRevision, binding.DecisionRef)
	if err != nil {
		return 0, err
	}
	if err := p.complete(ctx, caller.InvocationID, binding.DecisionRef, revision); err != nil {
		return 0, err
	}
	return revision, nil
}

func (p *productionTaskManagerRuntime) graph(caller auth.Principal) coordination.TaskManagerGraph {
	return coordination.NewTaskManagerGraph(caller, p.graphStore, p.graphStore, p.idempotency)
}

func (p *productionTaskManagerRuntime) binding(ctx context.Context, caller auth.Principal, scope auth.BoundScope) (productionTaskManagerBinding, error) {
	if caller.Role != auth.RoleTaskManager || caller.ProjectID != p.projectID || caller.InvocationID == "" || scope.ProjectID != caller.ProjectID || scope.InvocationID != caller.InvocationID {
		return productionTaskManagerBinding{}, kernel.Forbidden("Task Manager invocation scope mismatch")
	}
	var binding productionTaskManagerBinding
	var taskID, endpointID, targetKind, targetRef, decisionRef, decisionKind, decisionAction sql.NullString
	var appliedRevision sql.NullInt64
	err := p.db.QueryRowContext(ctx, `SELECT b.invocation_id, b.input_ref, i.conversation_id, i.observed_graph_revision,
i.selected_task_id, i.selected_endpoint_id, i.target_kind, i.target_ref,
b.decision_ref, b.decision_kind, b.decision_action, b.mutation_applied, b.applied_graph_revision
FROM production_taskmanager_bindings b
JOIN production_manager_inputs i ON i.project_id=b.project_id AND i.input_ref=b.input_ref
WHERE b.project_id=$1 AND b.invocation_id=$2`, p.projectID, caller.InvocationID).
		Scan(&binding.InvocationID, &binding.InputRef, &binding.ConversationID, &binding.SeenRevision, &taskID, &endpointID, &targetKind, &targetRef, &decisionRef, &decisionKind, &decisionAction, &binding.MutationApplied, &appliedRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return productionTaskManagerBinding{}, kernel.Forbidden("Task Manager invocation is not bound to a production input")
	}
	if err != nil {
		return productionTaskManagerBinding{}, err
	}
	binding.SelectedTaskID = kernel.TaskID(taskID.String)
	binding.SelectedEndpoint = coordination.EndpointID(endpointID.String)
	binding.TargetKind, binding.TargetRef = targetKind.String, targetRef.String
	binding.DecisionRef, binding.DecisionKind, binding.DecisionAction = decisionRef.String, taskmanager.DecisionKind(decisionKind.String), decisionAction.String
	if appliedRevision.Valid {
		binding.AppliedRevision = kernel.Revision(appliedRevision.Int64)
	}
	return binding, nil
}

func (p *productionTaskManagerRuntime) decisionMatches(ctx context.Context, binding productionTaskManagerBinding, decision taskmanager.TaskManagerDecision) (bool, error) {
	var storedRaw string
	err := p.db.QueryRowContext(ctx, `SELECT decision::text FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2 AND input_ref=$3`, p.projectID, binding.DecisionRef, binding.InputRef).Scan(&storedRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager decision is missing", Recoverable: true}
	}
	if err != nil {
		return false, err
	}
	var stored taskmanager.TaskManagerDecision
	if err := json.Unmarshal([]byte(storedRaw), &stored); err != nil {
		return false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager decision is invalid", Recoverable: true}
	}
	storedCanonical, err := json.Marshal(stored)
	if err != nil {
		return false, err
	}
	replayedCanonical, err := json.Marshal(decision)
	if err != nil {
		return false, kernel.InvalidArgument("manager decision must be JSON serializable")
	}
	return bytes.Equal(storedCanonical, replayedCanonical), nil
}

// recoverAppliedRevision closes the only non-atomic gap between the
// coordination transaction and the production binding transaction. The graph
// revision is accepted only at the invocation's exact next revision and with
// the decision reference derived by the coordination store.
func (p *productionTaskManagerRuntime) recoverAppliedRevision(ctx context.Context, binding productionTaskManagerBinding) (kernel.Revision, bool, error) {
	if binding.MutationApplied {
		if binding.AppliedRevision <= 0 {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "applied Task Manager mutation has no graph revision", Recoverable: true}
		}
		return binding.AppliedRevision, true, nil
	}
	if binding.DecisionRef == "" || (binding.DecisionKind != taskmanager.DecisionKindReplacePending && binding.DecisionKind != taskmanager.DecisionKindTransition) {
		return 0, false, nil
	}
	graphDecisionRef := binding.DecisionRef
	if binding.DecisionKind == taskmanager.DecisionKindTransition {
		var transitionRaw string
		err := p.db.QueryRowContext(ctx, `SELECT transition_payload::text FROM taskmanager_decisions
WHERE project_id=$1 AND decision_ref=$2 AND input_ref=$3`, p.projectID, binding.DecisionRef, binding.InputRef).Scan(&transitionRaw)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager transition is missing", Recoverable: true}
		}
		if err != nil {
			return 0, false, err
		}
		var transition coordination.GraphTransition
		if err := json.Unmarshal([]byte(transitionRaw), &transition); err != nil {
			return 0, false, kernel.Error{Code: kernel.CodeInternalError, Message: "persisted Task Manager transition is invalid", Recoverable: true}
		}
		canonical, err := json.Marshal(transition)
		if err != nil {
			return 0, false, err
		}
		graphDecisionRef = "transition:" + hashProductionBytes(canonical)
	}
	want := binding.SeenRevision.Next()
	var revision kernel.Revision
	err := p.db.QueryRowContext(ctx, `SELECT revision FROM coordination_graph_revisions
WHERE project_id=$1 AND revision=$2 AND decision_ref=$3`, p.projectID, want, graphDecisionRef).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return revision, true, nil
}

func trustedDecisionMutation(binding productionTaskManagerBinding, snapshot coordination.GraphSnapshot, decision taskmanager.TaskManagerDecision) (taskmanager.DecisionKind, coordination.GraphTransition, error) {
	switch decision.Action {
	case "replace_pending":
		if decision.TargetRef != "" {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("replace_pending target_ref must be omitted")
		}
		return taskmanager.DecisionKindReplacePending, coordination.GraphTransition{}, nil
	case "reject", "defer", "no_change":
		if decision.TargetRef != "" {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("terminal decision target_ref must be omitted")
		}
		return taskmanager.DecisionKindTerminal, coordination.GraphTransition{}, nil
	case "held", "released":
		ref, err := trustedEndpoint(binding)
		if err != nil {
			return "", coordination.GraphTransition{}, err
		}
		want := fmt.Sprintf("%s/%s", ref.TaskID, ref.EndpointID)
		if decision.TargetRef != want {
			return "", coordination.GraphTransition{}, kernel.InvalidArgument("decision target_ref does not match Runtime-selected endpoint")
		}
		for _, endpoint := range snapshot.Endpoints {
			if endpoint.Ref == ref {
				return taskmanager.DecisionKindTransition, coordination.GraphTransition{TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref, Action: decision.Action, Generation: endpoint.Generation}, nil
			}
		}
		return "", coordination.GraphTransition{}, kernel.Error{Code: kernel.CodeNotFound, Message: "Runtime-selected endpoint not found", Recoverable: true}
	default:
		return "", coordination.GraphTransition{}, kernel.Forbidden("decision action requires a Runtime-authenticated phase or delivery boundary")
	}
}

func trustedEndpoint(binding productionTaskManagerBinding) (coordination.PhaseEndpointRef, error) {
	if binding.SelectedTaskID != "" && binding.SelectedEndpoint != "" {
		return coordination.PhaseEndpointRef{TaskID: binding.SelectedTaskID, EndpointID: binding.SelectedEndpoint}, nil
	}
	if binding.TargetKind == "endpoint" || binding.TargetKind == "phase_endpoint" {
		parts := strings.Split(binding.TargetRef, "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return coordination.PhaseEndpointRef{TaskID: kernel.TaskID(parts[0]), EndpointID: coordination.EndpointID(parts[1])}, nil
		}
	}
	return coordination.PhaseEndpointRef{}, kernel.Forbidden("input is not bound to a Runtime-selected endpoint")
}

func (p *productionTaskManagerRuntime) complete(ctx context.Context, invocationID kernel.InvocationID, decisionRef string, revision kernel.Revision) error {
	if revision <= 0 {
		return kernel.InvalidArgument("applied graph revision is required")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := p.now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE production_taskmanager_bindings
SET mutation_applied=TRUE, applied_graph_revision=COALESCE(applied_graph_revision,$3), completed_at=COALESCE(completed_at,$4)
WHERE invocation_id=$1 AND decision_ref=$2 AND (applied_graph_revision IS NULL OR applied_graph_revision=$3)`, invocationID, decisionRef, revision, now)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return kernel.IdempotencyConflict()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_manager_inputs SET status='completed', updated_at=$2 WHERE invocation_id=$1`, invocationID, now); err != nil {
		return err
	}
	// AgentTeams may start the agent before ingress records the dispatch ack.
	// Completing from prepared is the fail-safe catch-up for that race; the
	// later dispatch ack cannot move a completed invocation back to running.
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_invocations SET status='completed' WHERE invocation_id=$1 AND status IN ('prepared','running','waiting')`, invocationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE production_conversation_entries SET graph_revision=$1 WHERE project_id=$2 AND decision_ref=$3`, revision, p.projectID, decisionRef); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *productionTaskManagerRuntime) persistDecisionAcceptance(ctx context.Context, binding productionTaskManagerBinding, decisionRef string, kind taskmanager.DecisionKind, decision taskmanager.TaskManagerDecision, revision kernel.Revision) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE production_taskmanager_bindings
SET decision_ref=$2, decision_kind=$3, decision_action=$4
WHERE invocation_id=$1 AND (decision_ref IS NULL OR decision_ref=$2)`, binding.InvocationID, decisionRef, kind, decision.Action)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return kernel.IdempotencyConflict()
	}
	body, _ := json.Marshal(decision)
	if _, err := tx.ExecContext(ctx, `INSERT INTO production_conversation_entries(project_id, conversation_id, entry_id, entry_kind, manager_input_ref, decision_ref, graph_revision, body, disposition, created_at)
VALUES ($1,$2,$3,'decision',$4,$5,$6,$7,$8,$9)
ON CONFLICT (project_id, conversation_id, entry_id) DO NOTHING`, p.projectID, binding.ConversationID, "decision:"+decisionRef, binding.InputRef, decisionRef, revision, string(body), "accepted", p.now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

var _ mcpapi.TaskManagerAgentRuntime = (*productionTaskManagerRuntime)(nil)
