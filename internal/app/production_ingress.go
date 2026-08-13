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

const taskManagerInvocationTTL = 24 * time.Hour

type productionInvocationDispatcher interface {
	Dispatch(context.Context, string) (agentteams.AgentTeamsExecutionRef, error)
}

type productionTaskManagerExecutionCleaner interface {
	CleanupCompletedTaskManagerInvocations(context.Context) error
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
	if err != nil {
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
	if err != nil {
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
	if err != nil {
		return httpapi.HumanDecisionResponse{}, err
	}
	return httpapi.HumanDecisionResponse{HumanDecisionRef: "human-decision:" + stableProductionSuffix(p.projectID, "human", req.RequestID), ManagerInputRef: stored.InputRef, InvocationRef: stored.InvocationID, Status: "accepted"}, nil
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
	if alreadyDispatched {
		return stored, nil
	}
	if p.cleaner != nil {
		if err := p.cleaner.CleanupCompletedTaskManagerInvocations(ctx); err != nil {
			return persistedProductionInput{}, err
		}
	}
	if _, err := p.dispatcher.Dispatch(ctx, string(stored.InvocationID)); err != nil {
		return persistedProductionInput{}, err
	}
	if err := p.markDispatched(ctx, stored.InvocationID); err != nil {
		return persistedProductionInput{}, err
	}
	stored.Status = "dispatched"
	return stored, nil
}

func (p *productionIngress) DispatchTaskManagerFollowup(ctx context.Context, input productionInput) (persistedProductionInput, error) {
	if input.Kind != "phase_orchestration" || (input.TargetKind != "phase_evaluation" && input.TargetKind != "stop_release") {
		return persistedProductionInput{}, kernel.InvalidArgument("Task Manager follow-up must be phase_evaluation or stop_release")
	}
	if input.RequestID == "" || input.ConversationID == "" || input.SeenRevision <= 0 || input.SelectedEndpoint == nil || input.TargetRef == "" || len(input.Payload) == 0 {
		return persistedProductionInput{}, kernel.InvalidArgument("Task Manager follow-up identity and payload are required")
	}
	return p.persistAndDispatch(ctx, input)
}

func (p *productionIngress) prepare(input productionInput) (agentteams.PreparedInvocation, runtimepkg.Invocation, string, error) {
	payloadHash := hashProductionBytes(input.Payload)
	suffix := stableProductionSuffix(p.projectID, input.Kind, input.RequestID)
	inputRef := "manager-input:" + suffix
	invocationID := kernel.InvocationID("tm-invocation:" + suffix)
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
	prepared := agentteams.PreparedInvocation{InvocationID: invocationID, ProjectID: p.projectID, Role: auth.RoleTaskManager, RoomID: p.roomID, Spec: assembly.Prompt.Text, RuntimeConfigRef: "runtime-config:" + string(invocationID), EnvelopeRef: "runtime-envelope:" + string(invocationID), RequiredCapabilities: []string{}}
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
		return existing.persistedProductionInput, existing.Status == "dispatched" || existing.Status == "completed", nil
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
