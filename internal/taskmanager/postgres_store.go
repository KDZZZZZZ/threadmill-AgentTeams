package taskmanager

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/jackc/pgx/v5/pgconn"
)

type postgresBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type CoordinationDecisionRegistrar interface {
	RegisterReplacePending(context.Context, kernel.ProjectID, kernel.IdempotencyKey) error
	RegisterTransition(context.Context, kernel.ProjectID, string, coordination.GraphTransition) error
}

type PostgresStore struct {
	db        postgresBeginner
	projectID kernel.ProjectID
	decisions CoordinationDecisionRegistrar
}

func NewPostgresStore(db postgresBeginner, projectID kernel.ProjectID, decisions CoordinationDecisionRegistrar) *PostgresStore {
	return &PostgresStore{db: db, projectID: projectID, decisions: decisions}
}

func (s *PostgresStore) SubmitDecision(ctx context.Context, submission DecisionSubmission) (string, error) {
	if err := s.requireProject(submission.ProjectID); err != nil {
		return "", err
	}
	if submission.InputRef == "" {
		return "", kernel.InvalidArgument("input_ref is required")
	}
	if submission.ExpectedRevision <= 0 {
		return "", kernel.InvalidArgument("expected_revision is required")
	}
	if submission.Decision.Action == "" || submission.Decision.Reason == "" {
		return "", kernel.InvalidArgument("manager decision action and reason are required")
	}
	if submission.Kind == DecisionKindTransition {
		if err := validateTransitionForDecision(submission.Transition); err != nil {
			return "", err
		}
	}
	payload, err := decisionPayload(submission)
	if err != nil {
		return "", err
	}
	ref := "tmdec:" + hashBytes(payload)
	if err := s.insertDecision(ctx, submission, ref, payload); err != nil {
		return "", err
	}
	switch submission.Kind {
	case DecisionKindReplacePending:
		if s.decisions == nil {
			return "", kernel.Error{Code: kernel.CodeInternalError, Message: "coordination decision registrar is required", Recoverable: true}
		}
		return ref, s.decisions.RegisterReplacePending(ctx, submission.ProjectID, kernel.IdempotencyKey(ref))
	case DecisionKindTransition:
		if s.decisions == nil {
			return "", kernel.Error{Code: kernel.CodeInternalError, Message: "coordination decision registrar is required", Recoverable: true}
		}
		return ref, s.decisions.RegisterTransition(ctx, submission.ProjectID, ref, submission.Transition)
	case DecisionKindTerminal:
		return ref, nil
	default:
		return "", kernel.InvalidArgument("decision kind is not allowed")
	}
}

func (s *PostgresStore) ResolveRequirementContract(ctx context.Context, input RequirementInput) (TaskContract, error) {
	if err := s.requireDefaultProject(); err != nil {
		return TaskContract{}, err
	}
	if err := validateRequirementInput(input); err != nil {
		return TaskContract{}, err
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return TaskContract{}, err
	}
	defer tx.Rollback()
	if err := insertRequirement(ctx, tx, s.projectID, input); err != nil {
		return TaskContract{}, err
	}
	contract, err := taskContractByReference(ctx, tx, s.projectID, input.TaskID, input.ContractRef)
	if err != nil {
		return TaskContract{}, err
	}
	if err := validateTaskContract(contract); err != nil {
		return TaskContract{}, err
	}
	if err := insertContract(ctx, tx, s.projectID, input.InputRef, contract); err != nil {
		return TaskContract{}, err
	}
	if err := tx.Commit(); err != nil {
		return TaskContract{}, mapPostgresError(err)
	}
	return contract, nil
}

func (s *PostgresStore) PersistRequirementContract(ctx context.Context, input RequirementInput, contract TaskContract) error {
	if err := s.requireDefaultProject(); err != nil {
		return err
	}
	if err := validateRequirementInput(input); err != nil {
		return err
	}
	if err := validateRequirementContractBinding(input, contract); err != nil {
		return err
	}
	if err := validateTaskContract(contract); err != nil {
		return err
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := insertRequirement(ctx, tx, s.projectID, input); err != nil {
		return err
	}
	if err := insertContract(ctx, tx, s.projectID, input.InputRef, contract); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return mapPostgresError(err)
	}
	return nil
}

func taskContractByReference(ctx context.Context, tx *sql.Tx, projectID kernel.ProjectID, taskID kernel.TaskID, contractRef string) (TaskContract, error) {
	var storedTaskID kernel.TaskID
	var storedContractRef, policy, specsRaw string
	err := tx.QueryRowContext(ctx, `SELECT task_id, contract_ref, delivery_policy, phase_specs::text
FROM taskmanager_contracts WHERE project_id = $1 AND task_id = $2 AND contract_ref = $3`, projectID, taskID, contractRef).
		Scan(&storedTaskID, &storedContractRef, &policy, &specsRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskContract{}, kernel.Error{Code: kernel.CodeNotFound, Message: "runtime-attested task contract is not persisted", Recoverable: true}
	}
	if err != nil {
		return TaskContract{}, mapPostgresError(err)
	}
	var specs map[coordination.EndpointID]string
	if err := json.Unmarshal([]byte(specsRaw), &specs); err != nil {
		return TaskContract{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored task contract phase specs are invalid", Recoverable: true}
	}
	return TaskContract{TaskID: storedTaskID, ContractRef: storedContractRef, DeliveryPolicy: DeliveryPolicy(policy), PhaseSpecs: specs}, nil
}

func (s *PostgresStore) TaskContract(ctx context.Context, taskID kernel.TaskID) (TaskContract, error) {
	if err := s.requireDefaultProject(); err != nil {
		return TaskContract{}, err
	}
	if err := kernel.RequireID("task_id", taskID); err != nil {
		return TaskContract{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return TaskContract{}, err
	}
	defer tx.Rollback()
	var contractRef, policy, specsRaw string
	err = tx.QueryRowContext(ctx, `SELECT contract_ref, delivery_policy, phase_specs::text FROM taskmanager_contracts WHERE project_id = $1 AND task_id = $2`, s.projectID, taskID).Scan(&contractRef, &policy, &specsRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskContract{}, kernel.Error{Code: kernel.CodeNotFound, Message: "task contract not found", Recoverable: false}
	}
	if err != nil {
		return TaskContract{}, mapPostgresError(err)
	}
	var specs map[coordination.EndpointID]string
	if err := json.Unmarshal([]byte(specsRaw), &specs); err != nil {
		return TaskContract{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored task contract phase specs are invalid", Recoverable: true}
	}
	if err := tx.Commit(); err != nil {
		return TaskContract{}, mapPostgresError(err)
	}
	return TaskContract{TaskID: taskID, ContractRef: contractRef, DeliveryPolicy: DeliveryPolicy(policy), PhaseSpecs: specs}, nil
}

func (s *PostgresStore) AppendManagerReply(ctx context.Context, reply ManagerReplyEvent) error {
	if err := s.requireDefaultProject(); err != nil {
		return err
	}
	if reply.InputRef == "" {
		return kernel.InvalidArgument("reply input_ref is required")
	}
	if reply.Status == "" {
		return kernel.InvalidArgument("reply status is required")
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return kernel.InvalidArgument("manager reply must be JSON serializable")
	}
	tx, err := s.begin(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO taskmanager_replies(project_id, input_ref, status, decision_ref, graph_revision, reason, payload_hash)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (project_id, input_ref) DO UPDATE SET input_ref = taskmanager_replies.input_ref
WHERE taskmanager_replies.payload_hash = EXCLUDED.payload_hash`, s.projectID, reply.InputRef, reply.Status, reply.DecisionRef, reply.GraphRevision, reply.Reason, hashBytes(payload))
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return tx.Commit()
}

func (s *PostgresStore) ManagerReply(ctx context.Context, inputRef string) (ManagerReplyEvent, bool, error) {
	if err := s.requireDefaultProject(); err != nil {
		return ManagerReplyEvent{}, false, err
	}
	if inputRef == "" {
		return ManagerReplyEvent{}, false, kernel.InvalidArgument("input_ref is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ManagerReplyEvent{}, false, err
	}
	defer tx.Rollback()
	var reply ManagerReplyEvent
	err = tx.QueryRowContext(ctx, `SELECT input_ref, status, decision_ref, graph_revision, reason FROM taskmanager_replies WHERE project_id = $1 AND input_ref = $2`, s.projectID, inputRef).
		Scan(&reply.InputRef, &reply.Status, &reply.DecisionRef, &reply.GraphRevision, &reply.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return ManagerReplyEvent{}, false, nil
	}
	if err != nil {
		return ManagerReplyEvent{}, false, mapPostgresError(err)
	}
	if err := tx.Commit(); err != nil {
		return ManagerReplyEvent{}, false, mapPostgresError(err)
	}
	return reply, true, nil
}

func (s *PostgresStore) insertDecision(ctx context.Context, submission DecisionSubmission, ref string, payload []byte) error {
	decisionRaw, err := json.Marshal(submission.Decision)
	if err != nil {
		return kernel.InvalidArgument("manager decision must be JSON serializable")
	}
	var transitionRaw []byte
	if submission.Kind == DecisionKindTransition {
		transitionRaw, err = json.Marshal(submission.Transition)
		if err != nil {
			return kernel.InvalidArgument("transition must be JSON serializable")
		}
	} else {
		transitionRaw = []byte("null")
	}
	tx, err := s.begin(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO taskmanager_decisions(project_id, decision_ref, input_ref, expected_revision, kind, decision, transition_payload, payload_hash)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
ON CONFLICT (project_id, decision_ref) DO UPDATE SET decision_ref = taskmanager_decisions.decision_ref
WHERE taskmanager_decisions.payload_hash = EXCLUDED.payload_hash`, submission.ProjectID, ref, submission.InputRef, submission.ExpectedRevision, submission.Kind, string(decisionRaw), string(transitionRaw), hashBytes(payload))
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return tx.Commit()
}

func insertRequirement(ctx context.Context, tx *sql.Tx, projectID kernel.ProjectID, input RequirementInput) error {
	raw, err := json.Marshal(input.Requirement)
	if err != nil {
		return kernel.InvalidArgument("requirement input must be JSON serializable")
	}
	payload, err := json.Marshal(struct {
		InputRef    string      `json:"input_ref"`
		TaskID      string      `json:"task_id"`
		ContractRef string      `json:"contract_ref"`
		Requirement Requirement `json:"requirement"`
	}{InputRef: input.InputRef, TaskID: string(input.TaskID), ContractRef: input.ContractRef, Requirement: input.Requirement})
	if err != nil {
		return kernel.InvalidArgument("requirement input must be JSON serializable")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO taskmanager_requirement_inputs(project_id, input_ref, task_id, contract_ref, requirement, payload_hash)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (project_id, input_ref) DO UPDATE SET input_ref = taskmanager_requirement_inputs.input_ref
WHERE taskmanager_requirement_inputs.payload_hash = EXCLUDED.payload_hash`, projectID, input.InputRef, input.TaskID, input.ContractRef, string(raw), hashBytes(payload))
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func insertContract(ctx context.Context, tx *sql.Tx, projectID kernel.ProjectID, inputRef string, contract TaskContract) error {
	specsRaw, err := json.Marshal(contract.PhaseSpecs)
	if err != nil {
		return kernel.InvalidArgument("task contract phase specs must be JSON serializable")
	}
	payload, err := json.Marshal(contract)
	if err != nil {
		return kernel.InvalidArgument("task contract must be JSON serializable")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO taskmanager_contracts(project_id, task_id, input_ref, contract_ref, delivery_policy, phase_specs, payload_hash)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
ON CONFLICT (project_id, task_id) DO UPDATE SET task_id = taskmanager_contracts.task_id
WHERE taskmanager_contracts.payload_hash = EXCLUDED.payload_hash`, projectID, contract.TaskID, inputRef, contract.ContractRef, contract.DeliveryPolicy, string(specsRaw), hashBytes(payload))
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func validateRequirementContractBinding(input RequirementInput, contract TaskContract) error {
	if contract.TaskID != input.TaskID {
		return kernel.InvalidArgument("task contract task_id must match requirement input")
	}
	if contract.ContractRef != input.ContractRef {
		return kernel.InvalidArgument("task contract contract_ref must match requirement input")
	}
	return nil
}

func decisionPayload(submission DecisionSubmission) ([]byte, error) {
	return json.Marshal(struct {
		ProjectID        kernel.ProjectID             `json:"project_id"`
		InputRef         string                       `json:"input_ref"`
		ExpectedRevision kernel.Revision              `json:"expected_revision"`
		Kind             DecisionKind                 `json:"kind"`
		Decision         TaskManagerDecision          `json:"decision"`
		Transition       coordination.GraphTransition `json:"transition,omitempty"`
	}{
		ProjectID:        submission.ProjectID,
		InputRef:         submission.InputRef,
		ExpectedRevision: submission.ExpectedRevision,
		Kind:             submission.Kind,
		Decision:         submission.Decision,
		Transition:       submission.Transition,
	})
}

func validateTransitionForDecision(transition coordination.GraphTransition) error {
	if transition.Action == "" {
		return kernel.InvalidArgument("transition action is required")
	}
	return nil
}

func (s *PostgresStore) requireProject(projectID kernel.ProjectID) error {
	if err := s.requireDefaultProject(); err != nil {
		return err
	}
	if projectID != s.projectID {
		return kernel.Forbidden("decision project_id does not match taskmanager store project")
	}
	return nil
}

func (s *PostgresStore) requireDefaultProject() error {
	if s == nil || s.db == nil {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "taskmanager postgres database is not configured", Recoverable: true}
	}
	if kernel.IsZeroID(s.projectID) {
		return kernel.InvalidArgument("project_id is required")
	}
	return nil
}

func (s *PostgresStore) begin(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return tx, nil
}

func serializableTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelSerializable}
}

func hashBytes(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func affected(result sql.Result) int64 {
	n, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return kernel.IdempotencyConflict()
		case "23503":
			return kernel.InvalidGraph("taskmanager record references missing rows")
		case "40001":
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "serialization conflict while updating taskmanager store", Recoverable: true}
		}
	}
	if strings.Contains(err.Error(), "violates unique constraint") {
		return kernel.IdempotencyConflict()
	}
	return err
}

var _ DecisionStore = (*PostgresStore)(nil)
var _ ContractStore = (*PostgresStore)(nil)
var _ RuntimeContractRecorder = (*PostgresStore)(nil)
var _ ReplyRecorder = (*PostgresStore)(nil)
