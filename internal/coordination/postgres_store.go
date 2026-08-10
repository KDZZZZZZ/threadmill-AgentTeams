package coordination

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/jackc/pgx/v5/pgconn"
)

type postgresDBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type postgresTx interface {
	postgresDBTX
	Commit() error
	Rollback() error
}

type postgresBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type PostgresStore struct {
	db postgresBeginner
}

func NewPostgresStore(db postgresBeginner) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Latest(ctx context.Context, projectID kernel.ProjectID) (GraphSnapshot, error) {
	if err := requireProject(projectID); err != nil {
		return GraphSnapshot{}, err
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GraphSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := loadLatestSnapshot(ctx, tx, projectID, false)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return GraphSnapshot{}, err
	}
	return snapshot, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision) (GraphSnapshot, error) {
	if err := requireProject(projectID); err != nil {
		return GraphSnapshot{}, err
	}
	if revision.IsLatestRead() {
		return GraphSnapshot{}, kernel.InvalidArgument("concrete revision is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GraphSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := loadSnapshot(ctx, tx, projectID, revision)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return GraphSnapshot{}, err
	}
	return snapshot, nil
}

func (s *PostgresStore) ReplacePending(ctx context.Context, projectID kernel.ProjectID, next PendingSubgraph) (GraphSnapshot, error) {
	if err := requireProject(projectID); err != nil {
		return GraphSnapshot{}, err
	}
	if kernel.IsZeroID(next.RequestID) {
		return GraphSnapshot{}, kernel.InvalidArgument("request_id is required")
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return GraphSnapshot{}, err
	}
	defer tx.Rollback()

	current, err := loadLatestSnapshot(ctx, tx, projectID, true)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if err := kernel.CheckExpectedRevision(next.BaseRevision, current.Revision); err != nil {
		return GraphSnapshot{}, err
	}
	currentState := stateFromSnapshot(current)
	if err := validatePendingSubgraph(currentState, next); err != nil {
		return GraphSnapshot{}, err
	}
	runtimeState, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if err := validatePendingSubgraphRuntime(runtimeState, next); err != nil {
		return GraphSnapshot{}, err
	}
	nextState := currentState.clone()
	applyPendingSubgraph(&nextState, next)
	if err := validateGraph(nextState); err != nil {
		return GraphSnapshot{}, err
	}
	updated := nextState.snapshot(current.Revision.Next())
	if err := persistGraphSnapshot(ctx, tx, projectID, updated, string(next.RequestID)); err != nil {
		return GraphSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return GraphSnapshot{}, mapPostgresError(err)
	}
	return updated, nil
}

func (s *PostgresStore) Transition(ctx context.Context, projectID kernel.ProjectID, expectedRevision kernel.Revision, transition GraphTransition) (GraphSnapshot, error) {
	if err := requireProject(projectID); err != nil {
		return GraphSnapshot{}, err
	}
	if err := validateTransitionShape(transition); err != nil {
		return GraphSnapshot{}, err
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return GraphSnapshot{}, err
	}
	defer tx.Rollback()

	current, err := loadLatestSnapshot(ctx, tx, projectID, true)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if err := kernel.CheckExpectedRevision(expectedRevision, current.Revision); err != nil {
		return GraphSnapshot{}, err
	}
	runtimeState, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return GraphSnapshot{}, err
	}
	if transition.TargetKind == TargetPhaseEndpoint && transition.Action == "released" {
		for _, lease := range runtimeState.leases {
			if lease.State == "active" && lease.Endpoint == transition.Endpoint {
				return GraphSnapshot{}, kernel.TransitionRejected("released transition requires the phase lease to be terminally released")
			}
		}
	}
	state := stateFromSnapshot(current).clone()
	if err := applyTransition(&state, transition); err != nil {
		return GraphSnapshot{}, err
	}
	if err := validateGraph(state); err != nil {
		return GraphSnapshot{}, err
	}
	updated := state.snapshot(current.Revision.Next())
	if err := persistGraphSnapshot(ctx, tx, projectID, updated, transitionDecisionRef(transition)); err != nil {
		return GraphSnapshot{}, err
	}
	if err := persistRuntimeBinding(ctx, tx, projectID, transition); err != nil {
		return GraphSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return GraphSnapshot{}, mapPostgresError(err)
	}
	return updated, nil
}

func (s *PostgresStore) RegisterReplacePending(ctx context.Context, projectID kernel.ProjectID, decisionRef kernel.IdempotencyKey) error {
	if err := requireProject(projectID); err != nil {
		return err
	}
	if kernel.IsZeroID(decisionRef) {
		return kernel.InvalidArgument("decision_ref is required")
	}
	return s.registerDecision(ctx, projectID, string(decisionRef), DecisionReplacePending, nil)
}

func (s *PostgresStore) RegisterTransition(ctx context.Context, projectID kernel.ProjectID, transitionRef string, transition GraphTransition) error {
	if err := requireProject(projectID); err != nil {
		return err
	}
	if transitionRef == "" {
		return kernel.InvalidArgument("transition_ref is required")
	}
	if err := validateTransitionShape(transition); err != nil {
		return err
	}
	payload, err := json.Marshal(transition)
	if err != nil {
		return kernel.InvalidArgument("transition payload must be JSON serializable")
	}
	return s.registerDecision(ctx, projectID, transitionRef, DecisionTransition, payload)
}

func (s *PostgresStore) AuthorizeReplacePending(ctx context.Context, projectID kernel.ProjectID, decisionRef kernel.IdempotencyKey) error {
	kind, _, err := s.loadDecision(ctx, projectID, string(decisionRef))
	if err != nil {
		if kernel.IsCode(err, kernel.CodeForbidden) {
			return kernel.Forbidden("replacePending requires a persisted Task Manager decision")
		}
		return err
	}
	if kind != DecisionReplacePending {
		return kernel.Forbidden("decision_ref is not a replacePending decision")
	}
	return nil
}

func (s *PostgresStore) ResolveTransition(ctx context.Context, projectID kernel.ProjectID, transitionRef string) (GraphTransition, error) {
	kind, payload, err := s.loadDecision(ctx, projectID, transitionRef)
	if err != nil {
		return GraphTransition{}, err
	}
	if kind != DecisionTransition {
		return GraphTransition{}, kernel.Forbidden("transition_ref is not a transition decision")
	}
	var transition GraphTransition
	if err := json.Unmarshal(payload, &transition); err != nil {
		return GraphTransition{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored transition payload is invalid", Recoverable: true}
	}
	return transition, nil
}

func (s *PostgresStore) loadRuntimeView(ctx context.Context, projectID kernel.ProjectID) (runtimeView, error) {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return runtimeView{}, err
	}
	defer tx.Rollback()
	snapshot, err := loadLatestSnapshot(ctx, tx, projectID, false)
	if err != nil {
		return runtimeView{}, err
	}
	runtimeState, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return runtimeView{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeView{}, err
	}
	return newRuntimeViewFrom(snapshot, runtimeState), nil
}

func (s *PostgresStore) markCommandObserved(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	return s.foldObservation(ctx, projectID, observation, false)
}

func (s *PostgresStore) completeCommandAndReleaseLease(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	return s.foldObservation(ctx, projectID, observation, true)
}

func (s *PostgresStore) releaseOrphanLease(ctx context.Context, projectID kernel.ProjectID, leaseRef kernel.LeaseID) error {
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE coordination_phase_leases SET state = 'released', released_at = now() WHERE project_id = $1 AND lease_ref = $2`, projectID, leaseRef)
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "lease not found", Recoverable: true}
	}
	return tx.Commit()
}

func (s *PostgresStore) getOrCreateStopCommand(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision, lease phaseLease) (PhaseCommand, bool, error) {
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return PhaseCommand{}, false, err
	}
	defer tx.Rollback()
	commandID := fmt.Sprintf("cmd:stop:%s", lease.LeaseRef)
	record, ok, err := loadCommand(ctx, tx, projectID, commandID)
	if err != nil {
		return PhaseCommand{}, false, err
	}
	if ok {
		if err := tx.Commit(); err != nil {
			return PhaseCommand{}, false, mapPostgresError(err)
		}
		deliverable := record.CompletedEventRef == "" && !record.Quarantined && !record.NotExecutable
		return record.Command, deliverable, nil
	}
	command := PhaseCommand{
		ID:         commandID,
		Endpoint:   lease.Endpoint,
		Generation: lease.Generation,
		BindingRef: lease.BindingRef,
		LeaseRef:   lease.LeaseRef,
		Action:     CommandStop,
		CauseRef:   fmt.Sprintf("revision://%d", revision),
	}
	if err := insertCommand(ctx, tx, projectID, command); err != nil {
		return PhaseCommand{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return PhaseCommand{}, false, mapPostgresError(err)
	}
	return command, true, nil
}

func (s *PostgresStore) claimLeaseAndAppendCommand(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision, endpoint PhaseEndpoint, action CommandAction, causeRef string) (PhaseCommand, bool, error) {
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return PhaseCommand{}, false, err
	}
	defer tx.Rollback()
	snapshot, err := loadLatestSnapshot(ctx, tx, projectID, true)
	if err != nil {
		return PhaseCommand{}, false, err
	}
	if err := kernel.CheckExpectedRevision(revision, snapshot.Revision); err != nil {
		return PhaseCommand{}, false, nil
	}
	runtimeState, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return PhaseCommand{}, false, err
	}
	view := newRuntimeViewFrom(snapshot, runtimeState)
	current, ok := view.endpoint(endpoint.Ref)
	if !ok || current.Generation != endpoint.Generation || !view.isRunnable(current) {
		return PhaseCommand{}, false, nil
	}
	leaseRef := kernel.LeaseID(fmt.Sprintf("lease:%s:%s:%d", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation))
	if lease, ok := runtimeState.leases[leaseRef]; ok && lease.State == "active" {
		if err := tx.Commit(); err != nil {
			return PhaseCommand{}, false, mapPostgresError(err)
		}
		return existingRunCommandFromState(runtimeState, endpoint.Ref, endpoint.Generation), false, nil
	}
	commandID := fmt.Sprintf("cmd:run:%s:%s:%d", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation)
	if record, ok := runtimeState.commands[commandID]; ok {
		if !isRunCommand(record.Command) {
			return PhaseCommand{}, false, kernel.Error{Code: kernel.CodeCommandConflict, Message: "run command id already has different content", Recoverable: false}
		}
		if err := tx.Commit(); err != nil {
			return PhaseCommand{}, false, mapPostgresError(err)
		}
		return record.Command, false, nil
	}
	command := PhaseCommand{
		ID:         commandID,
		Endpoint:   endpoint.Ref,
		Generation: endpoint.Generation,
		BindingRef: endpoint.BindingRef,
		LeaseRef:   leaseRef,
		Action:     action,
		CauseRef:   causeRef,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordination_phase_leases(project_id, lease_ref, task_id, endpoint_id, generation, binding_ref, state) VALUES ($1, $2, $3, $4, $5, $6, 'active')`, projectID, leaseRef, endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation, endpoint.BindingRef); err != nil {
		return PhaseCommand{}, false, mapPostgresError(err)
	}
	if err := insertCommand(ctx, tx, projectID, command); err != nil {
		return PhaseCommand{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return PhaseCommand{}, false, mapPostgresError(err)
	}
	return command, true, nil
}

func (s *PostgresStore) markCommandAccepted(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	_ = s.execBestEffort(ctx, `UPDATE coordination_phase_commands SET accepted_at = now(), retry_after = NULL, updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, commandID)
}

func (s *PostgresStore) scheduleRetry(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	_ = s.execBestEffort(ctx, `UPDATE coordination_phase_commands SET retry_after = now(), updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, commandID)
}

func (s *PostgresStore) quarantineCommand(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	_ = s.execBestEffort(ctx, `UPDATE coordination_phase_commands SET quarantined = true, updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, commandID)
}

func (s *PostgresStore) rejectCommand(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, err error) {
	tx, beginErr := s.begin(ctx, serializableTx())
	if beginErr != nil {
		return
	}
	defer tx.Rollback()
	record, ok, loadErr := loadCommand(ctx, tx, projectID, command.ID)
	if loadErr != nil || !ok || record.Command != command {
		return
	}
	eventID := fmt.Sprintf("dispatch-rejection:%s:%s", command.ID, kernel.ErrorCodeOf(err))
	_, _ = tx.ExecContext(ctx, `UPDATE coordination_phase_commands SET not_executable = true, retry_after = NULL, completed_event_ref = $3, updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, command.ID, eventID)
	if isRunCommand(command) && record.ObservedEventRef == "" {
		_, _ = tx.ExecContext(ctx, `UPDATE coordination_phase_leases SET state = 'released', released_at = now() WHERE project_id = $1 AND lease_ref = $2 AND state = 'active'`, projectID, command.LeaseRef)
	}
	_ = insertObservation(ctx, tx, projectID, phaseObservation{
		ID:             eventID,
		Kind:           "DispatchRejected",
		CommandID:      command.ID,
		Endpoint:       command.Endpoint,
		Generation:     command.Generation,
		BindingRef:     command.BindingRef,
		LeaseRef:       command.LeaseRef,
		DispatchTarget: command.Endpoint,
		DispatchError:  string(kernel.ErrorCodeOf(err)),
		Folded:         true,
	})
	_ = tx.Commit()
}

func (s *PostgresStore) recordEndpointDispatchRejection(ctx context.Context, projectID kernel.ProjectID, endpoint PhaseEndpointRef, generation int, bindingRef kernel.BindingRef, err error) {
	eventID := fmt.Sprintf("dispatch-rejection:%s:%s:%d:%s", endpoint.TaskID, endpoint.EndpointID, generation, kernel.ErrorCodeOf(err))
	tx, beginErr := s.begin(ctx, serializableTx())
	if beginErr != nil {
		return
	}
	defer tx.Rollback()
	_ = insertObservation(ctx, tx, projectID, phaseObservation{
		ID:             eventID,
		Kind:           "DispatchRejected",
		Endpoint:       endpoint,
		Generation:     generation,
		BindingRef:     bindingRef,
		DispatchTarget: endpoint,
		DispatchError:  string(kernel.ErrorCodeOf(err)),
		Folded:         true,
	})
	_ = tx.Commit()
}

func (s *PostgresStore) appendObservation(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if observation.ID == "" {
		return kernel.InvalidArgument("observation.id is required")
	}
	if observation.Kind != "DispatchRejected" {
		state, err := loadRuntimeState(ctx, tx, projectID)
		if err != nil {
			return err
		}
		record, ok := state.commands[observation.CommandID]
		if !ok {
			return commandError(kernel.CodeStaleCommand, "observation command does not exist")
		}
		project := &projectState{runtime: state}
		if _, err := matchingObservationLease(project, record.Command, observation); err != nil {
			return err
		}
	}
	if err := insertObservation(ctx, tx, projectID, observation); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) expireLease(ctx context.Context, projectID kernel.ProjectID, leaseRef kernel.LeaseID) error {
	return s.execBestEffort(ctx, `UPDATE coordination_phase_leases SET expires_at = now() - interval '1 second' WHERE project_id = $1 AND lease_ref = $2`, projectID, leaseRef)
}

func (s *PostgresStore) runtimeCommands(ctx context.Context, projectID kernel.ProjectID) []PhaseCommand {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	state, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return nil
	}
	commands := make([]PhaseCommand, 0, len(state.commands))
	for _, record := range state.commands {
		commands = append(commands, record.Command)
	}
	return commands
}

func (s *PostgresStore) runtimeLeases(ctx context.Context, projectID kernel.ProjectID) []phaseLease {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	state, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return nil
	}
	leases := make([]phaseLease, 0, len(state.leases))
	for _, lease := range state.leases {
		leases = append(leases, lease)
	}
	return leases
}

func (s *PostgresStore) foldObservation(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation, terminal bool) error {
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, err := loadRuntimeState(ctx, tx, projectID)
	if err != nil {
		return err
	}
	record, ok := state.commands[observation.CommandID]
	if !ok {
		return commandError(kernel.CodeStaleCommand, "observation command does not exist")
	}
	project := &projectState{runtime: state}
	lease, err := matchingObservationLease(project, record.Command, observation)
	if err != nil {
		return err
	}
	if terminal {
		if observation.Kind == "PhaseInvocationStopped" {
			if record.Command.Action != CommandStop {
				return commandError(kernel.CodeStaleCommand, "stopped observation must match a stop command")
			}
		} else if !isRunCommand(record.Command) {
			return commandError(kernel.CodeStaleCommand, "terminal execution observation must match a start or resume command")
		}
		_, err = tx.ExecContext(ctx, `UPDATE coordination_phase_commands SET completed_event_ref = $3, updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, observation.CommandID, observation.ID)
		if err != nil {
			return mapPostgresError(err)
		}
		_, err = tx.ExecContext(ctx, `UPDATE coordination_phase_leases SET state = 'released', released_at = now() WHERE project_id = $1 AND lease_ref = $2`, projectID, observation.LeaseRef)
		if err != nil {
			return mapPostgresError(err)
		}
	} else {
		if !isRunCommand(record.Command) {
			return commandError(kernel.CodeStaleCommand, "started observation must match a start or resume command")
		}
		if lease.State != "active" {
			return kernel.LeaseConflict("started observation lease is not active")
		}
		_, err = tx.ExecContext(ctx, `UPDATE coordination_phase_commands SET observed_event_ref = $3, updated_at = now() WHERE project_id = $1 AND command_id = $2`, projectID, observation.CommandID, observation.ID)
		if err != nil {
			return mapPostgresError(err)
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE coordination_runtime_observations SET folded = true WHERE project_id = $1 AND event_id = $2`, projectID, observation.ID)
	if err != nil {
		return mapPostgresError(err)
	}
	return tx.Commit()
}

func (s *PostgresStore) registerDecision(ctx context.Context, projectID kernel.ProjectID, ref string, kind DecisionKind, payload []byte) error {
	hash := payloadHash(payload)
	if payload == nil {
		payload = []byte("null")
	}
	result, err := s.exec(ctx, `INSERT INTO coordination_decisions(project_id, decision_ref, kind, payload_hash, transition_payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (project_id, decision_ref) DO UPDATE SET decision_ref = coordination_decisions.decision_ref
WHERE coordination_decisions.kind = EXCLUDED.kind AND coordination_decisions.payload_hash = EXCLUDED.payload_hash`, projectID, ref, kind, hash, string(payload))
	if err != nil {
		return mapPostgresError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func (s *PostgresStore) loadDecision(ctx context.Context, projectID kernel.ProjectID, ref string) (DecisionKind, []byte, error) {
	if err := requireProject(projectID); err != nil {
		return "", nil, err
	}
	if ref == "" {
		return "", nil, kernel.InvalidArgument("decision_ref is required")
	}
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", nil, err
	}
	defer tx.Rollback()
	var kind DecisionKind
	var payload sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT kind, transition_payload::text FROM coordination_decisions WHERE project_id = $1 AND decision_ref = $2`, projectID, ref).Scan(&kind, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, kernel.Forbidden("transition requires a persisted Task Manager decision")
	}
	if err != nil {
		return "", nil, mapPostgresError(err)
	}
	if err := tx.Commit(); err != nil {
		return "", nil, err
	}
	return kind, []byte(payload.String), nil
}

func (s *PostgresStore) execBestEffort(ctx context.Context, query string, args ...any) error {
	_, err := s.exec(ctx, query, args...)
	return err
}

func (s *PostgresStore) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx, err := s.begin(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, mapPostgresError(err)
	}
	return result, nil
}

func (s *PostgresStore) begin(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres store database is not configured", Recoverable: true}
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	return tx, nil
}

func loadLatestSnapshot(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, lock bool) (GraphSnapshot, error) {
	query := `SELECT revision, snapshot::text FROM coordination_graph_revisions WHERE project_id = $1 ORDER BY revision DESC LIMIT 1`
	if lock {
		query = `SELECT revision, snapshot::text FROM coordination_graph_revisions WHERE project_id = $1 ORDER BY revision DESC LIMIT 1 FOR UPDATE`
	}
	var revision kernel.Revision
	var raw string
	err := q.QueryRowContext(ctx, query, projectID).Scan(&revision, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return newGraphState().snapshot(1), nil
	}
	if err != nil {
		return GraphSnapshot{}, mapPostgresError(err)
	}
	snapshot, err := decodeSnapshot(revision, []byte(raw))
	if err != nil {
		return GraphSnapshot{}, err
	}
	return snapshot, nil
}

func loadSnapshot(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, revision kernel.Revision) (GraphSnapshot, error) {
	if revision == 1 {
		var exists int
		err := q.QueryRowContext(ctx, `SELECT 1 FROM coordination_graph_revisions WHERE project_id = $1 LIMIT 1`, projectID).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return newGraphState().snapshot(1), nil
		}
		if err != nil {
			return GraphSnapshot{}, mapPostgresError(err)
		}
	}
	var raw string
	err := q.QueryRowContext(ctx, `SELECT snapshot::text FROM coordination_graph_revisions WHERE project_id = $1 AND revision = $2`, projectID, revision).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return GraphSnapshot{}, kernel.Error{Code: kernel.CodeNotFound, Message: fmt.Sprintf("graph revision %d not found", revision), Recoverable: false}
	}
	if err != nil {
		return GraphSnapshot{}, mapPostgresError(err)
	}
	return decodeSnapshot(revision, []byte(raw))
}

func persistGraphSnapshot(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, snapshot GraphSnapshot, decisionRef string) error {
	if err := writeGraphTables(ctx, q, projectID, snapshot); err != nil {
		return err
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return kernel.InvalidArgument("graph snapshot must be JSON serializable")
	}
	_, err = q.ExecContext(ctx, `INSERT INTO coordination_graph_revisions(project_id, revision, snapshot, decision_ref) VALUES ($1, $2, $3::jsonb, $4)`, projectID, snapshot.Revision, string(raw), decisionRef)
	return mapPostgresError(err)
}

func writeGraphTables(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, snapshot GraphSnapshot) error {
	for _, stmt := range []string{
		`DELETE FROM coordination_edges WHERE project_id = $1`,
		`DELETE FROM coordination_blockers WHERE project_id = $1`,
		`DELETE FROM coordination_phase_results WHERE project_id = $1`,
	} {
		if _, err := q.ExecContext(ctx, stmt, projectID); err != nil {
			return mapPostgresError(err)
		}
	}
	for _, task := range snapshot.Tasks {
		if _, err := q.ExecContext(ctx, `INSERT INTO coordination_tasks(project_id, task_id, contract_ref, outcome)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project_id, task_id) DO UPDATE SET
	contract_ref = EXCLUDED.contract_ref,
	outcome = EXCLUDED.outcome,
	updated_at = now()`, projectID, task.ID, task.ContractRef, task.Outcome); err != nil {
			return mapPostgresError(err)
		}
	}
	for _, endpoint := range snapshot.Endpoints {
		if _, err := q.ExecContext(ctx, `INSERT INTO coordination_endpoints(project_id, task_id, endpoint_id, spec_ref, binding_ref, generation, state, run_policy)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (project_id, task_id, endpoint_id) DO UPDATE SET
	spec_ref = EXCLUDED.spec_ref,
	binding_ref = EXCLUDED.binding_ref,
	generation = EXCLUDED.generation,
	state = EXCLUDED.state,
	run_policy = EXCLUDED.run_policy,
	updated_at = now()`, projectID, endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.SpecRef, endpoint.BindingRef, endpoint.Generation, endpoint.State, endpoint.RunPolicy); err != nil {
			return mapPostgresError(err)
		}
	}
	for _, edge := range snapshot.Edges {
		if _, err := q.ExecContext(ctx, `INSERT INTO coordination_edges(project_id, from_task_id, from_endpoint_id, to_task_id, to_endpoint_id, signal, required_by, artifact_kinds, on_false) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text[], $9)`, projectID, edge.From.TaskID, edge.From.EndpointID, edge.To.TaskID, edge.To.EndpointID, edge.Signal, edge.RequiredBy, textArrayLiteral(edge.ArtifactKinds), edge.OnFalse); err != nil {
			return mapPostgresError(err)
		}
	}
	for _, blocker := range snapshot.Blockers {
		if _, err := q.ExecContext(ctx, `INSERT INTO coordination_blockers(project_id, blocker_id, target_task_id, target_endpoint_id, required_by, on_false, state) VALUES ($1, $2, $3, $4, $5, $6, $7)`, projectID, blocker.ID, blocker.Target.TaskID, blocker.Target.EndpointID, blocker.RequiredBy, blocker.OnFalse, blocker.State); err != nil {
			return mapPostgresError(err)
		}
	}
	for _, result := range snapshot.Results {
		if _, err := q.ExecContext(ctx, `INSERT INTO coordination_phase_results(project_id, result_id, task_id, endpoint_id, binding_ref, output_ref, verdict) VALUES ($1, $2, $3, $4, $5, $6, $7)`, projectID, result.ID, result.Endpoint.TaskID, result.Endpoint.EndpointID, result.BindingRef, result.OutputRef, result.Verdict); err != nil {
			return mapPostgresError(err)
		}
	}
	return nil
}

func loadRuntimeState(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID) (memoryRuntimeState, error) {
	state := newMemoryRuntimeState()
	rows, err := q.QueryContext(ctx, `SELECT command_id, task_id, endpoint_id, generation, binding_ref, lease_ref, action, cause_ref, accepted_at IS NOT NULL, observed_event_ref, completed_event_ref, retry_after IS NOT NULL, quarantined, not_executable FROM coordination_phase_commands WHERE project_id = $1`, projectID)
	if err != nil {
		return state, mapPostgresError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var record commandRecord
		var observed, completed sql.NullString
		if err := rows.Scan(&record.Command.ID, &record.Command.Endpoint.TaskID, &record.Command.Endpoint.EndpointID, &record.Command.Generation, &record.Command.BindingRef, &record.Command.LeaseRef, &record.Command.Action, &record.Command.CauseRef, &record.Accepted, &observed, &completed, &record.RetryScheduled, &record.Quarantined, &record.NotExecutable); err != nil {
			return state, mapPostgresError(err)
		}
		record.ObservedEventRef = observed.String
		record.CompletedEventRef = completed.String
		state.commands[record.Command.ID] = record
	}
	if err := rows.Err(); err != nil {
		return state, mapPostgresError(err)
	}
	rows, err = q.QueryContext(ctx, `SELECT lease_ref, task_id, endpoint_id, generation, binding_ref, state, expires_at IS NOT NULL AND expires_at <= now() FROM coordination_phase_leases WHERE project_id = $1`, projectID)
	if err != nil {
		return state, mapPostgresError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var lease phaseLease
		if err := rows.Scan(&lease.LeaseRef, &lease.Endpoint.TaskID, &lease.Endpoint.EndpointID, &lease.Generation, &lease.BindingRef, &lease.State, &lease.Expired); err != nil {
			return state, mapPostgresError(err)
		}
		state.leases[lease.LeaseRef] = lease
	}
	if err := rows.Err(); err != nil {
		return state, mapPostgresError(err)
	}
	rows, err = q.QueryContext(ctx, `SELECT event_id, COALESCE(command_id, ''), COALESCE(lease_ref, ''), task_id, endpoint_id, generation, binding_ref, kind, COALESCE(checkpoint_ref, ''), non_resumable, folded FROM coordination_runtime_observations WHERE project_id = $1 ORDER BY created_at, event_id`, projectID)
	if err != nil {
		return state, mapPostgresError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var observation phaseObservation
		if err := rows.Scan(&observation.ID, &observation.CommandID, &observation.LeaseRef, &observation.Endpoint.TaskID, &observation.Endpoint.EndpointID, &observation.Generation, &observation.BindingRef, &observation.Kind, &observation.CheckpointRef, &observation.NonResumable, &observation.Folded); err != nil {
			return state, mapPostgresError(err)
		}
		state.observations = append(state.observations, observation)
	}
	if err := rows.Err(); err != nil {
		return state, mapPostgresError(err)
	}
	rows, err = q.QueryContext(ctx, `SELECT binding_ref, COALESCE(checkpoint_ref, ''), non_resumable FROM coordination_binding_runtime WHERE project_id = $1`, projectID)
	if err != nil {
		return state, mapPostgresError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref kernel.BindingRef
		var info bindingRuntimeInfo
		if err := rows.Scan(&ref, &info.CheckpointRef, &info.NonResumable); err != nil {
			return state, mapPostgresError(err)
		}
		state.bindings[ref] = info
	}
	return state, mapPostgresError(rows.Err())
}

func loadCommand(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, commandID string) (commandRecord, bool, error) {
	state, err := loadRuntimeState(ctx, q, projectID)
	if err != nil {
		return commandRecord{}, false, err
	}
	record, ok := state.commands[commandID]
	return record, ok, nil
}

func insertCommand(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, command PhaseCommand) error {
	_, err := q.ExecContext(ctx, `INSERT INTO coordination_phase_commands(project_id, command_id, task_id, endpoint_id, generation, binding_ref, lease_ref, action, cause_ref) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, projectID, command.ID, command.Endpoint.TaskID, command.Endpoint.EndpointID, command.Generation, command.BindingRef, command.LeaseRef, command.Action, command.CauseRef)
	return mapPostgresError(err)
}

func insertObservation(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, observation phaseObservation) error {
	_, err := q.ExecContext(ctx, `INSERT INTO coordination_runtime_observations(project_id, event_id, command_id, lease_ref, task_id, endpoint_id, generation, binding_ref, kind, checkpoint_ref, non_resumable, folded)
VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, $8, $9, NULLIF($10, ''), $11, $12)`, projectID, observation.ID, observation.CommandID, observation.LeaseRef, observation.Endpoint.TaskID, observation.Endpoint.EndpointID, observation.Generation, observation.BindingRef, observation.Kind, observation.CheckpointRef, observation.NonResumable, observation.Folded)
	return mapPostgresError(err)
}

func persistRuntimeBinding(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, transition GraphTransition) error {
	if transition.TargetKind != TargetPhaseEndpoint || transition.NewBindingRef == "" {
		return nil
	}
	if transition.Action != "stopped" && transition.Action != "reopened" {
		return nil
	}
	_, err := q.ExecContext(ctx, `INSERT INTO coordination_binding_runtime(project_id, binding_ref, checkpoint_ref, non_resumable) VALUES ($1, $2, NULLIF($3, ''), $4)
ON CONFLICT (project_id, binding_ref) DO UPDATE SET checkpoint_ref = EXCLUDED.checkpoint_ref, non_resumable = EXCLUDED.non_resumable`, projectID, transition.NewBindingRef, transition.CheckpointRef, transition.NonResumable)
	return mapPostgresError(err)
}

func decodeSnapshot(revision kernel.Revision, raw []byte) (GraphSnapshot, error) {
	var snapshot GraphSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return GraphSnapshot{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored graph snapshot is invalid", Recoverable: true}
	}
	snapshot.Revision = revision
	return snapshot, nil
}

func stateFromSnapshot(snapshot GraphSnapshot) graphState {
	state := newGraphState()
	for _, task := range snapshot.Tasks {
		state.tasks[task.ID] = task
	}
	for _, endpoint := range snapshot.Endpoints {
		state.endpoints[endpoint.Ref] = endpoint
	}
	state.edges = cloneEdges(snapshot.Edges)
	for _, blocker := range snapshot.Blockers {
		state.blockers[blocker.ID] = blocker
	}
	for _, result := range snapshot.Results {
		state.results[result.ID] = result
	}
	return state
}

func existingRunCommandFromState(state memoryRuntimeState, ref PhaseEndpointRef, generation int) PhaseCommand {
	for _, record := range state.commands {
		if record.Command.Endpoint == ref && record.Command.Generation == generation && isRunCommand(record.Command) {
			return record.Command
		}
	}
	return PhaseCommand{}
}

func serializableTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelSerializable}
}

func requireProject(projectID kernel.ProjectID) error {
	if kernel.IsZeroID(projectID) {
		return kernel.InvalidArgument("project_id is required")
	}
	return nil
}

func transitionDecisionRef(transition GraphTransition) string {
	raw, err := json.Marshal(transition)
	if err != nil {
		return "transition"
	}
	return "transition:" + payloadHash(raw)
}

func payloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func textArrayLiteral(values []string) string {
	if len(values) == 0 {
		return "{}"
	}
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
	}
	return "{" + strings.Join(quoted, ",") + "}"
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
			if strings.Contains(pgErr.ConstraintName, "graph_revisions") {
				return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "graph revision was concurrently committed", Recoverable: true}
			}
			if strings.Contains(pgErr.ConstraintName, "lease") {
				return kernel.LeaseConflict("phase lease already exists")
			}
			return kernel.Error{Code: kernel.CodeCommandConflict, Message: "unique coordination record already exists", Recoverable: true}
		case "23503":
			return kernel.InvalidGraph("coordination graph references missing rows")
		case "40001":
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "serialization conflict while updating graph", Recoverable: true}
		}
	}
	if strings.Contains(err.Error(), "violates unique constraint") {
		return kernel.Error{Code: kernel.CodeCommandConflict, Message: "unique coordination record already exists", Recoverable: true}
	}
	return err
}

var _ Store = (*PostgresStore)(nil)
var _ DecisionLog = (*PostgresStore)(nil)
var _ graphRuntimeStore = (*PostgresStore)(nil)
