package evidence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/outbox"
	"github.com/jackc/pgx/v5/pgconn"
)

type postgresBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type postgresDBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type PostgresEventStore struct {
	db              postgresBeginner
	maxPayloadBytes int
}

func NewPostgresEventStore(db postgresBeginner, maxPayloadBytes int) *PostgresEventStore {
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = 64 * 1024
	}
	return &PostgresEventStore{db: db, maxPayloadBytes: maxPayloadBytes}
}

func (s *PostgresEventStore) Append(ctx context.Context, appendEvent AppendEvent) (Event, error) {
	return s.AppendWithOutbox(ctx, appendEvent, nil)
}

func (s *PostgresEventStore) AppendWithOutbox(ctx context.Context, appendEvent AppendEvent, outboxEvents []outbox.Event) (Event, error) {
	if s == nil || s.db == nil {
		return Event{}, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres event store is not configured", Recoverable: true}
	}
	if appendEvent.Type == "" {
		return Event{}, kernel.InvalidArgument("event type is required")
	}
	if appendEvent.StableKey == "" {
		return Event{}, kernel.InvalidArgument("stable event key is required")
	}
	payload, err := encodePayload(appendEvent.Payload, s.maxPayloadBytes)
	if err != nil {
		return Event{}, err
	}
	requestHash, err := canonicalEventRequestHash(appendEvent, payload)
	if err != nil {
		return Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, serializableTx())
	if err != nil {
		return Event{}, mapPostgresEvidenceError(err)
	}
	defer tx.Rollback()

	event, inserted, err := insertEvent(ctx, tx, appendEvent, payload, requestHash)
	if err != nil {
		return Event{}, err
	}
	if inserted {
		for _, event := range outboxEvents {
			if err := outbox.EnqueueTx(ctx, tx, event); err != nil {
				return Event{}, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return Event{}, mapPostgresEvidenceError(err)
	}
	return event, nil
}

func (s *PostgresEventStore) Replay(ctx context.Context, after Cursor, limit int) ([]Event, Cursor, error) {
	if s == nil || s.db == nil {
		return nil, after, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres event store is not configured", Recoverable: true}
	}
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	defer tx.Rollback()
	events, next, err := replayEvents(ctx, tx, after, limit)
	if err != nil {
		return nil, after, err
	}
	if err := tx.Commit(); err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	return events, next, nil
}

func (s *PostgresEventStore) ReplayTask(ctx context.Context, projectID kernel.ProjectID, taskID kernel.TaskID, after Cursor, limit int) ([]Event, Cursor, error) {
	if s == nil || s.db == nil {
		return nil, after, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres event store is not configured", Recoverable: true}
	}
	if projectID == "" {
		return nil, after, kernel.InvalidArgument("project_id is required")
	}
	if taskID == "" {
		return nil, after, kernel.InvalidArgument("task_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	defer tx.Rollback()
	events, next, err := replayTaskEvents(ctx, tx, projectID, taskID, after, limit)
	if err != nil {
		return nil, after, err
	}
	if err := tx.Commit(); err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	return events, next, nil
}

func insertEvent(ctx context.Context, q postgresDBTX, appendEvent AppendEvent, payload json.RawMessage, requestHash string) (Event, bool, error) {
	id := EventID("evt_" + requestHash[:20])
	artifactRefs := make([]string, len(appendEvent.ArtifactRefs))
	for i, ref := range appendEvent.ArtifactRefs {
		artifactRefs[i] = string(ref)
	}
	payloadArg := any(nil)
	if len(payload) > 0 {
		payloadArg = string(payload)
	}

	var event Event
	var payloadText string
	var refsJSON string
	var storedHash string
	var inserted bool
	err := q.QueryRowContext(ctx, `
WITH inserted(
	id, sequence, stable_key, type, project_id, task_id, workspace_ref,
	phase_endpoint, agent_invocation_id, payload, artifact_refs_json, graph_revision,
	created_at, request_hash, inserted
) AS (
	INSERT INTO evidence_events (
		id, stable_key, request_hash, type, project_id, task_id, workspace_ref,
		phase_endpoint, agent_invocation_id, payload, artifact_refs, graph_revision
	)
	VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), $10::jsonb, $11::text[], NULLIF($12, 0))
	ON CONFLICT (stable_key) DO NOTHING
	RETURNING id, sequence, stable_key, type, COALESCE(project_id, ''), COALESCE(task_id, ''),
		COALESCE(workspace_ref, ''), COALESCE(phase_endpoint, ''), COALESCE(agent_invocation_id, ''),
		COALESCE(payload::text, ''), COALESCE(array_to_json(artifact_refs)::text, '[]'),
		COALESCE(graph_revision, 0), created_at, request_hash, true
)
SELECT id, sequence, stable_key, type, COALESCE(project_id, ''), COALESCE(task_id, ''),
	COALESCE(workspace_ref, ''), COALESCE(phase_endpoint, ''), COALESCE(agent_invocation_id, ''),
	COALESCE(payload::text, ''), artifact_refs_json,
	COALESCE(graph_revision, 0), created_at, request_hash, true
FROM inserted
UNION ALL
SELECT id, sequence, stable_key, type, COALESCE(project_id, ''), COALESCE(task_id, ''),
	COALESCE(workspace_ref, ''), COALESCE(phase_endpoint, ''), COALESCE(agent_invocation_id, ''),
	COALESCE(payload::text, ''), COALESCE(array_to_json(artifact_refs)::text, '[]'),
	COALESCE(graph_revision, 0), created_at, request_hash, false
FROM evidence_events
WHERE stable_key = $2
LIMIT 1`,
		id, appendEvent.StableKey, requestHash, appendEvent.Type, appendEvent.ProjectID, appendEvent.TaskID,
		appendEvent.WorkspaceRef, appendEvent.PhaseEndpoint, appendEvent.AgentInvocationID, payloadArg,
		textArrayLiteral(artifactRefs), appendEvent.GraphRevision,
	).Scan(&event.ID, &event.Sequence, &event.StableKey, &event.Type, &event.ProjectID, &event.TaskID,
		&event.WorkspaceRef, &event.PhaseEndpoint, &event.AgentInvocationID, &payloadText, &refsJSON,
		&event.GraphRevision, &event.CreatedAt, &storedHash, &inserted)
	if err != nil {
		return Event{}, false, mapPostgresEvidenceError(err)
	}
	if !inserted {
		if storedHash != requestHash {
			return Event{}, false, kernel.IdempotencyConflict()
		}
	}
	if payloadText != "" {
		event.Payload = json.RawMessage(payloadText)
	}
	if len(event.Payload) == 0 {
		event.Payload = nil
	}
	if err := json.Unmarshal([]byte(refsJSON), &event.ArtifactRefs); err != nil {
		return Event{}, false, kernel.Error{Code: kernel.CodeInternalError, Message: "stored event artifact refs are invalid", Recoverable: true}
	}
	event.CreatedAt = event.CreatedAt.UTC()
	return cloneEvent(event), inserted, nil
}

func replayEvents(ctx context.Context, q postgresDBTX, after Cursor, limit int) ([]Event, Cursor, error) {
	rows, err := q.QueryContext(ctx, `
SELECT id, sequence, stable_key, type, COALESCE(project_id, ''), COALESCE(task_id, ''),
	COALESCE(workspace_ref, ''), COALESCE(phase_endpoint, ''), COALESCE(agent_invocation_id, ''),
	COALESCE(payload::text, ''), COALESCE(array_to_json(artifact_refs)::text, '[]'),
	COALESCE(graph_revision, 0), created_at
FROM evidence_events
WHERE sequence > $1
ORDER BY sequence
LIMIT $2`, after, limit)
	if err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	defer rows.Close()

	var events []Event
	next := after
	for rows.Next() {
		var event Event
		var payloadText string
		var refsJSON string
		if err := rows.Scan(&event.ID, &event.Sequence, &event.StableKey, &event.Type, &event.ProjectID, &event.TaskID,
			&event.WorkspaceRef, &event.PhaseEndpoint, &event.AgentInvocationID, &payloadText, &refsJSON,
			&event.GraphRevision, &event.CreatedAt); err != nil {
			return nil, after, mapPostgresEvidenceError(err)
		}
		if payloadText != "" {
			event.Payload = json.RawMessage(payloadText)
		}
		if len(event.Payload) == 0 {
			event.Payload = nil
		}
		if err := json.Unmarshal([]byte(refsJSON), &event.ArtifactRefs); err != nil {
			return nil, after, kernel.Error{Code: kernel.CodeInternalError, Message: "stored event artifact refs are invalid", Recoverable: true}
		}
		event.CreatedAt = event.CreatedAt.UTC()
		next = event.Sequence
		events = append(events, cloneEvent(event))
	}
	if err := rows.Err(); err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	return events, next, nil
}

func replayTaskEvents(ctx context.Context, q postgresDBTX, projectID kernel.ProjectID, taskID kernel.TaskID, after Cursor, limit int) ([]Event, Cursor, error) {
	rows, err := q.QueryContext(ctx, `
SELECT id, sequence, stable_key, type, COALESCE(project_id, ''), COALESCE(task_id, ''),
	COALESCE(workspace_ref, ''), COALESCE(phase_endpoint, ''), COALESCE(agent_invocation_id, ''),
	COALESCE(payload::text, ''), COALESCE(array_to_json(artifact_refs)::text, '[]'),
	COALESCE(graph_revision, 0), created_at
FROM evidence_events
WHERE project_id = $1 AND task_id = $2 AND sequence > $3
ORDER BY sequence
LIMIT $4`, projectID, taskID, after, limit)
	if err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	defer rows.Close()
	return scanEventRows(rows, after)
}

func scanEventRows(rows *sql.Rows, after Cursor) ([]Event, Cursor, error) {
	var events []Event
	next := after
	for rows.Next() {
		var event Event
		var payloadText string
		var refsJSON string
		if err := rows.Scan(&event.ID, &event.Sequence, &event.StableKey, &event.Type, &event.ProjectID, &event.TaskID,
			&event.WorkspaceRef, &event.PhaseEndpoint, &event.AgentInvocationID, &payloadText, &refsJSON,
			&event.GraphRevision, &event.CreatedAt); err != nil {
			return nil, after, mapPostgresEvidenceError(err)
		}
		if payloadText != "" {
			event.Payload = json.RawMessage(payloadText)
		}
		if len(event.Payload) == 0 {
			event.Payload = nil
		}
		if err := json.Unmarshal([]byte(refsJSON), &event.ArtifactRefs); err != nil {
			return nil, after, kernel.Error{Code: kernel.CodeInternalError, Message: "stored event artifact refs are invalid", Recoverable: true}
		}
		event.CreatedAt = event.CreatedAt.UTC()
		next = event.Sequence
		events = append(events, cloneEvent(event))
	}
	if err := rows.Err(); err != nil {
		return nil, after, mapPostgresEvidenceError(err)
	}
	return events, next, nil
}

type PostgresArtifactRegistry struct {
	db     postgresBeginner
	bucket string
	store  objectstore.Store
	now    func() time.Time
}

func NewPostgresArtifactRegistry(db postgresBeginner, store objectstore.Store, bucket string) *PostgresArtifactRegistry {
	if bucket == "" {
		bucket = "artifacts"
	}
	return &PostgresArtifactRegistry{db: db, store: store, bucket: bucket, now: time.Now}
}

func (r *PostgresArtifactRegistry) Register(ctx context.Context, req RegisterArtifact) (Artifact, error) {
	if r == nil || r.db == nil || r.store == nil {
		return Artifact{}, kernel.InvalidArgument("artifact store is required")
	}
	if req.Type == "" {
		return Artifact{}, kernel.InvalidArgument("artifact type is required")
	}
	if req.ProjectID == "" {
		return Artifact{}, kernel.InvalidArgument("project_id is required")
	}
	if req.TaskID == "" {
		return Artifact{}, kernel.InvalidArgument("task_id is required")
	}
	if err := rejectSensitivePath(req.Path); err != nil {
		return Artifact{}, err
	}
	if err := rejectSensitiveContent(req.Body); err != nil {
		return Artifact{}, err
	}
	hash := hashBytes(req.Body)
	id := artifactID(req.Type, hash)
	key := path.Join(string(req.Type), hash)
	put, err := r.store.Put(ctx, objectstore.PutObject{
		Bucket:      r.bucket,
		Key:         key,
		ContentType: req.ContentType,
		Body:        bytes.NewReader(req.Body),
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("put artifact object: %w", err)
	}
	if put.SHA256 != hash {
		return Artifact{}, kernel.InvalidArgument("artifact hash mismatch after write")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Artifact{}, mapPostgresEvidenceError(err)
	}
	defer tx.Rollback()
	artifact, err := r.upsertArtifactMetadata(ctx, tx, req, id, hash, put)
	if err != nil {
		return Artifact{}, err
	}
	if err := tx.Commit(); err != nil {
		return Artifact{}, mapPostgresEvidenceError(err)
	}
	return artifact, nil
}

func (r *PostgresArtifactRegistry) Open(ctx context.Context, principal Principal, id ArtifactID) (Artifact, []byte, error) {
	if !r.CanRead(principal, id) {
		return Artifact{}, nil, kernel.Forbidden("artifact is not readable by principal")
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Artifact{}, nil, mapPostgresEvidenceError(err)
	}
	defer tx.Rollback()
	artifact, ok, err := loadArtifact(ctx, tx, id)
	if err != nil {
		return Artifact{}, nil, err
	}
	if !ok {
		return Artifact{}, nil, kernel.Error{Code: kernel.CodeNotFound, Message: "artifact not found"}
	}
	if err := tx.Commit(); err != nil {
		return Artifact{}, nil, mapPostgresEvidenceError(err)
	}
	ref, err := parseBlobRef(artifact.PathOrBlobRef)
	if err != nil {
		return Artifact{}, nil, err
	}
	got, err := r.store.Get(ctx, ref)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("get artifact object: %w", err)
	}
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("read artifact object: %w", err)
	}
	if hashBytes(body) != artifact.ContentHash || got.SHA256 != artifact.ContentHash {
		return Artifact{}, nil, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "artifact hash verification failed"}
	}
	return artifact, body, nil
}

func (r *PostgresArtifactRegistry) CanRead(principal Principal, id ArtifactID) bool {
	if r == nil || r.db == nil || requirePrincipal(principal) != nil {
		return false
	}
	tx, err := r.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false
	}
	defer tx.Rollback()
	var artifactType ArtifactType
	var granted bool
	err = tx.QueryRowContext(context.Background(), `
SELECT a.type, EXISTS (
	SELECT 1 FROM evidence_artifact_grants g
	WHERE g.artifact_id = a.id AND g.project_id = $2 AND g.task_id = $3
)
FROM evidence_artifacts a
WHERE a.id = $1`, id, principal.ProjectID, principal.TaskID).Scan(&artifactType, &granted)
	if err != nil {
		return false
	}
	if artifactType == ArtifactAgentTranscript && principal.Role == RoleTaskManager {
		return false
	}
	return granted
}

func (r *PostgresArtifactRegistry) upsertArtifactMetadata(ctx context.Context, tx postgresDBTX, req RegisterArtifact, id ArtifactID, hash string, put objectstore.PutResult) (Artifact, error) {
	blobRef := put.Bucket + "/" + put.Key
	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var artifact Artifact
	err := tx.QueryRowContext(ctx, `
INSERT INTO evidence_artifacts (
	id, type, path_or_blob_ref, content_hash, size_bytes, project_id, task_id,
	agent_invocation_id, content_type
)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9)
ON CONFLICT (type, content_hash) DO UPDATE SET content_hash = evidence_artifacts.content_hash
RETURNING id, type, path_or_blob_ref, content_hash, size_bytes, COALESCE(project_id, ''),
	COALESCE(task_id, ''), COALESCE(agent_invocation_id, ''), created_at`,
		id, req.Type, blobRef, hash, put.Size, req.ProjectID, req.TaskID, req.AgentInvocationID, contentType,
	).Scan(&artifact.ID, &artifact.Type, &artifact.PathOrBlobRef, &artifact.ContentHash, &artifact.Size,
		&artifact.ProjectID, &artifact.TaskID, &artifact.AgentInvocationID, &artifact.CreatedAt)
	if err != nil {
		return Artifact{}, mapPostgresEvidenceError(err)
	}
	if artifact.ContentHash != hash || artifact.Size != put.Size {
		return Artifact{}, kernel.IdempotencyConflict()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO evidence_artifact_grants(artifact_id, project_id, task_id)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING`, artifact.ID, req.ProjectID, req.TaskID); err != nil {
		return Artifact{}, mapPostgresEvidenceError(err)
	}
	artifact.CreatedAt = artifact.CreatedAt.UTC()
	return artifact, nil
}

func loadArtifact(ctx context.Context, q postgresDBTX, id ArtifactID) (Artifact, bool, error) {
	var artifact Artifact
	err := q.QueryRowContext(ctx, `
SELECT id, type, path_or_blob_ref, content_hash, size_bytes, COALESCE(project_id, ''),
	COALESCE(task_id, ''), COALESCE(agent_invocation_id, ''), created_at
FROM evidence_artifacts
WHERE id = $1`, id).Scan(&artifact.ID, &artifact.Type, &artifact.PathOrBlobRef, &artifact.ContentHash,
		&artifact.Size, &artifact.ProjectID, &artifact.TaskID, &artifact.AgentInvocationID, &artifact.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Artifact{}, false, nil
	}
	if err != nil {
		return Artifact{}, false, mapPostgresEvidenceError(err)
	}
	artifact.CreatedAt = artifact.CreatedAt.UTC()
	return artifact, true, nil
}

func serializableTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelSerializable}
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

func mapPostgresEvidenceError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return kernel.IdempotencyConflict()
		case "40001":
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "serialization conflict while updating evidence", Recoverable: true}
		}
	}
	return err
}

var _ EventStoreWithOutbox = (*PostgresEventStore)(nil)
var _ ArtifactStore = (*PostgresArtifactRegistry)(nil)
