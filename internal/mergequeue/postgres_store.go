package mergequeue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
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

type PostgresStore struct {
	db       postgresBeginner
	ownerID  string
	claimTTL time.Duration
	now      func() time.Time
}

func NewPostgresStore(db postgresBeginner) *PostgresStore {
	return &PostgresStore{
		db:       db,
		ownerID:  newMergeQueueOwnerID(),
		claimTTL: 5 * time.Minute,
		now:      time.Now,
	}
}

func (s *PostgresStore) SetClaimTTL(ttl time.Duration) {
	if ttl > 0 {
		s.claimTTL = ttl
	}
}

func (s *PostgresStore) enqueue(ctx context.Context, req EnqueueRequest, audit auditRecord) (Candidate, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, err
	}
	normalizedTarget, err := normalizeTargetRepository(req.TargetRepository)
	if err != nil {
		return Candidate{}, err
	}
	req.TargetRepository = normalizedTarget
	if req.TargetBranch == "" {
		req.TargetBranch = "main"
	}
	if err := validateEnqueue(req); err != nil {
		return Candidate{}, err
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return Candidate{}, err
	}
	defer tx.Rollback()

	now := s.now().UTC()
	refs := dedupeEvidence(append([]evidence.ArtifactID{req.VerifyResultRef, req.DiffArtifactRef}, req.EvidenceRefs...))
	result, err := tx.ExecContext(ctx, `
INSERT INTO merge_candidates(
	id, project_id, task_id, workspace_ref, verify_result_ref, diff_artifact_ref,
	target_repository, target_branch, base_revision, main_revision, candidate_revision,
	status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'queued', $12, $12)
ON CONFLICT (id) DO NOTHING`,
		req.ID, req.ProjectID, req.TaskID, req.WorkspaceRef, req.VerifyResultRef, req.DiffArtifactRef,
		req.TargetRepository, req.TargetBranch, req.BaseRevision, req.MainRevision, req.CandidateRevision, now)
	if err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	if affected(result) == 1 {
		if err := insertEvidenceRefs(ctx, tx, req.ID, refs); err != nil {
			return Candidate{}, err
		}
		if err := appendAudit(ctx, tx, audit); err != nil {
			return Candidate{}, err
		}
		if err := tx.Commit(); err != nil {
			return Candidate{}, mapPostgresMergeQueueError(err)
		}
		return s.Get(ctx, req.ID)
	}
	existing, err := loadCandidate(ctx, tx, req.ID)
	if err != nil {
		return Candidate{}, err
	}
	if !sameEnqueue(existing, req) {
		return Candidate{}, kernel.IdempotencyConflict()
	}
	if err := appendAudit(ctx, tx, audit); err != nil {
		return Candidate{}, err
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	return existing, nil
}

func (s *PostgresStore) Get(ctx context.Context, id CandidateID) (Candidate, error) {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Candidate{}, err
	}
	defer tx.Rollback()
	candidate, err := loadCandidate(ctx, tx, id)
	if err != nil {
		return Candidate{}, err
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	return candidate, nil
}

func (s *PostgresStore) ClaimNext(ctx context.Context, targetRepository string) (Candidate, bool, error) {
	if err := ctx.Err(); err != nil {
		return Candidate{}, false, err
	}
	normalizedTarget, err := normalizeTargetRepository(targetRepository)
	if err != nil {
		return Candidate{}, false, err
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return Candidate{}, false, err
	}
	defer tx.Rollback()
	var id CandidateID
	claimMillis := s.claimTTL.Milliseconds()
	if claimMillis <= 0 {
		claimMillis = 1
	}
	err = tx.QueryRowContext(ctx, `
WITH next_candidate AS (
	SELECT id
	FROM merge_candidates
	WHERE target_repository = $1
	  AND status IN ('queued', 'merge_check', 'targeted_verify')
	ORDER BY created_at, id
	LIMIT 1
	FOR UPDATE SKIP LOCKED
), claimed AS (
	INSERT INTO merge_repository_claims(target_repository, candidate_id, lease_owner, claimed_at, lease_expires_at)
	SELECT $1, id, $2, now(), now() + ($3 * interval '1 millisecond')
	FROM next_candidate
	ON CONFLICT (target_repository) DO UPDATE
	SET candidate_id = EXCLUDED.candidate_id,
	    lease_owner = EXCLUDED.lease_owner,
	    claimed_at = now(),
	    lease_expires_at = EXCLUDED.lease_expires_at
	WHERE merge_repository_claims.lease_expires_at <= now()
	RETURNING candidate_id
)
UPDATE merge_candidates c
SET status = CASE WHEN c.status = 'queued' THEN 'merge_check' ELSE c.status END,
    updated_at = CASE WHEN c.status = 'queued' THEN now() ELSE c.updated_at END
FROM claimed
WHERE c.id = claimed.candidate_id
RETURNING c.id`, normalizedTarget, s.ownerID, claimMillis).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return Candidate{}, false, mapPostgresMergeQueueError(err)
		}
		return Candidate{}, false, nil
	}
	if err != nil {
		return Candidate{}, false, mapPostgresMergeQueueError(err)
	}
	candidate, err := loadCandidate(ctx, tx, id)
	if err != nil {
		return Candidate{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, false, mapPostgresMergeQueueError(err)
	}
	return candidate, true, nil
}

func (s *PostgresStore) advance(ctx context.Context, id CandidateID, from, to Status, evidenceRefs []evidence.ArtifactID, mergedRevision string) (Candidate, error) {
	if !validAdvance(from, to) {
		return Candidate{}, kernel.InvalidArgument("invalid merge candidate transition")
	}
	if to == StatusMerged && strings.TrimSpace(mergedRevision) == "" {
		return Candidate{}, kernel.InvalidArgument("merged_revision is required")
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return Candidate{}, err
	}
	defer tx.Rollback()
	candidate, err := loadCandidateForUpdate(ctx, tx, id)
	if err != nil {
		return Candidate{}, err
	}
	if candidate.Status != from {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate status changed", Recoverable: true}
	}
	if err := insertEvidenceRefs(ctx, tx, id, evidenceRefs); err != nil {
		return Candidate{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE merge_candidates SET status = $2, merged_revision = NULLIF($3, ''), updated_at = now() WHERE id = $1`, id, to, mergedRevision); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	return s.Get(ctx, id)
}

func (s *PostgresStore) commitMerged(ctx context.Context, id CandidateID, evidenceRefs []evidence.ArtifactID, mergedRevision string, audit auditRecord) (Candidate, error) {
	if strings.TrimSpace(mergedRevision) == "" {
		return Candidate{}, kernel.InvalidArgument("merged_revision is required")
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return Candidate{}, err
	}
	defer tx.Rollback()
	candidate, err := loadCandidateForUpdate(ctx, tx, id)
	if err != nil {
		return Candidate{}, err
	}
	if candidate.Status == StatusMerged && candidate.MergedRevision == mergedRevision {
		if err := appendAudit(ctx, tx, audit); err != nil {
			return Candidate{}, err
		}
		if err := tx.Commit(); err != nil {
			return Candidate{}, mapPostgresMergeQueueError(err)
		}
		return candidate, nil
	}
	if candidate.Status != StatusTargetedVerify {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate status changed", Recoverable: true}
	}
	if err := insertEvidenceRefs(ctx, tx, id, evidenceRefs); err != nil {
		return Candidate{}, err
	}
	if err := appendAudit(ctx, tx, audit); err != nil {
		return Candidate{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE merge_candidates SET status = 'merged', merged_revision = $2, updated_at = now() WHERE id = $1`, id, mergedRevision); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	return s.Get(ctx, id)
}

func (s *PostgresStore) fail(ctx context.Context, id CandidateID, from Status, reason FailureReason, evidenceRefs []evidence.ArtifactID, audit auditRecord) (Candidate, error) {
	if !validFailureReason(reason) || len(evidenceRefs) == 0 {
		return Candidate{}, kernel.InvalidArgument("merge failure requires a valid reason and evidence refs")
	}
	tx, err := s.begin(ctx, serializableTx())
	if err != nil {
		return Candidate{}, err
	}
	defer tx.Rollback()
	candidate, err := loadCandidateForUpdate(ctx, tx, id)
	if err != nil {
		return Candidate{}, err
	}
	if candidate.Status == StatusFailed && candidate.FailureReason == reason {
		if err := appendAudit(ctx, tx, audit); err != nil {
			return Candidate{}, err
		}
		if err := tx.Commit(); err != nil {
			return Candidate{}, mapPostgresMergeQueueError(err)
		}
		return candidate, nil
	}
	if candidate.Status != from || (from != StatusMergeCheck && from != StatusTargetedVerify) {
		return Candidate{}, kernel.Error{Code: kernel.CodeTransitionRejected, Message: "merge candidate cannot fail from current status", Recoverable: true}
	}
	if err := insertEvidenceRefs(ctx, tx, id, evidenceRefs); err != nil {
		return Candidate{}, err
	}
	if err := appendAudit(ctx, tx, audit); err != nil {
		return Candidate{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE merge_candidates SET status = 'failed', failure_reason = $2, failure_evidence_ref = $3, updated_at = now() WHERE id = $1`, id, reason, evidenceRefs[0]); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	if err := tx.Commit(); err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	return s.Get(ctx, id)
}

func (s *PostgresStore) pendingAudits(ctx context.Context) ([]auditRecord, error) {
	tx, err := s.begin(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
SELECT stable_key, type, project_id, task_id, workspace_ref, payload::text, COALESCE(array_to_json(artifact_refs)::text, '[]'), delivered
FROM merge_audits
WHERE delivered = false
ORDER BY created_at, stable_key`)
	if err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	defer rows.Close()
	var out []auditRecord
	for rows.Next() {
		audit, err := scanAudit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, audit)
	}
	if err := rows.Err(); err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	return out, nil
}

func (s *PostgresStore) markAuditDelivered(ctx context.Context, key kernel.IdempotencyKey) error {
	result, err := s.exec(ctx, `UPDATE merge_audits SET delivered = true, delivered_at = now() WHERE stable_key = $1`, key)
	if err != nil {
		return err
	}
	if affected(result) == 0 {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "merge audit not found"}
	}
	return nil
}

func (s *PostgresStore) ReleaseClaim(ctx context.Context, targetRepository string, id CandidateID) error {
	normalizedTarget, err := normalizeTargetRepository(targetRepository)
	if err != nil {
		return err
	}
	_, err = s.exec(ctx, `DELETE FROM merge_repository_claims WHERE target_repository = $1 AND candidate_id = $2`, normalizedTarget, id)
	return err
}

func (s *PostgresStore) begin(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if s == nil || s.db == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "postgres merge queue database is not configured", Recoverable: true}
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	return tx, nil
}

func (s *PostgresStore) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx, err := s.begin(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	return result, nil
}

func loadCandidate(ctx context.Context, q postgresDBTX, id CandidateID) (Candidate, error) {
	return loadCandidateWithQuery(ctx, q, `
SELECT c.id, c.project_id, c.task_id, c.workspace_ref, c.verify_result_ref, c.diff_artifact_ref,
       c.target_repository, c.target_branch, c.base_revision, c.main_revision, c.candidate_revision,
       c.status, COALESCE(c.failure_reason, ''), COALESCE(c.merged_revision, ''), c.created_at, c.updated_at
FROM merge_candidates c
WHERE c.id = $1`, id)
}

func loadCandidateForUpdate(ctx context.Context, q postgresDBTX, id CandidateID) (Candidate, error) {
	return loadCandidateWithQuery(ctx, q, `
SELECT c.id, c.project_id, c.task_id, c.workspace_ref, c.verify_result_ref, c.diff_artifact_ref,
       c.target_repository, c.target_branch, c.base_revision, c.main_revision, c.candidate_revision,
       c.status, COALESCE(c.failure_reason, ''), COALESCE(c.merged_revision, ''), c.created_at, c.updated_at
FROM merge_candidates c
WHERE c.id = $1
FOR UPDATE`, id)
}

func loadCandidateWithQuery(ctx context.Context, q postgresDBTX, query string, id CandidateID) (Candidate, error) {
	var candidate Candidate
	err := q.QueryRowContext(ctx, query, id).Scan(
		&candidate.ID, &candidate.ProjectID, &candidate.TaskID, &candidate.WorkspaceRef,
		&candidate.VerifyResultRef, &candidate.DiffArtifactRef, &candidate.TargetRepository,
		&candidate.TargetBranch, &candidate.BaseRevision, &candidate.MainRevision,
		&candidate.CandidateRevision, &candidate.Status, &candidate.FailureReason,
		&candidate.MergedRevision, &candidate.CreatedAt, &candidate.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Candidate{}, kernel.Error{Code: kernel.CodeNotFound, Message: "merge candidate not found"}
	}
	if err != nil {
		return Candidate{}, mapPostgresMergeQueueError(err)
	}
	refs, err := loadCandidateEvidenceRefs(ctx, q, id)
	if err != nil {
		return Candidate{}, err
	}
	candidate.EvidenceRefs = refs
	candidate.CreatedAt = candidate.CreatedAt.UTC()
	candidate.UpdatedAt = candidate.UpdatedAt.UTC()
	return cloneCandidate(candidate), nil
}

func loadCandidateEvidenceRefs(ctx context.Context, q postgresDBTX, id CandidateID) ([]evidence.ArtifactID, error) {
	var refsJSON string
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(array_to_json(array_agg(artifact_id ORDER BY artifact_id))::text, '[]') FROM merge_candidate_evidence_refs WHERE candidate_id = $1`, id).Scan(&refsJSON); err != nil {
		return nil, mapPostgresMergeQueueError(err)
	}
	var refs []evidence.ArtifactID
	if err := json.Unmarshal([]byte(refsJSON), &refs); err != nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "stored merge candidate evidence refs are invalid", Recoverable: true}
	}
	return refs, nil
}

func insertEvidenceRefs(ctx context.Context, q postgresDBTX, id CandidateID, refs []evidence.ArtifactID) error {
	for _, ref := range dedupeEvidence(refs) {
		if _, err := q.ExecContext(ctx, `INSERT INTO merge_candidate_evidence_refs(candidate_id, artifact_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, ref); err != nil {
			return mapPostgresMergeQueueError(err)
		}
	}
	return nil
}

func appendAudit(ctx context.Context, q postgresDBTX, audit auditRecord) error {
	if audit.StableKey == "" || audit.Type == "" || audit.ProjectID == "" || audit.TaskID == "" || audit.WorkspaceRef == "" {
		return kernel.InvalidArgument("merge audit identity is required")
	}
	audit.Delivered = false
	audit.Payload = cloneStringMap(audit.Payload)
	audit.ArtifactRefs = dedupeEvidence(audit.ArtifactRefs)
	payload, err := json.Marshal(audit.Payload)
	if err != nil {
		return kernel.InvalidArgument("merge audit payload must be JSON serializable")
	}
	hash := auditHash(audit, payload)
	result, err := q.ExecContext(ctx, `
INSERT INTO merge_audits(stable_key, request_hash, type, project_id, task_id, workspace_ref, payload, artifact_refs)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::text[])
ON CONFLICT (stable_key) DO UPDATE SET stable_key = merge_audits.stable_key
WHERE merge_audits.request_hash = EXCLUDED.request_hash`,
		audit.StableKey, hash, audit.Type, audit.ProjectID, audit.TaskID, audit.WorkspaceRef, string(payload), textArrayLiteral(artifactIDsToStrings(audit.ArtifactRefs)))
	if err != nil {
		return mapPostgresMergeQueueError(err)
	}
	if affected(result) == 0 {
		return kernel.IdempotencyConflict()
	}
	return nil
}

func scanAudit(row interface{ Scan(...any) error }) (auditRecord, error) {
	var audit auditRecord
	var payloadText, refsJSON string
	if err := row.Scan(&audit.StableKey, &audit.Type, &audit.ProjectID, &audit.TaskID, &audit.WorkspaceRef, &payloadText, &refsJSON, &audit.Delivered); err != nil {
		return auditRecord{}, mapPostgresMergeQueueError(err)
	}
	if err := json.Unmarshal([]byte(payloadText), &audit.Payload); err != nil {
		return auditRecord{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored merge audit payload is invalid", Recoverable: true}
	}
	if err := json.Unmarshal([]byte(refsJSON), &audit.ArtifactRefs); err != nil {
		return auditRecord{}, kernel.Error{Code: kernel.CodeInternalError, Message: "stored merge audit refs are invalid", Recoverable: true}
	}
	return cloneAudit(audit), nil
}

func auditHash(audit auditRecord, payload []byte) string {
	refs, _ := json.Marshal(audit.ArtifactRefs)
	sum := sha256.Sum256([]byte(strings.Join([]string{
		string(audit.StableKey), audit.Type, string(audit.ProjectID), string(audit.TaskID), string(audit.WorkspaceRef), string(payload), string(refs),
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func artifactIDsToStrings(refs []evidence.ArtifactID) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = string(ref)
	}
	return out
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

func serializableTx() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelSerializable}
}

func affected(result sql.Result) int64 {
	n, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

func newMergeQueueOwnerID() string {
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return fmt.Sprintf("mergequeue:%d:%d", os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("mergequeue:%d:%s", os.Getpid(), hex.EncodeToString(token[:]))
}

func mapPostgresMergeQueueError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return kernel.IdempotencyConflict()
		case "23503", "23514":
			return kernel.InvalidArgument("merge queue row references invalid state")
		case "40001":
			return kernel.Error{Code: kernel.CodeRevisionConflict, Message: "serialization conflict while updating merge queue", Recoverable: true}
		}
	}
	return err
}

var _ Store = (*PostgresStore)(nil)
