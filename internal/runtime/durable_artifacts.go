package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
)

type sqliteArtifactStore struct{ r *SQLiteRuntimeStateRepository }

func (s sqliteArtifactStore) RegisterArtifact(ctx context.Context, metadata artifacts.DurableMetadata, owner artifacts.TrustedOwner) (artifacts.ArtifactRef, bool, error) {
	if metadata.Ref == "" || metadata.Type == "" || metadata.ContentHash == "" || metadata.BlobRef == "" || owner.TaskID == "" || owner.InvocationID == "" || owner.Generation <= 0 || filepath.IsAbs(metadata.BlobRef) {
		return "", false, errors.New("durable artifact metadata, logical owner, and opaque blob ref are required")
	}
	if metadata.OriginTaskID != owner.TaskID || metadata.OriginInvocationID != owner.InvocationID {
		return "", false, errors.New("artifact origin must match trusted logical owner")
	}
	payload, err := runtimeJSON(metadata)
	if err != nil {
		return "", false, err
	}
	if err = noSecrets(payload); err != nil {
		return "", false, err
	}
	tx, err := s.r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	var stored []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM runtime_artifacts WHERE content_hash=?", metadata.ContentHash).Scan(&stored)
	created := false
	ref := metadata.Ref
	if err == nil {
		var existing artifacts.DurableMetadata
		if err = jsonUnmarshal(stored, &existing); err != nil {
			return "", false, err
		}
		if existing.Ref != metadata.Ref {
			return "", false, errors.New("content hash maps to conflicting artifact ref")
		}
		ref = existing.Ref // existing contract: first metadata wins for identical bytes.
	} else if isNoRows(err) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO runtime_artifacts VALUES(?,?,?)", metadata.Ref, metadata.ContentHash, payload); err != nil {
			return "", false, err
		}
		created = true
	} else {
		return "", false, err
	}
	result, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO runtime_artifact_access VALUES(?,?,?)", ref, owner.TaskID, owner.InvocationID)
	if err != nil {
		return "", false, err
	}
	grant, _ := result.RowsAffected()
	if created {
		key := WaitingKey{TaskID: owner.TaskID, InvocationID: owner.InvocationID, Generation: owner.Generation}
		if err = appendEvent(ctx, tx, artifacts.EventArtifactRegistered, key, 0, "artifact", 1, payload); err != nil {
			return "", false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return "", false, err
	}
	return ref, created || grant > 0, nil
}

func (s sqliteArtifactStore) GetArtifact(ctx context.Context, ref artifacts.ArtifactRef) (artifacts.DurableMetadata, bool, error) {
	var payload []byte
	err := s.r.db.QueryRowContext(ctx, "SELECT payload FROM runtime_artifacts WHERE artifact_ref=?", ref).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return artifacts.DurableMetadata{}, false, nil
	}
	if err != nil {
		return artifacts.DurableMetadata{}, false, err
	}
	var metadata artifacts.DurableMetadata
	err = jsonUnmarshal(payload, &metadata)
	return metadata, err == nil, err
}

func (s sqliteArtifactStore) ValidateArtifactAccess(ctx context.Context, owner artifacts.TrustedOwner, refs []artifacts.ArtifactRef) error {
	if owner.TaskID == "" || owner.InvocationID == "" {
		return errors.New("trusted logical owner is required")
	}
	for _, ref := range refs {
		if ref == "" {
			return errors.New("empty artifact reference")
		}
		var n int
		err := s.r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runtime_artifact_access WHERE artifact_ref=? AND task_id=? AND invocation_id=?", ref, owner.TaskID, owner.InvocationID).Scan(&n)
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("artifact %q is not accessible by current invocation", ref)
		}
	}
	return nil
}
