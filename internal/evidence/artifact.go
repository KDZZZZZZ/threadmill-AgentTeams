package evidence

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
)

type ArtifactType string

const (
	ArtifactAgentTranscript ArtifactType = "agent_transcript"
	ArtifactToolOutput      ArtifactType = "tool_output"
	ArtifactTestOutput      ArtifactType = "test_output"
	ArtifactDiffPatch       ArtifactType = "diff_patch"
	ArtifactScreenshot      ArtifactType = "screenshot"
	ArtifactGeneratedReport ArtifactType = "generated_report"
)

type PrincipalRole string

const (
	RoleTaskManager  PrincipalRole = "task_manager"
	RoleContextAgent PrincipalRole = "context_agent"
	RolePhaseAgent   PrincipalRole = "phase_agent"
	RoleAuditor      PrincipalRole = "auditor"
)

type Principal struct {
	Role      PrincipalRole
	ProjectID kernel.ProjectID
	TaskID    kernel.TaskID
}

type Artifact struct {
	ID                ArtifactID          `json:"id"`
	Type              ArtifactType        `json:"type"`
	PathOrBlobRef     string              `json:"path_or_blob_ref"`
	ContentHash       string              `json:"content_hash"`
	Size              int64               `json:"size"`
	ProjectID         kernel.ProjectID    `json:"project_id,omitempty"`
	TaskID            kernel.TaskID       `json:"task_id,omitempty"`
	AgentInvocationID kernel.InvocationID `json:"agent_invocation_id,omitempty"`
	CreatedAt         time.Time           `json:"created_at"`
}

type RegisterArtifact struct {
	Type              ArtifactType
	ProjectID         kernel.ProjectID
	Path              string
	ContentType       string
	Body              []byte
	TaskID            kernel.TaskID
	AgentInvocationID kernel.InvocationID
}

type ArtifactGrant struct {
	ProjectID kernel.ProjectID `json:"project_id"`
	TaskID    kernel.TaskID    `json:"task_id"`
}

type ArtifactRegistry struct {
	mu      sync.RWMutex
	bucket  string
	store   objectstore.Store
	records map[ArtifactID]Artifact
	byHash  map[string]ArtifactID
	grants  map[ArtifactID]map[ArtifactGrant]struct{}
	now     func() time.Time
}

func NewArtifactRegistry(store objectstore.Store, bucket string) *ArtifactRegistry {
	if bucket == "" {
		bucket = "artifacts"
	}
	return &ArtifactRegistry{
		bucket:  bucket,
		store:   store,
		records: make(map[ArtifactID]Artifact),
		byHash:  make(map[string]ArtifactID),
		grants:  make(map[ArtifactID]map[ArtifactGrant]struct{}),
		now:     time.Now,
	}
}

func (r *ArtifactRegistry) Register(ctx context.Context, req RegisterArtifact) (Artifact, error) {
	if r == nil || r.store == nil {
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

	r.mu.RLock()
	if existingID, ok := r.byHash[hash]; ok {
		existing := r.records[existingID]
		r.mu.RUnlock()
		r.addGrant(existingID, ArtifactGrant{ProjectID: req.ProjectID, TaskID: req.TaskID})
		return existing, nil
	}
	r.mu.RUnlock()

	put, err := r.store.Put(ctx, objectstore.PutObject{
		Bucket:      r.bucket,
		Key:         path.Join(string(req.Type), hash),
		ContentType: req.ContentType,
		Body:        bytes.NewReader(req.Body),
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("put artifact object: %w", err)
	}
	if put.SHA256 != hash {
		return Artifact{}, kernel.InvalidArgument("artifact hash mismatch after write")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existingID, ok := r.byHash[hash]; ok {
		r.addGrantLocked(existingID, ArtifactGrant{ProjectID: req.ProjectID, TaskID: req.TaskID})
		return r.records[existingID], nil
	}
	artifact := Artifact{
		ID:                ArtifactID("art_" + hash[:20]),
		Type:              req.Type,
		PathOrBlobRef:     put.Bucket + "/" + put.Key,
		ContentHash:       hash,
		Size:              put.Size,
		ProjectID:         req.ProjectID,
		TaskID:            req.TaskID,
		AgentInvocationID: req.AgentInvocationID,
		CreatedAt:         r.now().UTC(),
	}
	r.records[artifact.ID] = artifact
	r.byHash[hash] = artifact.ID
	r.addGrantLocked(artifact.ID, ArtifactGrant{ProjectID: req.ProjectID, TaskID: req.TaskID})
	return artifact, nil
}

func (r *ArtifactRegistry) Open(ctx context.Context, principal Principal, id ArtifactID) (Artifact, []byte, error) {
	if !r.CanRead(principal, id) {
		return Artifact{}, nil, kernel.Forbidden("artifact is not readable by principal")
	}
	r.mu.RLock()
	artifact, ok := r.records[id]
	r.mu.RUnlock()
	if !ok {
		return Artifact{}, nil, kernel.Error{Code: kernel.CodeNotFound, Message: "artifact not found"}
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

func (r *ArtifactRegistry) CanRead(principal Principal, id ArtifactID) bool {
	if r == nil {
		return false
	}
	if requirePrincipal(principal) != nil {
		return false
	}
	r.mu.RLock()
	artifact, ok := r.records[id]
	granted := false
	for grant := range r.grants[id] {
		if grant.ProjectID == principal.ProjectID && grant.TaskID == principal.TaskID {
			granted = true
			break
		}
	}
	r.mu.RUnlock()
	if !ok {
		return false
	}
	if artifact.Type == ArtifactAgentTranscript && principal.Role == RoleTaskManager {
		return false
	}
	if !granted {
		return false
	}
	return true
}

func (r *ArtifactRegistry) addGrant(id ArtifactID, grant ArtifactGrant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addGrantLocked(id, grant)
}

func (r *ArtifactRegistry) addGrantLocked(id ArtifactID, grant ArtifactGrant) {
	if r.grants[id] == nil {
		r.grants[id] = make(map[ArtifactGrant]struct{})
	}
	r.grants[id][grant] = struct{}{}
}

func requirePrincipal(principal Principal) error {
	if principal.Role == "" {
		return kernel.Forbidden("principal role is required")
	}
	if principal.ProjectID == "" {
		return kernel.Forbidden("principal project is required")
	}
	if principal.TaskID == "" {
		return kernel.Forbidden("principal task is required")
	}
	return nil
}

func parseBlobRef(ref string) (objectstore.ObjectRef, error) {
	bucket, key, ok := strings.Cut(ref, "/")
	if !ok || bucket == "" || key == "" {
		return objectstore.ObjectRef{}, kernel.InvalidArgument("invalid artifact blob ref")
	}
	return objectstore.ObjectRef{Bucket: bucket, Key: key}, nil
}

func rejectSensitivePath(p string) error {
	if p == "" {
		return nil
	}
	clean := strings.ToLower(path.Clean(strings.ReplaceAll(p, "\\", "/")))
	parts := strings.Split(clean, "/")
	for _, part := range parts {
		switch part {
		case "sessions", "credentials", "secrets", ".ssh", ".aws":
			return kernel.Forbidden("sensitive artifact path is not allowed")
		}
		if strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".key") || part == ".env" {
			return kernel.Forbidden("sensitive artifact path is not allowed")
		}
	}
	return nil
}

func rejectSensitiveContent(body []byte) error {
	lower := strings.ToLower(string(body))
	sensitive := []string{
		"-----begin private key-----",
		"aws_secret_access_key",
		"private_key=",
		"password=",
		"secret=",
	}
	for _, marker := range sensitive {
		if strings.Contains(lower, marker) {
			return kernel.Forbidden("sensitive artifact content is not allowed")
		}
	}
	return nil
}
