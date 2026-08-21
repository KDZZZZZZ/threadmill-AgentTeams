// Package artifacts contains the small Artifact/Event boundary used by the
// Phase MCP service. It is deliberately storage-provider neutral.
package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ArtifactType string

const (
	ArtifactTypeAgentTranscript ArtifactType = "agent_transcript"
	ArtifactTypeToolOutput      ArtifactType = "tool_output"
	ArtifactTypeTestOutput      ArtifactType = "test_output"
	ArtifactTypeDiffPatch       ArtifactType = "diff_patch"
	ArtifactTypeScreenshot      ArtifactType = "screenshot"
	ArtifactTypeBenchmarkResult ArtifactType = "benchmark_result"
	ArtifactTypeGeneratedReport ArtifactType = "generated_report"
)

// Artifact follows the event/artifact-store model. PathOrBlobRef is never
// returned to an agent; ArtifactRef is the agent-facing opaque reference.
type Artifact struct {
	ID                string       `json:"id"`
	Type              ArtifactType `json:"type"`
	PathOrBlobRef     string       `json:"path_or_blob_ref"`
	ContentHash       string       `json:"content_hash"`
	TaskID            string       `json:"task_id,omitempty"`
	AgentInvocationID string       `json:"agent_invocation_id,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
}

type ArtifactRef string

type TrustedOwner struct {
	TaskID       string
	InvocationID string
	// Generation is audit evidence from the trusted Runtime binding; it is not
	// part of the logical artifact ownership/access key.
	Generation    int
	WorkspaceRoot string
	AllowedDirs   []string
}

type RegisterRequest struct {
	Owner          TrustedOwner
	ControlledPath string
	Kind           ArtifactType
	MediaType      string
}

// Registrar is the Runtime-facing seam. The caller supplies TrustedOwner from
// its server-side invocation binding, never from an agent request body.
type Registrar interface {
	Register(context.Context, RegisterRequest) (ArtifactRef, error)
	ValidateReferences(context.Context, TrustedOwner, []ArtifactRef) error
}

// DurableMetadata is the secret-free logical record kept by Runtime. BlobRef
// is an opaque object-store reference, never a workspace absolute path.
type DurableMetadata struct {
	Ref                ArtifactRef
	Type               ArtifactType
	ContentHash        string
	BlobRef            string
	OriginTaskID       string
	OriginInvocationID string
	CreatedAt          time.Time
}

// DurableStore is implemented by RuntimeStateRepository. Register atomically
// writes metadata, owner access and the Runtime outbox event.
type DurableStore interface {
	RegisterArtifact(context.Context, DurableMetadata, TrustedOwner) (ArtifactRef, bool, error)
	GetArtifact(context.Context, ArtifactRef) (DurableMetadata, bool, error)
	ValidateArtifactAccess(context.Context, TrustedOwner, []ArtifactRef) error
}

// BlobPublisher verifies and publishes bytes before metadata is committed.
// A failed metadata transaction may therefore leave an orphan blob, but never
// a durable record pointing at unpublished bytes.
type BlobPublisher interface {
	Publish(context.Context, string, string) (string, error)
}

// DurableRegistry retains the current controlled-path/hash semantics while
// making metadata and access grants recoverable. It deliberately does not
// expose object bytes to Runtime SQLite.
type DurableRegistry struct {
	store     DurableStore
	publisher BlobPublisher
	validator LocalPathValidator
}

func NewDurableRegistry(store DurableStore, publisher BlobPublisher) *DurableRegistry {
	return &DurableRegistry{store: store, publisher: publisher}
}

func (r *DurableRegistry) Register(ctx context.Context, request RegisterRequest) (ArtifactRef, error) {
	if r == nil || r.store == nil || r.publisher == nil || request.Kind == "" {
		return "", errors.New("durable artifact registry dependencies and kind are required")
	}
	path, err := r.validator.Validate(request.Owner, request.ControlledPath)
	if err != nil {
		return "", err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(contents)
	hash := hex.EncodeToString(digest[:])
	blobRef, err := r.publisher.Publish(ctx, path, hash)
	if err != nil {
		return "", err
	}
	if blobRef == "" {
		return "", errors.New("published blob reference is required")
	}
	ref := ArtifactRef("artifact-" + hash[:24])
	got, _, err := r.store.RegisterArtifact(ctx, DurableMetadata{Ref: ref, Type: request.Kind, ContentHash: hash, BlobRef: blobRef, OriginTaskID: request.Owner.TaskID, OriginInvocationID: request.Owner.InvocationID, CreatedAt: time.Now().UTC()}, request.Owner)
	return got, err
}

func (r *DurableRegistry) ValidateReferences(ctx context.Context, owner TrustedOwner, refs []ArtifactRef) error {
	if r == nil || r.store == nil {
		return errors.New("durable artifact registry is unavailable")
	}
	return r.store.ValidateArtifactAccess(ctx, owner, refs)
}

type Event struct {
	Type         string        `json:"type"`
	TaskID       string        `json:"task_id"`
	InvocationID string        `json:"invocation_id"`
	ArtifactRefs []ArtifactRef `json:"artifact_refs,omitempty"`
}

const (
	EventArtifactRegistered           = "ArtifactRegistered"
	EventPhaseOutputSubmitted         = "PhaseOutputSubmitted"
	EventAgentTeamsExecutionCompleted = "AgentTeamsExecutionCompleted"
	EventAgentTeamsExecutionFailed    = "AgentTeamsExecutionFailed"
)

type EventRecorder interface {
	Record(context.Context, Event) error
}

// LocalPathValidator is a logical path fence, not a production filesystem
// sandbox. It resolves an existing file and rejects relative/absolute escape,
// including a symlink that resolves outside permitted directories.
type LocalPathValidator struct{}

func (LocalPathValidator) Validate(owner TrustedOwner, controlledPath string) (string, error) {
	if owner.WorkspaceRoot == "" || owner.TaskID == "" || owner.InvocationID == "" || controlledPath == "" {
		return "", errors.New("trusted owner and controlled path are required")
	}
	if filepath.IsAbs(controlledPath) {
		return "", errors.New("controlled path must be relative to workspace")
	}
	root, err := filepath.EvalSymlinks(owner.WorkspaceRoot)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	candidate := filepath.Join(root, controlledPath)
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve controlled path: %w", err)
	}
	if !within(root, resolved) {
		return "", errors.New("controlled path escapes workspace")
	}
	if len(owner.AllowedDirs) == 0 {
		return "", errors.New("no allowed directories")
	}
	for _, dir := range owner.AllowedDirs {
		if filepath.IsAbs(dir) {
			continue
		}
		allowed := filepath.Join(root, dir)
		if within(allowed, resolved) {
			return resolved, nil
		}
	}
	return "", errors.New("controlled path is outside allowed directories")
}

func within(root, target string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(target))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// InMemoryRegistry is an MVP registry with SHA-256 deduplication. Access is
// granted per registered invocation, so deduplicating bytes never grants a
// second Task implicit access to the original owner's artifact.
type InMemoryRegistry struct {
	mu        sync.RWMutex
	validator LocalPathValidator
	byID      map[ArtifactRef]Artifact
	byHash    map[string]ArtifactRef
	access    map[ArtifactRef]map[string]struct{}
	recorder  EventRecorder
}

func NewInMemoryRegistry(recorder EventRecorder) *InMemoryRegistry {
	return &InMemoryRegistry{byID: make(map[ArtifactRef]Artifact), byHash: make(map[string]ArtifactRef), access: make(map[ArtifactRef]map[string]struct{}), recorder: recorder}
}

func (r *InMemoryRegistry) Register(ctx context.Context, request RegisterRequest) (ArtifactRef, error) {
	if request.Kind == "" {
		return "", errors.New("artifact kind is required")
	}
	path, err := r.validator.Validate(request.Owner, request.ControlledPath)
	if err != nil {
		return "", err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	hashBytes := sha256.Sum256(contents)
	hash := hex.EncodeToString(hashBytes[:])
	key := ownerKey(request.Owner)
	r.mu.Lock()
	ref, exists := r.byHash[hash]
	if !exists {
		ref = ArtifactRef("artifact-" + hash[:24])
		r.byHash[hash] = ref
		r.byID[ref] = Artifact{ID: string(ref), Type: request.Kind, PathOrBlobRef: path, ContentHash: hash, TaskID: request.Owner.TaskID, AgentInvocationID: request.Owner.InvocationID, CreatedAt: time.Now().UTC()}
	}
	if r.access[ref] == nil {
		r.access[ref] = make(map[string]struct{})
	}
	r.access[ref][key] = struct{}{}
	r.mu.Unlock()
	if r.recorder != nil {
		if err := r.recorder.Record(ctx, Event{Type: EventArtifactRegistered, TaskID: request.Owner.TaskID, InvocationID: request.Owner.InvocationID, ArtifactRefs: []ArtifactRef{ref}}); err != nil {
			return "", err
		}
	}
	return ref, nil
}

func (r *InMemoryRegistry) ValidateReferences(_ context.Context, owner TrustedOwner, refs []ArtifactRef) error {
	key := ownerKey(owner)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ref := range refs {
		if ref == "" {
			return errors.New("empty artifact reference")
		}
		if _, exists := r.byID[ref]; !exists {
			return fmt.Errorf("artifact %q is not registered", ref)
		}
		if _, granted := r.access[ref][key]; !granted {
			return fmt.Errorf("artifact %q is not accessible by current invocation", ref)
		}
	}
	return nil
}

func ownerKey(owner TrustedOwner) string { return owner.TaskID + "\x00" + owner.InvocationID }
