package agentteams

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
)

const executionWorkspaceManifestVersion = "threadmill.agentteams.workspace.v1"

type executionWorkspaceManifest struct {
	Version string                  `json:"version"`
	Payload json.RawMessage         `json:"payload"`
	Files   []executionManifestFile `json:"files"`
}

type executionManifestFile struct {
	Path   string `json:"path"`
	Mode   uint32 `json:"mode"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type SharedObjectFileTransport struct {
	store     objectstore.Store
	bucket    string
	prefix    string
	limit     int64
	workspace ExecutionFileProjector
}

func NewSharedObjectFileTransport(store objectstore.Store, bucket, prefix string, workspace ...ExecutionFileProjector) (*SharedObjectFileTransport, error) {
	bucket = strings.TrimSpace(bucket)
	if store == nil || bucket == "" {
		return nil, kernel.InvalidArgument("AgentTeams shared object file transport requires store and bucket")
	}
	if len(workspace) > 1 {
		return nil, kernel.InvalidArgument("AgentTeams shared object file transport accepts one workspace projector")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	transport := &SharedObjectFileTransport{
		store: store, bucket: bucket, prefix: prefix,
		limit: 512 << 20,
	}
	if len(workspace) == 1 {
		transport.workspace = workspace[0]
	}
	return transport, nil
}

func (t *SharedObjectFileTransport) PrepareExecution(ctx context.Context, execution AgentTeamsExecutionRef, prepared PreparedInvocation) error {
	if !safeProviderID(execution.AgentTeamsTaskID) || prepared.InvocationID != execution.InvocationID {
		return kernel.InvalidArgument("AgentTeams execution workspace identity is invalid")
	}
	if t.workspace == nil {
		// Task Manager and Context Agent executions do not own Workspace
		// Bindings. Only phase invocations require a native project carrier.
		return nil
	}
	owned, err := t.workspace.OwnsExecution(ctx, execution)
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}
	projection, err := t.workspace.ExportExecutionFiles(ctx, execution)
	if err != nil {
		return err
	}
	manifest, err := buildExecutionWorkspaceManifest(projection)
	if err != nil {
		return err
	}
	for _, file := range projection.Files {
		if err := t.put(ctx, execution.AgentTeamsTaskID, path.Join("workspace", file.Path), "application/octet-stream", file.Content); err != nil {
			return err
		}
	}
	return t.put(ctx, execution.AgentTeamsTaskID, "threadmill/workspace.json", "application/json", manifest)
}

func (t *SharedObjectFileTransport) PullExecution(ctx context.Context, execution AgentTeamsExecutionRef) (ExecutionWorkspaceCheckpoint, error) {
	if !safeProviderID(execution.AgentTeamsTaskID) || execution.InvocationID == "" {
		return ExecutionWorkspaceCheckpoint{}, kernel.InvalidArgument("AgentTeams execution identity is invalid")
	}
	if t.workspace == nil {
		return ExecutionWorkspaceCheckpoint{}, nil
	}
	owned, err := t.workspace.OwnsExecution(ctx, execution)
	if err != nil {
		return ExecutionWorkspaceCheckpoint{}, err
	}
	if !owned {
		return ExecutionWorkspaceCheckpoint{}, nil
	}
	manifestRaw, err := t.read(ctx, execution.AgentTeamsTaskID, "threadmill/workspace.json", 8<<20)
	if err != nil {
		return ExecutionWorkspaceCheckpoint{}, err
	}
	manifest, err := parseExecutionWorkspaceManifest(manifestRaw)
	if err != nil {
		return ExecutionWorkspaceCheckpoint{}, err
	}
	projection := ExecutionFileProjection{Manifest: append([]byte(nil), manifest.Payload...), Files: make([]ExecutionFile, 0, len(manifest.Files))}
	var total int64
	for _, file := range manifest.Files {
		body, err := t.read(ctx, execution.AgentTeamsTaskID, path.Join("workspace", file.Path), file.Size)
		if err != nil {
			return ExecutionWorkspaceCheckpoint{}, err
		}
		total += int64(len(body))
		if total > t.limit {
			return ExecutionWorkspaceCheckpoint{}, kernel.Forbidden("AgentTeams workspace snapshot exceeds size limit")
		}
		projection.Files = append(projection.Files, ExecutionFile{Path: file.Path, Mode: file.Mode, Content: body, SHA256: file.SHA256})
	}
	return t.workspace.ImportExecutionFiles(ctx, execution, projection)
}

func (t *SharedObjectFileTransport) ReadResult(ctx context.Context, taskID string) ([]byte, error) {
	if !safeProviderID(taskID) {
		return nil, kernel.InvalidArgument("AgentTeams task_id is invalid")
	}
	return t.read(ctx, taskID, "result.md", 1<<20)
}

func (t *SharedObjectFileTransport) put(ctx context.Context, taskID, relative, contentType string, body []byte) error {
	key := path.Join(t.prefix, "shared", "tasks", taskID, relative)
	_, err := t.store.Put(ctx, objectstore.PutObject{Bucket: t.bucket, Key: key, ContentType: contentType, Body: bytes.NewReader(body)})
	return err
}

func (t *SharedObjectFileTransport) read(ctx context.Context, taskID, relative string, limit int64) ([]byte, error) {
	key := path.Join(t.prefix, "shared", "tasks", taskID, relative)
	got, err := t.store.Get(ctx, objectstore.ObjectRef{Bucket: t.bucket, Key: key})
	if err != nil {
		return nil, err
	}
	defer got.Body.Close()
	if limit <= 0 || limit > t.limit {
		limit = t.limit
	}
	body, err := io.ReadAll(io.LimitReader(got.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams execution file exceeded the limit", Recoverable: true}
	}
	return body, nil
}

func buildExecutionWorkspaceManifest(projection ExecutionFileProjection) ([]byte, error) {
	manifest := executionWorkspaceManifest{Version: executionWorkspaceManifestVersion, Payload: append([]byte(nil), projection.Manifest...)}
	seen := make(map[string]struct{}, len(projection.Files))
	for _, file := range projection.Files {
		clean, err := safeExecutionFilePath(file.Path)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[clean]; duplicate {
			return nil, kernel.InvalidArgument("AgentTeams workspace projection contains duplicate path")
		}
		seen[clean] = struct{}{}
		if strings.TrimSpace(file.SHA256) == "" || int64(len(file.Content)) > 4<<20 {
			return nil, kernel.Forbidden("AgentTeams workspace projection file is invalid")
		}
		manifest.Files = append(manifest.Files, executionManifestFile{Path: clean, Mode: file.Mode, SHA256: strings.ToLower(file.SHA256), Size: int64(len(file.Content))})
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	return json.Marshal(manifest)
}

func parseExecutionWorkspaceManifest(raw []byte) (executionWorkspaceManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest executionWorkspaceManifest
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return executionWorkspaceManifest{}, kernel.InvalidArgument("AgentTeams workspace manifest is invalid")
	}
	if manifest.Version != executionWorkspaceManifestVersion || len(manifest.Payload) == 0 {
		return executionWorkspaceManifest{}, kernel.InvalidArgument("AgentTeams workspace manifest version or payload is invalid")
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for index := range manifest.Files {
		clean, err := safeExecutionFilePath(manifest.Files[index].Path)
		if err != nil {
			return executionWorkspaceManifest{}, err
		}
		if _, duplicate := seen[clean]; duplicate || manifest.Files[index].Size < 0 || manifest.Files[index].Size > 4<<20 || !validExecutionSHA256(manifest.Files[index].SHA256) || manifest.Files[index].Mode&^0o777 != 0 {
			return executionWorkspaceManifest{}, kernel.InvalidArgument("AgentTeams workspace manifest file is invalid")
		}
		seen[clean] = struct{}{}
		manifest.Files[index].Path = clean
	}
	return manifest, nil
}

func validExecutionSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func safeExecutionFilePath(value string) (string, error) {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") || clean != value {
		return "", kernel.Forbidden("AgentTeams workspace file path is invalid")
	}
	return clean, nil
}

var _ FileTransport = (*SharedObjectFileTransport)(nil)
