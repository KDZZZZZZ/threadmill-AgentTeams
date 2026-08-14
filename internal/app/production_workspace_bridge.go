package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/workspace"
)

const productionWorkspaceProjectionVersion = "threadmill.workspace.native.v1"

// productionExecutionWorkspaceProjector is the trusted Runtime bridge between
// a phase Workspace Binding and AgentTeams' physical shared task directory.
// It never accepts task, binding, phase, or path authority from an Agent.
type productionExecutionWorkspaceProjector struct {
	invocations runtimepkg.InvocationStore
	workspaces  *workspace.Service
}

type productionWorkspaceProjection struct {
	Version           string              `json:"version"`
	InvocationID      kernel.InvocationID `json:"invocation_id"`
	BindingRef        kernel.BindingRef   `json:"binding_ref"`
	Phase             workspace.Phase     `json:"phase"`
	BindingRevision   kernel.Revision     `json:"binding_revision"`
	WorkspaceRevision string              `json:"workspace_revision"`
}

func (p productionExecutionWorkspaceProjector) OwnsExecution(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (bool, error) {
	invocation, err := p.invocation(ctx, execution)
	if err != nil {
		return false, err
	}
	return invocation.Role.IsPhase(), nil
}

func (p productionExecutionWorkspaceProjector) ExportExecutionFiles(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (agentteams.ExecutionFileProjection, error) {
	invocation, err := p.phaseInvocation(ctx, execution)
	if err != nil {
		return agentteams.ExecutionFileProjection{}, err
	}
	snapshot, err := p.workspaces.ExportNativeSnapshot(ctx, invocation.ID)
	if err != nil {
		return agentteams.ExecutionFileProjection{}, err
	}
	if snapshot.BindingRef != kernel.BindingRef(invocation.WorkspaceRef) || snapshot.InvocationID != invocation.ID {
		return agentteams.ExecutionFileProjection{}, kernel.StaleBinding("native workspace projection does not match invocation binding")
	}
	metadata, err := json.Marshal(productionWorkspaceProjection{
		Version: productionWorkspaceProjectionVersion, InvocationID: snapshot.InvocationID,
		BindingRef: snapshot.BindingRef, Phase: snapshot.Phase,
		BindingRevision: snapshot.BindingRevision, WorkspaceRevision: snapshot.WorkspaceRevision,
	})
	if err != nil {
		return agentteams.ExecutionFileProjection{}, err
	}
	projection := agentteams.ExecutionFileProjection{Manifest: metadata, Files: make([]agentteams.ExecutionFile, 0, len(snapshot.Files))}
	for _, file := range snapshot.Files {
		projection.Files = append(projection.Files, agentteams.ExecutionFile{
			Path: file.Path, Mode: file.Mode, Content: append([]byte(nil), file.Content...), SHA256: file.SHA256,
		})
	}
	return projection, nil
}

func (p productionExecutionWorkspaceProjector) ImportExecutionFiles(ctx context.Context, execution agentteams.AgentTeamsExecutionRef, projection agentteams.ExecutionFileProjection) (agentteams.ExecutionWorkspaceCheckpoint, error) {
	invocation, err := p.phaseInvocation(ctx, execution)
	if err != nil {
		return agentteams.ExecutionWorkspaceCheckpoint{}, err
	}
	metadata, err := decodeProductionWorkspaceProjection(projection.Manifest)
	if err != nil {
		return agentteams.ExecutionWorkspaceCheckpoint{}, err
	}
	if metadata.InvocationID != invocation.ID || metadata.BindingRef != kernel.BindingRef(invocation.WorkspaceRef) {
		return agentteams.ExecutionWorkspaceCheckpoint{}, kernel.StaleBinding("returned native workspace projection does not match invocation binding")
	}
	snapshot := workspace.NativeSnapshot{
		BindingRef: metadata.BindingRef, InvocationID: metadata.InvocationID, Phase: metadata.Phase,
		BindingRevision: metadata.BindingRevision, WorkspaceRevision: metadata.WorkspaceRevision,
		Files: make([]workspace.NativeSnapshotFile, 0, len(projection.Files)),
	}
	for _, file := range projection.Files {
		snapshot.Files = append(snapshot.Files, workspace.NativeSnapshotFile{
			Path: file.Path, Mode: file.Mode, Content: append([]byte(nil), file.Content...), SHA256: file.SHA256,
		})
	}
	updated, err := p.workspaces.ImportNativeSnapshot(ctx, invocation.ID, snapshot)
	if err != nil {
		return agentteams.ExecutionWorkspaceCheckpoint{}, err
	}
	return agentteams.ExecutionWorkspaceCheckpoint{WorkspaceRevision: updated.CurrentRevision}, nil
}

func (p productionExecutionWorkspaceProjector) invocation(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (runtimepkg.Invocation, error) {
	if p.invocations == nil || p.workspaces == nil {
		return runtimepkg.Invocation{}, kernel.Error{Code: kernel.CodeInternalError, Message: "production workspace bridge dependencies are required", Recoverable: false}
	}
	if err := kernel.RequireID("invocation_id", execution.InvocationID); err != nil {
		return runtimepkg.Invocation{}, err
	}
	invocation, ok, err := p.invocations.Get(ctx, execution.InvocationID)
	if err != nil {
		return runtimepkg.Invocation{}, err
	}
	if !ok {
		return runtimepkg.Invocation{}, kernel.Error{Code: kernel.CodeNotFound, Message: "workspace projection invocation not found"}
	}
	return invocation, nil
}

func (p productionExecutionWorkspaceProjector) phaseInvocation(ctx context.Context, execution agentteams.AgentTeamsExecutionRef) (runtimepkg.Invocation, error) {
	invocation, err := p.invocation(ctx, execution)
	if err != nil {
		return runtimepkg.Invocation{}, err
	}
	if !invocation.Role.IsPhase() || invocation.WorkspaceRef == "" {
		return runtimepkg.Invocation{}, kernel.Forbidden("native workspace projection requires a phase invocation")
	}
	switch invocation.Status {
	case runtimepkg.InvocationPrepared, runtimepkg.InvocationRunning, runtimepkg.InvocationWaiting:
		return invocation, nil
	default:
		return runtimepkg.Invocation{}, kernel.StaleBinding("native workspace projection invocation is not active")
	}
}

func decodeProductionWorkspaceProjection(raw []byte) (productionWorkspaceProjection, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var metadata productionWorkspaceProjection
	if err := decoder.Decode(&metadata); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return productionWorkspaceProjection{}, kernel.InvalidArgument("native workspace projection metadata is invalid")
	}
	if metadata.Version != productionWorkspaceProjectionVersion || metadata.InvocationID == "" || metadata.BindingRef == "" || metadata.Phase == "" {
		return productionWorkspaceProjection{}, kernel.InvalidArgument("native workspace projection metadata is incomplete")
	}
	return metadata, nil
}

var _ agentteams.ExecutionFileProjector = productionExecutionWorkspaceProjector{}
