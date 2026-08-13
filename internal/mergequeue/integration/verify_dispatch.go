package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

const defaultResultPollInterval = 20 * time.Millisecond

// BindingRegistrar registers the temporary latest-main merge-check workspace
// with the authoritative Phase Runtime binding resolver. The returned value is
// the existing runtime BindingSnapshot, not a Merge Queue copy of that object.
type BindingRegistrar interface {
	RegisterTargetedVerify(context.Context, mergequeue.TargetedVerifyRequest) (phase.BindingSnapshot, error)
}

type PhaseRuntime interface {
	Apply(context.Context, coordination.PhaseCommand) error
	OutputByCommand(context.Context, string) (phase.OutputReceipt, bool, error)
}

type Verifier struct {
	Bindings     BindingRegistrar
	Runtime      PhaseRuntime
	Revisions    MainRevisionReader
	Results      ResultAcceptor
	PollInterval time.Duration
}

func (v Verifier) Verify(ctx context.Context, req mergequeue.TargetedVerifyRequest) (mergequeue.TargetedVerifyResult, error) {
	if err := v.ready(req); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	if err := v.requireCurrentMain(ctx, req); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	binding, err := v.Bindings.RegisterTargetedVerify(ctx, req)
	if err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	if err := validateBinding(req, binding); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	command := commandFor(req, binding)
	if err := v.Runtime.Apply(ctx, command); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	receipt, err := v.awaitReceipt(ctx, command.ID)
	if err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	if err := validateReceipt(command, req, receipt); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	// Re-read after the Invocation completes. A result is valid only for the
	// exact main revision registered in its immutable binding.
	if err := v.requireCurrentMain(ctx, req); err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	result, err := v.Results.AcceptTargetedVerify(ctx, receipt)
	if err != nil {
		return mergequeue.TargetedVerifyResult{}, err
	}
	if len(result.EvidenceRefs) == 0 {
		return mergequeue.TargetedVerifyResult{}, kernel.InvalidArgument("targeted verify result requires evidence_refs")
	}
	return result, nil
}

func (v Verifier) ready(req mergequeue.TargetedVerifyRequest) error {
	if v.Bindings == nil || v.Runtime == nil || v.Revisions == nil || v.Results == nil {
		return kernel.InvalidArgument("targeted verify runtime dependencies are required")
	}
	if req.Candidate.ID == "" || req.Candidate.ProjectID == "" || req.Candidate.TaskID == "" {
		return kernel.InvalidArgument("targeted verify candidate identity is required")
	}
	if strings.TrimSpace(req.Candidate.TargetRepository) == "" || strings.TrimSpace(req.WorkspaceRoot) == "" || strings.TrimSpace(req.LatestMainRevision) == "" {
		return kernel.InvalidArgument("targeted verify repository, workspace root, and latest main revision are required")
	}
	return nil
}

func (v Verifier) requireCurrentMain(ctx context.Context, req mergequeue.TargetedVerifyRequest) error {
	current, err := v.Revisions.CurrentRevision(ctx, req.Candidate.TargetRepository, req.Candidate.TargetBranch)
	if err != nil {
		return err
	}
	if current != req.LatestMainRevision {
		return mergequeue.MainDrift(req.LatestMainRevision, current)
	}
	return nil
}

func (v Verifier) awaitReceipt(ctx context.Context, commandID string) (phase.OutputReceipt, error) {
	interval := v.PollInterval
	if interval <= 0 {
		interval = defaultResultPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		receipt, ok, err := v.Runtime.OutputByCommand(ctx, commandID)
		if err != nil {
			return phase.OutputReceipt{}, err
		}
		if ok {
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return phase.OutputReceipt{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func commandFor(req mergequeue.TargetedVerifyRequest, binding phase.BindingSnapshot) coordination.PhaseCommand {
	sum := sha256.Sum256([]byte(req.LatestMainRevision))
	revisionKey := hex.EncodeToString(sum[:8])
	return coordination.PhaseCommand{
		ID: fmt.Sprintf("cmd:targeted-verify:%s:%s", req.Candidate.ID, revisionKey),
		Endpoint: coordination.PhaseEndpointRef{
			TaskID:     req.Candidate.TaskID,
			EndpointID: coordination.EndpointVerify,
		},
		Generation: binding.Generation,
		BindingRef: binding.BindingRef,
		LeaseRef:   binding.LeaseRef,
		Action:     coordination.CommandStart,
		CauseRef:   fmt.Sprintf("merge-candidate:%s@%s", req.Candidate.ID, req.LatestMainRevision),
	}
}

func validateBinding(req mergequeue.TargetedVerifyRequest, binding phase.BindingSnapshot) error {
	if binding.ProjectID != req.Candidate.ProjectID || binding.TaskID != req.Candidate.TaskID || binding.EndpointID != coordination.EndpointVerify {
		return kernel.StaleBinding("targeted verify binding does not match candidate and verify endpoint")
	}
	if binding.Generation <= 0 || binding.BindingRef == "" || binding.LeaseRef == "" {
		return kernel.StaleBinding("targeted verify binding authority is incomplete")
	}
	if binding.WorkspaceRef != req.WorkspaceRoot || binding.WorkspaceRevision != req.LatestMainRevision {
		return kernel.StaleBinding("targeted verify binding is not the temporary latest-main workspace")
	}
	if binding.Inputs.InputRevision == "" || binding.TaskContract == "" || binding.PhaseSpec == "" {
		return kernel.StaleBinding("targeted verify binding contract or input revision is incomplete")
	}
	return nil
}

func validateReceipt(command coordination.PhaseCommand, req mergequeue.TargetedVerifyRequest, receipt phase.OutputReceipt) error {
	if receipt.InvocationID == "" || receipt.Endpoint != command.Endpoint || receipt.Generation != command.Generation || receipt.BindingRef != command.BindingRef || receipt.LeaseRef != command.LeaseRef {
		return kernel.StaleBinding("targeted verify output does not match dispatched phase authority")
	}
	if receipt.Output.Phase != string(coordination.EndpointVerify) || receipt.InputRevision == "" {
		return kernel.StaleBinding("targeted verify output is not a complete verify receipt")
	}
	if receipt.WorkspaceRef != req.WorkspaceRoot || receipt.WorkspaceHead != req.LatestMainRevision {
		return kernel.StaleBinding("targeted verify output is not bound to latest main")
	}
	return nil
}
