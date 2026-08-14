package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	mergeintegration "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue/integration"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/taskmanager"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/mcpapi"
)

const (
	productionTargetedVerifyProjectionVersion = "threadmill.targeted_verify.workspace.v1"
	productionTargetedVerifySpecSchema        = "threadmill.targeted_verify.v1"
)

// productionTargetedVerifyBundle is intentionally only an internal wiring
// bundle. It reuses the normal Phase Controller and AgentTeams host, but its
// binding source, workspace projector, and runtime route are latest-main
// Merge Queue specific and never write the Coordination Graph.
type productionTargetedVerifyBundle struct {
	Bindings  *productionTargetedVerifyBindingSource
	Projector *productionTargetedVerifyWorkspaceProjector
	Runtime   *productionTargetedVerifyPhaseRuntime
	Registry  *productionTargetedVerifyRegistry
}

type productionTargetedVerifyBundleOptions struct {
	ProjectID       kernel.ProjectID
	Graph           productionTargetedVerifyGraphReader
	Contracts       targetedVerifyContractStore
	Invocations     runtimepkg.InvocationStore
	Assembler       *runtimepkg.Assembler
	Host            phasepkg.Host
	Recovery        phasepkg.RecoveryStore
	Contexts        phasepkg.ContextRuntime
	TaskSubgraphs   targetedVerifyTaskSubgraphRegistrar
	ArtifactRouter  phasepkg.ArtifactRouter
	OutputValidator productionTargetedVerifyOutputValidator
	Registry        *productionTargetedVerifyRegistry
	Projector       *productionTargetedVerifyWorkspaceProjector
	Proposals       productionTargetedVerifyProposalDispatcher
	WorkspaceSync   interface {
		SyncWorkspace(context.Context, kernel.InvocationID) (adapter.ExecutionWorkspaceCheckpoint, error)
	}
	Now           func() time.Time
	InvocationTTL time.Duration
}

type targetedVerifyContractStore interface {
	TaskContract(context.Context, kernel.TaskID) (taskmanager.TaskContract, error)
}

type targetedVerifyTaskSubgraphRegistrar interface {
	RegisterTaskSubgraph(context.Context, auth.Principal, kernel.TaskID) (contextgraph.TaskContextSubgraphBinding, error)
}

type productionTargetedVerifyGraphReader interface {
	Latest(context.Context, kernel.ProjectID) (coordination.GraphSnapshot, error)
}

type productionTargetedVerifyProposalDispatcher interface {
	persistAndDispatch(context.Context, productionInput) (persistedProductionInput, error)
}

type productionTargetedVerifyOutputValidator interface {
	ValidateTargetedVerifyOutput(context.Context, kernel.TaskID, phasepkg.PhaseOutput) error
}

// productionTargetedVerifyProposalBoundary is emitted only by the internal
// targeted-verify Runtime. The Agent supplies OrchestrationIntent; Runtime
// binds this merge identity from the registered command. Task Manager later
// revalidates every field against PostgreSQL before it may reopen a round.
type productionTargetedVerifyProposalBoundary struct {
	phasepkg.OrchestrationProposal
	SourceKind  string                 `json:"source_kind"`
	CandidateID mergequeue.CandidateID `json:"candidate_id"`
}

const productionTargetedVerifyProposalSource = "merge_targeted_verify"

func buildProductionTargetedVerifyBundle(options productionTargetedVerifyBundleOptions) (*productionTargetedVerifyBundle, error) {
	if kernel.IsZeroID(options.ProjectID) || options.Contracts == nil || options.Invocations == nil || options.Assembler == nil || options.Host == nil || options.Recovery == nil || options.Contexts == nil || options.ArtifactRouter == nil || options.OutputValidator == nil || options.WorkspaceSync == nil {
		return nil, kernel.InvalidArgument("targeted verify project, contracts, invocations, assembler, host, recovery, contexts, artifacts, output validation, and workspace sync are required")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	registry := options.Registry
	if registry == nil {
		registry = newProductionTargetedVerifyRegistry()
	}
	projector := options.Projector
	if projector == nil {
		projector = &productionTargetedVerifyWorkspaceProjector{registry: registry}
	} else if projector.registry == nil {
		projector.registry = registry
	} else if projector.registry != registry {
		return nil, kernel.InvalidArgument("targeted verify projector registry must match bundle registry")
	}
	source := &productionTargetedVerifyBindingSource{
		projectID:     options.ProjectID,
		contracts:     options.Contracts,
		taskSubgraphs: options.TaskSubgraphs,
		registry:      registry,
		now:           now,
	}
	resolver := phasepkg.NewContextBindingResolver(source, options.Contexts)
	controller := phasepkg.NewController(phasepkg.Config{
		InvocationStore: options.Invocations,
		Assembler:       options.Assembler,
		BindingResolver: resolver,
		InputRuntime:    productionTargetedVerifyInputs{},
		ArtifactRouter:  options.ArtifactRouter,
		Host:            options.Host,
		RecoveryStore:   options.Recovery,
		Lifecycle:       phasepkg.ContextBindingLifecycle{Contexts: options.Contexts},
		Observations:    nil,
		Now:             now,
		InvocationTTL:   options.InvocationTTL,
	})
	runtime := &productionTargetedVerifyPhaseRuntime{
		projectID:       options.ProjectID,
		controller:      controller,
		invocations:     options.Invocations,
		graph:           options.Graph,
		proposals:       options.Proposals,
		artifactRouter:  options.ArtifactRouter,
		outputValidator: options.OutputValidator,
		workspaceSync:   options.WorkspaceSync,
		registry:        registry,
		targetedBinding: source,
	}
	return &productionTargetedVerifyBundle{
		Bindings: source, Projector: projector, Runtime: runtime, Registry: registry,
	}, nil
}

type productionTargetedVerifyBindingSource struct {
	projectID     kernel.ProjectID
	contracts     targetedVerifyContractStore
	taskSubgraphs targetedVerifyTaskSubgraphRegistrar
	registry      *productionTargetedVerifyRegistry
	now           func() time.Time
}

func (s *productionTargetedVerifyBindingSource) RegisterTargetedVerify(ctx context.Context, req mergequeue.TargetedVerifyRequest) (phasepkg.BindingSnapshot, error) {
	if err := validateProductionTargetedVerifyRequest(req); err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	if req.Candidate.ProjectID != s.projectID {
		return phasepkg.BindingSnapshot{}, kernel.Forbidden("targeted verify candidate project does not match runtime")
	}
	contract, err := s.contracts.TaskContract(ctx, req.Candidate.TaskID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	if contract.TaskID != req.Candidate.TaskID {
		return phasepkg.BindingSnapshot{}, kernel.StaleBinding("targeted verify task contract does not match candidate")
	}
	generation := productionTargetedVerifyGeneration(req)
	endpoint := coordination.PhaseEndpointRef{TaskID: req.Candidate.TaskID, EndpointID: coordination.EndpointVerify}
	binding := phasepkg.BindingSnapshot{
		ProjectID:         req.Candidate.ProjectID,
		ActorPrincipalID:  productionTargetedVerifyActorPrincipal(req),
		TaskID:            req.Candidate.TaskID,
		EndpointID:        coordination.EndpointVerify,
		Generation:        generation,
		BindingRef:        productionTargetedVerifyBindingRef(req),
		LeaseRef:          productionTargetedVerifyLeaseRef(req),
		WorkspaceRef:      req.WorkspaceRoot,
		WorkspaceRevision: req.LatestMainRevision,
		Inputs: phasepkg.PhaseInputSet{
			InputRevision: productionTargetedVerifyInputRevision(req),
			Delivered: []phasepkg.InputDelivery{{
				InputID:        "merge-candidate:" + string(req.Candidate.ID),
				FromEndpoint:   endpoint,
				PhaseOutputRef: string(req.Candidate.VerifyResultRef),
				ArtifactRefs:   productionTargetedVerifyEvidenceStrings(req.Candidate.EvidenceRefs),
				SourceRevision: req.Candidate.CandidateRevision,
			}},
		},
	}
	contractJSON, err := stableProductionJSON(contract)
	if err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	specJSON, err := stableProductionJSON(productionTargetedVerifyPhaseSpec(req))
	if err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	workspaceJSON, err := stableProductionJSON(productionTargetedVerifyWorkspaceBinding(req))
	if err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	binding.TaskContract = string(contractJSON)
	binding.PhaseSpec = string(specJSON)
	binding.WorkspaceBinding = string(workspaceJSON)
	command := productionTargetedVerifyCommand(req, binding)
	if err := s.registry.RegisterBinding(ctx, req, command, binding); err != nil {
		return phasepkg.BindingSnapshot{}, err
	}
	return binding, nil
}

func (s *productionTargetedVerifyBindingSource) ResolvePhaseBinding(ctx context.Context, req phasepkg.BindingResolveRequest) (phasepkg.BindingSnapshot, []string, error) {
	entry, ok := s.registry.ByCommand(req.Command.ID)
	if !ok {
		return phasepkg.BindingSnapshot{}, nil, kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify binding not registered"}
	}
	if entry.Command != req.Command {
		return phasepkg.BindingSnapshot{}, nil, kernel.StaleBinding("targeted verify command does not match registered binding")
	}
	binding := entry.Binding
	initial, err := s.initialSubgraphs(ctx, req.Command.Endpoint.TaskID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	return binding, initial, nil
}

func (s *productionTargetedVerifyBindingSource) RefreshPhaseBinding(ctx context.Context, active phasepkg.ActiveInvocation) (phasepkg.BindingSnapshot, []string, error) {
	entry, ok := s.registry.ByInvocation(active.Invocation.ID)
	if !ok {
		return phasepkg.BindingSnapshot{}, nil, kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify invocation not registered"}
	}
	if entry.Command != active.Command {
		return phasepkg.BindingSnapshot{}, nil, kernel.StaleBinding("targeted verify active command changed")
	}
	initial, err := s.initialSubgraphs(ctx, active.Command.Endpoint.TaskID)
	if err != nil {
		return phasepkg.BindingSnapshot{}, nil, err
	}
	return entry.Binding, initial, nil
}

func (s *productionTargetedVerifyBindingSource) AbortResolvedPhaseBinding(ctx context.Context, req phasepkg.BindingResolveRequest, _ phasepkg.BindingSnapshot) error {
	if req.InvocationID != "" {
		s.registry.ForgetInvocation(req.InvocationID)
	}
	return nil
}

func (s *productionTargetedVerifyBindingSource) initialSubgraphs(ctx context.Context, taskID kernel.TaskID) ([]string, error) {
	if s.taskSubgraphs == nil {
		return nil, nil
	}
	principal := auth.Principal{
		ActorPrincipalID: kernel.ActorPrincipalID("task-manager://targeted-verify-context"),
		Kind:             auth.PrincipalAgent,
		ProjectID:        s.projectID,
		Role:             auth.RoleTaskManager,
		TaskID:           taskID,
		Tools:            auth.ToolSet(auth.ToolContextRegisterTaskSubgraph),
	}
	binding, err := s.taskSubgraphs.RegisterTaskSubgraph(ctx, principal, taskID)
	if err != nil {
		return nil, err
	}
	return []string{binding.SubgraphID}, nil
}

type productionTargetedVerifyPhaseRuntime struct {
	projectID       kernel.ProjectID
	controller      *phasepkg.Controller
	invocations     runtimepkg.InvocationStore
	graph           productionTargetedVerifyGraphReader
	proposals       productionTargetedVerifyProposalDispatcher
	artifactRouter  phasepkg.ArtifactRouter
	outputValidator productionTargetedVerifyOutputValidator
	workspaceSync   interface {
		SyncWorkspace(context.Context, kernel.InvocationID) (adapter.ExecutionWorkspaceCheckpoint, error)
	}
	registry        *productionTargetedVerifyRegistry
	targetedBinding *productionTargetedVerifyBindingSource
}

func (r *productionTargetedVerifyPhaseRuntime) Apply(ctx context.Context, command coordination.PhaseCommand) error {
	if r == nil || r.controller == nil {
		return kernel.InvalidArgument("targeted verify runtime is not configured")
	}
	return r.controller.Apply(ctx, command)
}

func (r *productionTargetedVerifyPhaseRuntime) OutputByCommand(ctx context.Context, commandID string) (phasepkg.OutputReceipt, bool, error) {
	if r == nil || r.registry == nil {
		return phasepkg.OutputReceipt{}, false, kernel.InvalidArgument("targeted verify runtime registry is not configured")
	}
	if err, ok := r.registry.FailureByCommand(commandID); ok {
		return phasepkg.OutputReceipt{}, true, err
	}
	if r.controller == nil {
		return phasepkg.OutputReceipt{}, false, kernel.InvalidArgument("targeted verify runtime is not configured")
	}
	return r.controller.OutputByCommand(ctx, commandID)
}

func (r *productionTargetedVerifyPhaseRuntime) AwaitInputs(ctx context.Context, invocationID kernel.InvocationID, req phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	if r == nil || r.registry == nil {
		return phasepkg.InputWaitResult{}, kernel.InvalidArgument("targeted verify runtime registry is not configured")
	}
	if err, ok := r.registry.FailureByInvocation(invocationID); ok {
		return phasepkg.InputWaitResult{}, err
	}
	if r.controller == nil {
		return phasepkg.InputWaitResult{}, kernel.InvalidArgument("targeted verify runtime is not configured")
	}
	return r.controller.AwaitInputs(ctx, invocationID, req)
}

func (r *productionTargetedVerifyPhaseRuntime) SubmitPhaseOutput(ctx context.Context, invocationID kernel.InvocationID, output phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	if r == nil || r.registry == nil {
		return phasepkg.OutputReceipt{}, kernel.InvalidArgument("targeted verify runtime registry is not configured")
	}
	if err, ok := r.registry.FailureByInvocation(invocationID); ok {
		return phasepkg.OutputReceipt{}, err
	}
	if r.controller == nil || r.workspaceSync == nil || r.outputValidator == nil {
		return phasepkg.OutputReceipt{}, kernel.InvalidArgument("targeted verify runtime is not configured")
	}
	entry, ok := r.registry.ByInvocation(invocationID)
	if !ok {
		return phasepkg.OutputReceipt{}, kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify invocation not registered"}
	}
	if !r.registry.IsTerminal(invocationID) {
		// A failing targeted verifier must ask Task Manager to replan before
		// the Phase becomes terminal. Rejecting a fail report here keeps the
		// one-shot invocation alive so agent.proposeOrchestration can persist
		// the only authority capable of reopening Execute+Verify.
		if err := r.outputValidator.ValidateTargetedVerifyOutput(ctx, entry.Binding.TaskID, output); err != nil {
			return phasepkg.OutputReceipt{}, err
		}
		checkpoint, err := r.workspaceSync.SyncWorkspace(ctx, invocationID)
		if err != nil {
			return phasepkg.OutputReceipt{}, err
		}
		if checkpoint.WorkspaceRevision != "" && checkpoint.WorkspaceRevision != entry.Request.LatestMainRevision {
			return phasepkg.OutputReceipt{}, kernel.StaleBinding("targeted verify workspace sync returned non-latest-main revision")
		}
	}
	receipt, err := r.controller.SubmitPhaseOutput(ctx, invocationID, output)
	if err != nil {
		return phasepkg.OutputReceipt{}, err
	}
	r.registry.MarkTerminal(invocationID)
	return receipt, nil
}

func (r *productionTargetedVerifyPhaseRuntime) SubmitOrchestrationIntent(ctx context.Context, principal auth.Principal, scope auth.BoundScope, intent phasepkg.OrchestrationIntent) (phasepkg.OrchestrationProposal, error) {
	if err := phasepkg.ValidateOrchestrationIntent(intent); err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	if r == nil || r.registry == nil || r.graph == nil || r.proposals == nil || r.artifactRouter == nil || r.invocations == nil {
		return phasepkg.OrchestrationProposal{}, kernel.InvalidArgument("targeted verify orchestration runtime is not configured")
	}
	if principal.ProjectID != r.projectID || scope.ProjectID != r.projectID || principal.InvocationID == "" || scope.InvocationID != principal.InvocationID || principal.Role != auth.RoleVerifier {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("targeted verify orchestration scope mismatch")
	}
	entry, ok := r.registry.ByInvocation(principal.InvocationID)
	if !ok {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("targeted verify invocation is not registered")
	}
	if err, ok := r.registry.FailureByInvocation(principal.InvocationID); ok {
		return phasepkg.OrchestrationProposal{}, err
	}
	if principal.TaskID != entry.Binding.TaskID || scope.TaskID != entry.Binding.TaskID || entry.Binding.EndpointID != coordination.EndpointVerify {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("targeted verify orchestration task scope mismatch")
	}
	invocation, ok, err := r.invocations.Get(ctx, principal.InvocationID)
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	if !ok || invocation.Status != runtimepkg.InvocationRunning || invocation.Role != auth.RoleVerifier || invocation.TaskID != entry.Binding.TaskID || invocation.EndpointID != coordination.EndpointVerify {
		return phasepkg.OrchestrationProposal{}, kernel.Forbidden("targeted verify orchestration requires an active verifier invocation")
	}
	snapshot, err := r.graph.Latest(ctx, r.projectID)
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	active := phasepkg.ActiveInvocation{
		Invocation: invocation,
		Command:    entry.Command,
		Binding:    entry.Binding,
		Inputs:     entry.Binding.Inputs,
	}
	evidenceRefs := append([]string(nil), intent.EvidenceRefs...)
	for i, ref := range evidenceRefs {
		routed, err := r.artifactRouter.Route(ctx, active, ref)
		if err != nil {
			return phasepkg.OrchestrationProposal{}, err
		}
		evidenceRefs[i] = routed
	}
	proposal := phasepkg.OrchestrationProposal{
		ProposalID:               stableRuntimeRef("targeted-verify-orchestration", principal.InvocationID, snapshot.Revision, intent.OrchestrationAdvice),
		ClientRef:                stableRuntimeRef("targeted-verify-orchestration-client", principal.InvocationID, intent.Rationale),
		FromEndpoint:             coordination.PhaseEndpointRef{TaskID: entry.Binding.TaskID, EndpointID: coordination.EndpointVerify},
		FromInvocationID:         principal.InvocationID,
		BasedOnGraphRevision:     snapshot.Revision,
		BasedOnWorkspaceRevision: entry.Request.LatestMainRevision,
		BasedOnInputRevision:     entry.Binding.Inputs.InputRevision,
		OrchestrationAdvice:      intent.OrchestrationAdvice,
		DeliverySpecAdvice:       intent.DeliverySpecAdvice,
		ReportSpecAdvice:         intent.ReportSpecAdvice,
		Rationale:                intent.Rationale,
		EvidenceRefs:             evidenceRefs,
	}
	payload, err := json.Marshal(productionTargetedVerifyProposalBoundary{
		OrchestrationProposal: proposal,
		SourceKind:            productionTargetedVerifyProposalSource,
		CandidateID:           entry.Request.Candidate.ID,
	})
	if err != nil {
		return phasepkg.OrchestrationProposal{}, err
	}
	stored, dispatchErr := r.proposals.persistAndDispatch(ctx, productionInput{
		Kind:             "phase_orchestration",
		RequestID:        proposal.ProposalID,
		ConversationID:   "runtime:" + string(entry.Binding.TaskID),
		Body:             proposal.Rationale,
		Payload:          payload,
		SeenRevision:     snapshot.Revision,
		SelectedEndpoint: &proposal.FromEndpoint,
		TargetKind:       "phase_orchestration",
		TargetRef:        proposal.ProposalID,
	})
	if dispatchErr != nil && stored.InputRef == "" {
		return phasepkg.OrchestrationProposal{}, dispatchErr
	}
	r.registry.MarkFailed(principal.InvocationID, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "targeted verify requested Task Manager replan", Recoverable: true})
	return proposal, nil
}

// FailTargetedInvocation is the hook root should call from an AgentTeams
// terminal-failure monitor. Targeted commands do not enter
// coordination_phase_commands, so the generic production phase monitor cannot
// discover them without a shared hook.
func (r *productionTargetedVerifyPhaseRuntime) FailTargetedInvocation(ctx context.Context, invocationID kernel.InvocationID) error {
	if r == nil || r.registry == nil || r.invocations == nil {
		return kernel.InvalidArgument("targeted verify runtime is not configured")
	}
	entry, ok := r.registry.ByInvocation(invocationID)
	if !ok {
		return r.failUnregisteredTargetedInvocation(ctx, invocationID)
	}
	if r.controller == nil {
		return kernel.InvalidArgument("targeted verify runtime controller is not configured")
	}
	err := r.controller.FailInvocation(ctx, entry.Command)
	if err == nil || kernel.IsCode(err, kernel.CodeStaleCommand) {
		r.registry.MarkFailed(invocationID, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "targeted verify invocation failed", Recoverable: true})
	}
	return err
}

func (r *productionTargetedVerifyPhaseRuntime) failUnregisteredTargetedInvocation(ctx context.Context, invocationID kernel.InvocationID) error {
	invocation, ok, err := r.invocations.Get(ctx, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify invocation not found"}
	}
	if invocation.ProjectID != r.projectID || invocation.Role != auth.RoleVerifier || invocation.EndpointID != coordination.EndpointVerify || !strings.HasPrefix(string(invocation.BindingRef), "targeted-verify-binding:") {
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "invocation is not a historical targeted verifier", Recoverable: true}
	}
	switch invocation.Status {
	case runtimepkg.InvocationPrepared, runtimepkg.InvocationRunning, runtimepkg.InvocationWaiting:
		return r.invocations.Transition(ctx, invocationID, invocation.Status, runtimepkg.InvocationFailed)
	case runtimepkg.InvocationFailed:
		return nil
	case runtimepkg.InvocationCompleted, runtimepkg.InvocationStopped:
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "targeted verify invocation already reached a different terminal state", Recoverable: true}
	default:
		return kernel.Error{Code: kernel.CodeStaleCommand, Message: "targeted verify invocation is not failure-eligible", Recoverable: true}
	}
}

type productionTargetedVerifyInputs struct{}

func (productionTargetedVerifyInputs) AwaitInputs(_ context.Context, active phasepkg.ActiveInvocation, _ phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	return phasepkg.InputWaitResult{
		InputRevision: active.Inputs.InputRevision,
		Delivered:     append([]phasepkg.InputDelivery(nil), active.Inputs.Delivered...),
		Pending:       append([]phasepkg.PendingInput(nil), active.Inputs.Pending...),
	}, nil
}

type productionTargetedVerifyWorkspaceProjector struct {
	registry *productionTargetedVerifyRegistry
}

func (p *productionTargetedVerifyWorkspaceProjector) OwnsExecution(ctx context.Context, execution adapter.AgentTeamsExecutionRef) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return p != nil && p.registry != nil && p.registry.OwnsInvocation(execution.InvocationID), nil
}

func (p *productionTargetedVerifyWorkspaceProjector) ExportExecutionFiles(ctx context.Context, execution adapter.AgentTeamsExecutionRef) (adapter.ExecutionFileProjection, error) {
	entry, ok := p.registry.ByInvocation(execution.InvocationID)
	if !ok {
		return adapter.ExecutionFileProjection{}, kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify invocation not registered"}
	}
	root, err := safeProductionTargetedVerifyRoot(entry.Request.WorkspaceRoot)
	if err != nil {
		return adapter.ExecutionFileProjection{}, err
	}
	files, baseline, err := readProductionTargetedVerifyFiles(root)
	if err != nil {
		return adapter.ExecutionFileProjection{}, err
	}
	manifest, err := json.Marshal(productionTargetedVerifyProjectionManifest{
		Version:            productionTargetedVerifyProjectionVersion,
		InvocationID:       execution.InvocationID,
		CommandID:          entry.Command.ID,
		CandidateID:        entry.Request.Candidate.ID,
		WorkspaceRoot:      root,
		LatestMainRevision: entry.Request.LatestMainRevision,
	})
	if err != nil {
		return adapter.ExecutionFileProjection{}, err
	}
	if err := p.registry.SetBaseline(execution.InvocationID, root, baseline); err != nil {
		return adapter.ExecutionFileProjection{}, err
	}
	return adapter.ExecutionFileProjection{Manifest: manifest, Files: files}, nil
}

func (p *productionTargetedVerifyWorkspaceProjector) ImportExecutionFiles(ctx context.Context, execution adapter.AgentTeamsExecutionRef, projection adapter.ExecutionFileProjection) (adapter.ExecutionWorkspaceCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	entry, ok := p.registry.ByInvocation(execution.InvocationID)
	if !ok {
		return adapter.ExecutionWorkspaceCheckpoint{}, kernel.Error{Code: kernel.CodeNotFound, Message: "targeted verify invocation not registered"}
	}
	manifest, err := decodeProductionTargetedVerifyProjectionManifest(projection.Manifest)
	if err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	root, err := safeProductionTargetedVerifyRoot(entry.Request.WorkspaceRoot)
	if err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	if manifest.InvocationID != execution.InvocationID || manifest.CommandID != entry.Command.ID || manifest.CandidateID != entry.Request.Candidate.ID || manifest.WorkspaceRoot != root || manifest.LatestMainRevision != entry.Request.LatestMainRevision {
		return adapter.ExecutionWorkspaceCheckpoint{}, kernel.StaleBinding("targeted verify projection manifest does not match invocation")
	}
	baseline, ok := p.registry.Baseline(execution.InvocationID)
	if !ok {
		return adapter.ExecutionWorkspaceCheckpoint{}, kernel.StaleBinding("targeted verify baseline projection is missing")
	}
	headBefore, hasHead, err := productionTargetedVerifyHead(ctx, root)
	if err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	if err := applyProductionTargetedVerifyReturnedFiles(ctx, root, baseline, projection.Files, productionTargetedVerifyAuthorizedWritePaths(entry.Request)); err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	headAfter, hasHeadAfter, err := productionTargetedVerifyHead(ctx, root)
	if err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	if hasHead != hasHeadAfter || (hasHead && headBefore != headAfter) {
		return adapter.ExecutionWorkspaceCheckpoint{}, kernel.Forbidden("targeted verify cannot change repository HEAD")
	}
	if err := ctx.Err(); err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	return adapter.ExecutionWorkspaceCheckpoint{WorkspaceRevision: entry.Request.LatestMainRevision}, nil
}

type productionTargetedVerifyProjectorRouter struct {
	Regular  adapter.ExecutionFileProjector
	Targeted adapter.ExecutionFileProjector
}

func (r productionTargetedVerifyProjectorRouter) OwnsExecution(ctx context.Context, execution adapter.AgentTeamsExecutionRef) (bool, error) {
	if r.Targeted != nil {
		owned, err := r.Targeted.OwnsExecution(ctx, execution)
		if err != nil || owned {
			return owned, err
		}
	}
	if r.Regular == nil {
		return false, nil
	}
	return r.Regular.OwnsExecution(ctx, execution)
}

func (r productionTargetedVerifyProjectorRouter) ExportExecutionFiles(ctx context.Context, execution adapter.AgentTeamsExecutionRef) (adapter.ExecutionFileProjection, error) {
	if r.Targeted != nil {
		owned, err := r.Targeted.OwnsExecution(ctx, execution)
		if err != nil {
			return adapter.ExecutionFileProjection{}, err
		}
		if owned {
			return r.Targeted.ExportExecutionFiles(ctx, execution)
		}
	}
	if r.Regular == nil {
		return adapter.ExecutionFileProjection{}, kernel.Forbidden("execution workspace projector is not configured")
	}
	return r.Regular.ExportExecutionFiles(ctx, execution)
}

func (r productionTargetedVerifyProjectorRouter) ImportExecutionFiles(ctx context.Context, execution adapter.AgentTeamsExecutionRef, projection adapter.ExecutionFileProjection) (adapter.ExecutionWorkspaceCheckpoint, error) {
	if r.Targeted != nil {
		owned, err := r.Targeted.OwnsExecution(ctx, execution)
		if err != nil {
			return adapter.ExecutionWorkspaceCheckpoint{}, err
		}
		if owned {
			return r.Targeted.ImportExecutionFiles(ctx, execution, projection)
		}
	}
	if r.Regular == nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, kernel.Forbidden("execution workspace projector is not configured")
	}
	return r.Regular.ImportExecutionFiles(ctx, execution, projection)
}

type productionTargetedVerifyRuntimeRouter struct {
	Regular         mcpapi.PhaseRuntime
	RegularProposal mcpapi.OrchestrationProposalRuntime
	Targeted        mcpapi.PhaseRuntime
	Registry        *productionTargetedVerifyRegistry
}

func (r productionTargetedVerifyRuntimeRouter) AwaitInputs(ctx context.Context, invocationID kernel.InvocationID, req phasepkg.AwaitInputsRequest) (phasepkg.InputWaitResult, error) {
	if r.Registry != nil && r.Registry.OwnsInvocation(invocationID) {
		if r.Targeted == nil {
			return phasepkg.InputWaitResult{}, kernel.InvalidArgument("targeted verify runtime is not configured")
		}
		return r.Targeted.AwaitInputs(ctx, invocationID, req)
	}
	if r.Regular == nil {
		return phasepkg.InputWaitResult{}, kernel.InvalidArgument("regular phase runtime is not configured")
	}
	return r.Regular.AwaitInputs(ctx, invocationID, req)
}

func (r productionTargetedVerifyRuntimeRouter) SubmitPhaseOutput(ctx context.Context, invocationID kernel.InvocationID, output phasepkg.PhaseOutput) (phasepkg.OutputReceipt, error) {
	if r.Registry != nil && r.Registry.OwnsInvocation(invocationID) {
		if r.Targeted == nil {
			return phasepkg.OutputReceipt{}, kernel.InvalidArgument("targeted verify runtime is not configured")
		}
		return r.Targeted.SubmitPhaseOutput(ctx, invocationID, output)
	}
	if r.Regular == nil {
		return phasepkg.OutputReceipt{}, kernel.InvalidArgument("regular phase runtime is not configured")
	}
	return r.Regular.SubmitPhaseOutput(ctx, invocationID, output)
}

func (r productionTargetedVerifyRuntimeRouter) SubmitOrchestrationIntent(ctx context.Context, principal auth.Principal, scope auth.BoundScope, intent phasepkg.OrchestrationIntent) (phasepkg.OrchestrationProposal, error) {
	if r.Registry != nil && r.Registry.OwnsInvocation(principal.InvocationID) {
		targeted, ok := r.Targeted.(mcpapi.OrchestrationProposalRuntime)
		if !ok || targeted == nil {
			return phasepkg.OrchestrationProposal{}, kernel.InvalidArgument("targeted verify orchestration runtime is not configured")
		}
		return targeted.SubmitOrchestrationIntent(ctx, principal, scope, intent)
	}
	regular := r.RegularProposal
	if regular == nil {
		if candidate, ok := r.Regular.(mcpapi.OrchestrationProposalRuntime); ok {
			regular = candidate
		}
	}
	if regular == nil {
		return phasepkg.OrchestrationProposal{}, kernel.InvalidArgument("regular orchestration runtime is not configured")
	}
	return regular.SubmitOrchestrationIntent(ctx, principal, scope, intent)
}

type productionTargetedVerifyRegistry struct {
	mu           sync.RWMutex
	byCommand    map[string]productionTargetedVerifyEntry
	byInvocation map[kernel.InvocationID]string
	baselines    map[kernel.InvocationID]productionTargetedVerifyBaseline
	terminal     map[kernel.InvocationID]struct{}
	failures     map[kernel.InvocationID]kernel.Error
}

type productionTargetedVerifyEntry struct {
	Request mergequeue.TargetedVerifyRequest
	Command coordination.PhaseCommand
	Binding phasepkg.BindingSnapshot
}

type productionTargetedVerifyBaseline struct {
	Root  string
	Files map[string]productionTargetedVerifyFile
}

type productionTargetedVerifyFile struct {
	Mode   uint32
	SHA256 string
}

func productionTargetedVerifyComparableMode(mode uint32) uint32 {
	// Windows checkout permissions (commonly 0666) and Linux carrier
	// permissions after umask (commonly 0644) differ even when the Agent did
	// not touch the file. The executable bits are the only portable semantic
	// permission carried by Git, so keep enforcing those while content hashes
	// and exact paths enforce the source/config ACL.
	return mode & 0o111
}

func newProductionTargetedVerifyRegistry() *productionTargetedVerifyRegistry {
	return &productionTargetedVerifyRegistry{
		byCommand:    make(map[string]productionTargetedVerifyEntry),
		byInvocation: make(map[kernel.InvocationID]string),
		baselines:    make(map[kernel.InvocationID]productionTargetedVerifyBaseline),
		terminal:     make(map[kernel.InvocationID]struct{}),
		failures:     make(map[kernel.InvocationID]kernel.Error),
	}
}

func (r *productionTargetedVerifyRegistry) RegisterBinding(ctx context.Context, req mergequeue.TargetedVerifyRequest, command coordination.PhaseCommand, binding phasepkg.BindingSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return kernel.InvalidArgument("targeted verify registry is required")
	}
	invocationID := deterministicPhaseInvocationID(command.ID)
	entry := productionTargetedVerifyEntry{Request: req, Command: command, Binding: binding}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byCommand[command.ID]; ok {
		if !sameProductionTargetedVerifyEntry(existing, entry) {
			return kernel.IdempotencyConflict()
		}
		if r.byInvocation[invocationID] == "" {
			r.byInvocation[invocationID] = command.ID
		}
		return nil
	}
	r.byCommand[command.ID] = entry
	r.byInvocation[invocationID] = command.ID
	return nil
}

func (r *productionTargetedVerifyRegistry) ByCommand(commandID string) (productionTargetedVerifyEntry, bool) {
	if r == nil {
		return productionTargetedVerifyEntry{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.byCommand[commandID]
	return entry, ok
}

func (r *productionTargetedVerifyRegistry) ByInvocation(invocationID kernel.InvocationID) (productionTargetedVerifyEntry, bool) {
	if r == nil {
		return productionTargetedVerifyEntry{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	commandID, ok := r.byInvocation[invocationID]
	if !ok {
		return productionTargetedVerifyEntry{}, false
	}
	entry, ok := r.byCommand[commandID]
	return entry, ok
}

func (r *productionTargetedVerifyRegistry) OwnsInvocation(invocationID kernel.InvocationID) bool {
	if r == nil || invocationID == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byInvocation[invocationID]
	return ok
}

func (r *productionTargetedVerifyRegistry) ForgetInvocation(invocationID kernel.InvocationID) {
	if r == nil || invocationID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	commandID := r.byInvocation[invocationID]
	delete(r.byInvocation, invocationID)
	delete(r.baselines, invocationID)
	delete(r.terminal, invocationID)
	if commandID != "" {
		delete(r.byCommand, commandID)
	}
}

func (r *productionTargetedVerifyRegistry) MarkTerminal(invocationID kernel.InvocationID) {
	if r == nil || invocationID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminal[invocationID] = struct{}{}
	delete(r.baselines, invocationID)
	delete(r.failures, invocationID)
}

func (r *productionTargetedVerifyRegistry) MarkFailed(invocationID kernel.InvocationID, cause error) {
	if r == nil || invocationID == "" {
		return
	}
	message := "targeted verify invocation failed"
	if cause != nil {
		message = cause.Error()
	}
	failure := kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: message, Recoverable: true}
	if coded, ok := cause.(kernel.Error); ok {
		failure = coded
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminal[invocationID] = struct{}{}
	r.failures[invocationID] = failure
	delete(r.baselines, invocationID)
}

func (r *productionTargetedVerifyRegistry) IsTerminal(invocationID kernel.InvocationID) bool {
	if r == nil || invocationID == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.terminal[invocationID]
	return ok
}

func (r *productionTargetedVerifyRegistry) FailureByCommand(commandID string) (error, bool) {
	if r == nil || commandID == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for invocationID, currentCommandID := range r.byInvocation {
		if currentCommandID != commandID {
			continue
		}
		if failure, ok := r.failures[invocationID]; ok {
			return failure, true
		}
	}
	return nil, false
}

func (r *productionTargetedVerifyRegistry) FailureByInvocation(invocationID kernel.InvocationID) (error, bool) {
	if r == nil || invocationID == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	failure, ok := r.failures[invocationID]
	if !ok {
		return nil, false
	}
	return failure, true
}

func (r *productionTargetedVerifyRegistry) ActiveInvocations() []kernel.InvocationID {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]kernel.InvocationID, 0, len(r.byInvocation))
	for invocationID := range r.byInvocation {
		if _, done := r.terminal[invocationID]; done {
			continue
		}
		out = append(out, invocationID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (r *productionTargetedVerifyRegistry) SetBaseline(invocationID kernel.InvocationID, root string, files map[string]productionTargetedVerifyFile) error {
	if r == nil || invocationID == "" || strings.TrimSpace(root) == "" {
		return kernel.InvalidArgument("targeted verify baseline identity is required")
	}
	copied := cloneProductionTargetedVerifyFiles(files)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.baselines[invocationID]; ok {
		if existing.Root != root || !sameProductionTargetedVerifyFiles(existing.Files, copied) {
			return kernel.StaleBinding("targeted verify baseline projection changed")
		}
		return nil
	}
	r.baselines[invocationID] = productionTargetedVerifyBaseline{Root: root, Files: copied}
	return nil
}

func (r *productionTargetedVerifyRegistry) Baseline(invocationID kernel.InvocationID) (productionTargetedVerifyBaseline, bool) {
	if r == nil {
		return productionTargetedVerifyBaseline{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	baseline, ok := r.baselines[invocationID]
	baseline.Files = cloneProductionTargetedVerifyFiles(baseline.Files)
	return baseline, ok
}

type productionTargetedVerifyProjectionManifest struct {
	Version            string                 `json:"version"`
	InvocationID       kernel.InvocationID    `json:"invocation_id"`
	CommandID          string                 `json:"command_id"`
	CandidateID        mergequeue.CandidateID `json:"candidate_id"`
	WorkspaceRoot      string                 `json:"workspace_root"`
	LatestMainRevision string                 `json:"latest_main_revision"`
}

type productionTargetedVerifySpec struct {
	Schema             string                                 `json:"schema"`
	Mode               string                                 `json:"mode"`
	EndpointID         kernel.EndpointID                      `json:"endpoint_id"`
	CandidateID        mergequeue.CandidateID                 `json:"candidate_id"`
	TargetRepository   string                                 `json:"target_repository"`
	TargetBranch       string                                 `json:"target_branch"`
	LatestMainRevision string                                 `json:"latest_main_revision"`
	CandidateRevision  string                                 `json:"candidate_revision"`
	VerifyResultRef    evidence.ArtifactID                    `json:"verify_result_ref"`
	DiffArtifactRef    evidence.ArtifactID                    `json:"diff_artifact_ref"`
	AllowedWritePaths  []string                               `json:"allowed_write_paths,omitempty"`
	ConflictPaths      []string                               `json:"conflict_paths,omitempty"`
	Report             productionTargetedVerifyReportContract `json:"report"`
	Rules              []string                               `json:"rules"`
}

type productionTargetedVerifyReportContract struct {
	Schema            string                `json:"schema"`
	ArtifactType      evidence.ArtifactType `json:"artifact_type"`
	ContentType       string                `json:"content_type"`
	RegisterTool      string                `json:"register_tool"`
	RegisterBodyField string                `json:"register_body_field"`
	PhaseOutputTool   string                `json:"phase_output_tool"`
	PhaseOutputRef    string                `json:"phase_output_report_ref_field"`
	VerdictField      string                `json:"verdict_field"`
	Verdicts          []string              `json:"verdicts"`
	ChecksField       string                `json:"checks_field"`
	EvidenceRefs      string                `json:"evidence_refs_field"`
	PassAlias         string                `json:"pass_alias"`
	PassedAlias       string                `json:"passed_alias"`
}

func productionTargetedVerifyPhaseSpec(req mergequeue.TargetedVerifyRequest) productionTargetedVerifySpec {
	return productionTargetedVerifySpec{
		Schema:             productionTargetedVerifySpecSchema,
		Mode:               "latest_main_candidate_targeted_verify",
		EndpointID:         coordination.EndpointVerify,
		CandidateID:        req.Candidate.ID,
		TargetRepository:   req.Candidate.TargetRepository,
		TargetBranch:       req.Candidate.TargetBranch,
		LatestMainRevision: req.LatestMainRevision,
		CandidateRevision:  req.Candidate.CandidateRevision,
		VerifyResultRef:    req.Candidate.VerifyResultRef,
		DiffArtifactRef:    req.Candidate.DiffArtifactRef,
		AllowedWritePaths:  productionTargetedVerifyAuthorizedWritePaths(req),
		ConflictPaths:      productionTargetedVerifyConflictPaths(req),
		Report: productionTargetedVerifyReportContract{
			Schema:            productionTargetedVerifySpecSchema,
			ArtifactType:      evidence.ArtifactGeneratedReport,
			ContentType:       "application/json",
			RegisterTool:      string(auth.ToolEvidenceRegister),
			RegisterBodyField: "body",
			PhaseOutputTool:   string(auth.ToolAgentSubmitPhaseOutput),
			PhaseOutputRef:    "report_ref",
			VerdictField:      "verdict",
			Verdicts:          []string{"pass", "fail"},
			ChecksField:       "checks",
			EvidenceRefs:      "evidence_refs",
			PassAlias:         "pass",
			PassedAlias:       "passed",
		},
		Rules: []string{
			"Run against the prepared latest-main workspace that already contains the candidate changes.",
			"This is a narrow merge-conflict resolution pass, not the normal post-merge acceptance Verify: inspect conflict_paths first and do not re-audit the whole project.",
			"Your only success condition is a conflict-free, syntactically valid working tree that can be merged; the normal post-merge Verify owns task acceptance, regression discovery, and any Manager re-orchestration advice.",
			"Do not try to satisfy the union of the latest-main and candidate test suites or reconcile two complete architectures. Treat the candidate side as authoritative for the assigned task on each conflicted path, retaining only the minimum latest-main integration needed for imports, parsing, or immediately referenced APIs.",
			"Use native read/search/file-edit/shell tools as needed, but keep reads and command output scoped to conflict_paths and their immediate build or test dependencies.",
			"You may edit only paths listed in allowed_write_paths, and only to resolve the listed latest-main conflicts.",
			"Resolve every conflict marker in one focused pass, then run only a syntax/parser check or one smallest directly relevant command; do not launch browser exploration, run the full acceptance suite, or repeat recorded quality checks.",
			"As soon as the workspace is conflict-free and the narrow mechanical check passes, submit verdict=pass. Do not keep investigating product quality; the post-merge Verify will judge it and advise Manager if it is inadequate.",
			"If even that mechanical resolution cannot safely preserve a coherent candidate implementation, immediately call agent.proposeOrchestration with concrete evidence and stop instead of expanding the investigation.",
			"Use context.search only for a specific missing decision or claim; do not retrieve the whole Context Graph, because the invocation receives the union of its subscribed subgraphs already.",
			"Do not commit, push, rewrite history, or write Threadmill graphs.",
			"Register evidence through evidence.register; file changes under evidence/** are ignored by the runtime carrier.",
			"Before submitting PhaseOutput, call evidence.register with type=generated_report, content_type=application/json, and body set to the strict v1 JSON report object only.",
			"The generated_report body must be exactly one JSON object with fields schema, verdict, checks, and evidence_refs; do not register the final report as type=json or type=tool_output, and do not use a file path as report_ref.",
			"Use the artifact id returned by evidence.register(type=generated_report, body=<strict v1 JSON>) as agent.submitPhaseOutput.report_ref.",
			"Set verdict to pass only when the candidate passes on latest main; pass and passed are equivalent acceptor vocabulary, but the canonical verdict value is pass.",
		},
	}
}

func productionTargetedVerifyWorkspaceBinding(req mergequeue.TargetedVerifyRequest) map[string]any {
	return map[string]any{
		"kind":                 "threadmill.targeted_verify.workspace_binding.v1",
		"workspace_ref":        req.WorkspaceRoot,
		"workspace_revision":   req.LatestMainRevision,
		"phase":                workspacePhase(coordination.EndpointVerify),
		"read_only_sources":    true,
		"candidate_id":         req.Candidate.ID,
		"candidate_revision":   req.Candidate.CandidateRevision,
		"latest_main_revision": req.LatestMainRevision,
		"allowed_write_paths":  productionTargetedVerifyAuthorizedWritePaths(req),
		"conflict_paths":       productionTargetedVerifyConflictPaths(req),
	}
}

func productionTargetedVerifyCommand(req mergequeue.TargetedVerifyRequest, binding phasepkg.BindingSnapshot) coordination.PhaseCommand {
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

func productionTargetedVerifyGeneration(req mergequeue.TargetedVerifyRequest) int {
	sum := sha256.Sum256([]byte(string(req.Candidate.ID) + "\x00" + req.LatestMainRevision))
	return int(sum[0]) + 1
}

func productionTargetedVerifyBindingRef(req mergequeue.TargetedVerifyRequest) kernel.BindingRef {
	return kernel.BindingRef(stableRuntimeRef("targeted-verify-binding", req.Candidate.ID, req.Candidate.TaskID, req.LatestMainRevision))
}

func productionTargetedVerifyLeaseRef(req mergequeue.TargetedVerifyRequest) kernel.LeaseID {
	return kernel.LeaseID(stableRuntimeRef("targeted-verify-lease", req.Candidate.ID, req.Candidate.TaskID, req.LatestMainRevision))
}

func productionTargetedVerifyActorPrincipal(req mergequeue.TargetedVerifyRequest) kernel.ActorPrincipalID {
	return kernel.ActorPrincipalID(stableRuntimeRef("targeted-verify-agent", req.Candidate.ProjectID, req.Candidate.TaskID, req.Candidate.ID, req.LatestMainRevision))
}

func productionTargetedVerifyInputRevision(req mergequeue.TargetedVerifyRequest) string {
	raw, _ := stableProductionJSON(map[string]any{
		"candidate_id":         req.Candidate.ID,
		"candidate_revision":   req.Candidate.CandidateRevision,
		"latest_main_revision": req.LatestMainRevision,
		"verify_result_ref":    req.Candidate.VerifyResultRef,
		"diff_artifact_ref":    req.Candidate.DiffArtifactRef,
		"candidate_evidence":   req.Candidate.EvidenceRefs,
	})
	sum := sha256.Sum256(raw)
	return "targeted-verify-inputs:" + hex.EncodeToString(sum[:16])
}

func productionTargetedVerifyEvidenceStrings(refs []evidence.ArtifactID) []string {
	out := make([]string, 0, len(refs)+2)
	for _, ref := range refs {
		if ref != "" {
			out = append(out, string(ref))
		}
	}
	sort.Strings(out)
	return out
}

func validateProductionTargetedVerifyRequest(req mergequeue.TargetedVerifyRequest) error {
	if req.Candidate.ID == "" || req.Candidate.ProjectID == "" || req.Candidate.TaskID == "" {
		return kernel.InvalidArgument("targeted verify candidate identity is required")
	}
	if strings.TrimSpace(req.WorkspaceRoot) == "" || strings.TrimSpace(req.LatestMainRevision) == "" || strings.TrimSpace(req.Candidate.TargetRepository) == "" || strings.TrimSpace(req.Candidate.CandidateRevision) == "" {
		return kernel.InvalidArgument("targeted verify workspace, target repository, latest main, and candidate revision are required")
	}
	if req.Candidate.VerifyResultRef == "" || req.Candidate.DiffArtifactRef == "" {
		return kernel.InvalidArgument("targeted verify candidate evidence refs are required")
	}
	return nil
}

func decodeProductionTargetedVerifyProjectionManifest(raw []byte) (productionTargetedVerifyProjectionManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest productionTargetedVerifyProjectionManifest
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return productionTargetedVerifyProjectionManifest{}, kernel.InvalidArgument("targeted verify projection manifest is invalid")
	}
	if manifest.Version != productionTargetedVerifyProjectionVersion || manifest.InvocationID == "" || manifest.CommandID == "" || manifest.CandidateID == "" || manifest.WorkspaceRoot == "" || manifest.LatestMainRevision == "" {
		return productionTargetedVerifyProjectionManifest{}, kernel.InvalidArgument("targeted verify projection manifest is incomplete")
	}
	return manifest, nil
}

func safeProductionTargetedVerifyRoot(root string) (string, error) {
	abs, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", kernel.Forbidden("targeted verify workspace root is invalid")
	}
	return filepath.Clean(resolved), nil
}

func readProductionTargetedVerifyFiles(root string) ([]adapter.ExecutionFile, map[string]productionTargetedVerifyFile, error) {
	var files []adapter.ExecutionFile
	baseline := make(map[string]productionTargetedVerifyFile)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() && name == ".git" {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || info.Mode().Type() != 0 {
			return kernel.Forbidden("targeted verify workspace contains unsupported file type")
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if err := validateProductionTargetedVerifyRelPath(rel); err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])
		mode := uint32(info.Mode().Perm())
		files = append(files, adapter.ExecutionFile{Path: rel, Mode: mode, Content: body, SHA256: sha})
		if !productionTargetedVerifyEvidencePath(rel) {
			baseline[rel] = productionTargetedVerifyFile{Mode: productionTargetedVerifyComparableMode(mode), SHA256: sha}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, baseline, nil
}

func validateProductionTargetedVerifyReturnedFiles(baseline productionTargetedVerifyBaseline, files []adapter.ExecutionFile) error {
	return validateProductionTargetedVerifyReturnedFileSet(baseline, files, nil)
}

func validateProductionTargetedVerifyReturnedFileSet(baseline productionTargetedVerifyBaseline, files []adapter.ExecutionFile, allowed []string) error {
	seen := make(map[string]productionTargetedVerifyFile, len(files))
	bodies := make(map[string][]byte, len(files))
	for _, file := range files {
		rel := strings.TrimSpace(strings.ReplaceAll(file.Path, "\\", "/"))
		if err := validateProductionTargetedVerifyRelPath(rel); err != nil {
			return err
		}
		if productionTargetedVerifyEvidencePath(rel) {
			continue
		}
		if file.Mode&^0o777 != 0 {
			return kernel.Forbidden("targeted verify returned unsupported file mode")
		}
		sum := sha256.Sum256(file.Content)
		sha := strings.ToLower(hex.EncodeToString(sum[:]))
		if strings.ToLower(file.SHA256) != sha {
			return kernel.InvalidArgument("targeted verify returned file hash is invalid")
		}
		if _, dup := seen[rel]; dup {
			return kernel.InvalidArgument("targeted verify returned duplicate source file")
		}
		seen[rel] = productionTargetedVerifyFile{Mode: productionTargetedVerifyComparableMode(file.Mode), SHA256: sha}
		bodies[rel] = append([]byte(nil), file.Content...)
	}
	allowedSet := productionTargetedVerifyAllowedPathSet(allowed)
	if len(allowedSet) == 0 {
		if !sameProductionTargetedVerifyFiles(baseline.Files, seen) {
			return kernel.Forbidden("targeted verify cannot modify, add, or delete source/config files")
		}
		return nil
	}
	for rel, original := range baseline.Files {
		current, ok := seen[rel]
		if !ok {
			if !allowedSet.Allows(rel) {
				return kernel.Forbidden("targeted verify cannot delete unauthorized source/config files")
			}
			continue
		}
		if current != original && !allowedSet.Allows(rel) {
			return kernel.Forbidden("targeted verify cannot modify unauthorized source/config files")
		}
	}
	for rel := range seen {
		if _, existed := baseline.Files[rel]; !existed && !allowedSet.Allows(rel) {
			return kernel.Forbidden("targeted verify cannot add unauthorized source/config files")
		}
	}
	_ = bodies
	return nil
}

func applyProductionTargetedVerifyReturnedFiles(ctx context.Context, root string, baseline productionTargetedVerifyBaseline, files []adapter.ExecutionFile, allowed []string) error {
	returned, err := decodeProductionTargetedVerifyReturnedFiles(files)
	if err != nil {
		return err
	}
	if err := validateProductionTargetedVerifyReturnedFileSet(baseline, files, allowed); err != nil {
		return err
	}
	allowedSet := productionTargetedVerifyAllowedPathSet(allowed)
	if len(allowedSet) == 0 {
		return nil
	}
	for rel := range baseline.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := returned[rel]; ok {
			continue
		}
		if !allowedSet.Allows(rel) {
			continue
		}
		if err := removeProductionTargetedVerifyFile(root, rel); err != nil {
			return err
		}
	}
	keys := make([]string, 0, len(returned))
	for rel := range returned {
		keys = append(keys, rel)
	}
	sort.Strings(keys)
	for _, rel := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		file := returned[rel]
		if baselineFile, ok := baseline.Files[rel]; ok && baselineFile == (productionTargetedVerifyFile{Mode: file.Mode, SHA256: file.SHA256}) {
			continue
		}
		if !allowedSet.Allows(rel) {
			continue
		}
		if err := writeProductionTargetedVerifyFile(root, rel, file); err != nil {
			return err
		}
	}
	return nil
}

type productionTargetedVerifyReturnedFile struct {
	Mode    uint32
	SHA256  string
	Content []byte
}

func decodeProductionTargetedVerifyReturnedFiles(files []adapter.ExecutionFile) (map[string]productionTargetedVerifyReturnedFile, error) {
	out := make(map[string]productionTargetedVerifyReturnedFile, len(files))
	for _, file := range files {
		rel := strings.TrimSpace(strings.ReplaceAll(file.Path, "\\", "/"))
		if err := validateProductionTargetedVerifyRelPath(rel); err != nil {
			return nil, err
		}
		if productionTargetedVerifyEvidencePath(rel) {
			continue
		}
		if file.Mode&^0o777 != 0 {
			return nil, kernel.Forbidden("targeted verify returned unsupported file mode")
		}
		sum := sha256.Sum256(file.Content)
		sha := strings.ToLower(hex.EncodeToString(sum[:]))
		if strings.ToLower(file.SHA256) != sha {
			return nil, kernel.InvalidArgument("targeted verify returned file hash is invalid")
		}
		if _, dup := out[rel]; dup {
			return nil, kernel.InvalidArgument("targeted verify returned duplicate source file")
		}
		out[rel] = productionTargetedVerifyReturnedFile{Mode: file.Mode, SHA256: sha, Content: append([]byte(nil), file.Content...)}
	}
	return out, nil
}

func writeProductionTargetedVerifyFile(root, rel string, file productionTargetedVerifyReturnedFile) error {
	target, err := productionTargetedVerifySafeWriteTarget(root, rel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return kernel.Forbidden("targeted verify cannot write through symlink paths")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".threadmill-targeted-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(file.Content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(os.FileMode(file.Mode & 0o777)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, target); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func removeProductionTargetedVerifyFile(root, rel string) error {
	target, err := productionTargetedVerifySafeWriteTarget(root, rel)
	if err != nil {
		return err
	}
	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return kernel.Forbidden("targeted verify cannot remove symlink paths")
		}
		return os.Remove(target)
	} else if os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func productionTargetedVerifySafeWriteTarget(root, rel string) (string, error) {
	if err := validateProductionTargetedVerifyRelPath(rel); err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	target := filepath.Clean(filepath.Join(root, filepath.FromSlash(rel)))
	relToRoot, err := filepath.Rel(root, target)
	if err != nil || relToRoot == ".." || strings.HasPrefix(filepath.ToSlash(relToRoot), "../") {
		return "", kernel.Forbidden("targeted verify workspace path escapes root")
	}
	current := root
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return target, productionTargetedVerifyEnsureParentSafe(root, filepath.Dir(target))
			}
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", kernel.Forbidden("targeted verify cannot write through symlink paths")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return "", kernel.Forbidden("targeted verify workspace path parent is not a directory")
		}
	}
	return target, nil
}

func productionTargetedVerifyEnsureParentSafe(root, parent string) error {
	rel, err := filepath.Rel(root, parent)
	if err != nil || rel == ".." || strings.HasPrefix(filepath.ToSlash(rel), "../") {
		return kernel.Forbidden("targeted verify workspace path escapes root")
	}
	if rel == "." {
		return nil
	}
	current := root
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return kernel.Forbidden("targeted verify cannot write through symlink paths")
			}
			if !info.IsDir() {
				return kernel.Forbidden("targeted verify workspace path parent is not a directory")
			}
			continue
		}
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

func productionTargetedVerifyHead(ctx context.Context, root string) (string, bool, error) {
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	cmd := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", false, kernel.InvalidArgument("targeted verify git HEAD cannot be read")
	}
	head := strings.TrimSpace(string(out))
	if head == "" {
		return "", false, kernel.InvalidArgument("targeted verify git HEAD is empty")
	}
	return head, true, nil
}

type productionTargetedVerifyAllowedPaths []string

func productionTargetedVerifyAllowedPathSet(paths []string) productionTargetedVerifyAllowedPaths {
	out := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		raw := strings.TrimSpace(strings.ReplaceAll(path, "\\", "/"))
		dir := strings.HasSuffix(raw, "/")
		clean := filepath.ToSlash(filepath.Clean(strings.Trim(raw, "/")))
		if clean == "." || clean == "" || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			continue
		}
		if dir {
			clean += "/"
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	sort.Strings(out)
	return productionTargetedVerifyAllowedPaths(out)
}

func (paths productionTargetedVerifyAllowedPaths) Allows(rel string) bool {
	rel = strings.Trim(strings.ReplaceAll(rel, "\\", "/"), "/")
	for _, allowed := range paths {
		if strings.HasSuffix(allowed, "/") {
			if strings.HasPrefix(rel, strings.TrimSuffix(allowed, "/")+"/") {
				return true
			}
			continue
		}
		if rel == allowed {
			return true
		}
	}
	return false
}

func productionTargetedVerifyAuthorizedWritePaths(req mergequeue.TargetedVerifyRequest) []string {
	return productionTargetedVerifyStringSliceFields(req, "AllowedWritePaths", "AuthorizedWritePaths", "WritablePaths", "AllowedPaths")
}

func productionTargetedVerifyConflictPaths(req mergequeue.TargetedVerifyRequest) []string {
	return productionTargetedVerifyStringSliceFields(req, "ConflictPaths", "ConflictedPaths")
}

func productionTargetedVerifyStringSliceFields(req mergequeue.TargetedVerifyRequest, names ...string) []string {
	value := reflect.ValueOf(req)
	out := make([]string, 0)
	for _, name := range names {
		field := value.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		switch field.Kind() {
		case reflect.Slice, reflect.Array:
			for i := 0; i < field.Len(); i++ {
				item := field.Index(i)
				if item.Kind() == reflect.String {
					out = append(out, item.String())
				}
			}
		}
	}
	return []string(productionTargetedVerifyAllowedPathSet(out))
}

func validateProductionTargetedVerifyRelPath(rel string) error {
	clean := filepath.ToSlash(filepath.Clean(rel))
	if rel == "" || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") || clean != rel {
		return kernel.Forbidden("targeted verify workspace path is invalid")
	}
	return nil
}

func productionTargetedVerifyEvidencePath(rel string) bool {
	rel = strings.Trim(strings.ReplaceAll(rel, "\\", "/"), "/")
	return rel == "evidence" || strings.HasPrefix(rel, "evidence/")
}

func cloneProductionTargetedVerifyFiles(in map[string]productionTargetedVerifyFile) map[string]productionTargetedVerifyFile {
	out := make(map[string]productionTargetedVerifyFile, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sameProductionTargetedVerifyFiles(a, b map[string]productionTargetedVerifyFile) bool {
	if len(a) != len(b) {
		return false
	}
	for key, av := range a {
		if bv, ok := b[key]; !ok || av != bv {
			return false
		}
	}
	return true
}

func sameProductionTargetedVerifyEntry(a, b productionTargetedVerifyEntry) bool {
	araw, _ := json.Marshal(a)
	braw, _ := json.Marshal(b)
	return bytes.Equal(araw, braw)
}

var _ mergeintegration.BindingRegistrar = (*productionTargetedVerifyBindingSource)(nil)
var _ mergeintegration.PhaseRuntime = (*productionTargetedVerifyPhaseRuntime)(nil)
var _ mcpapi.PhaseRuntime = (*productionTargetedVerifyPhaseRuntime)(nil)
var _ mcpapi.OrchestrationProposalRuntime = (*productionTargetedVerifyPhaseRuntime)(nil)
var _ phasepkg.BaseBindingSource = (*productionTargetedVerifyBindingSource)(nil)
var _ phasepkg.InputRuntime = productionTargetedVerifyInputs{}
var _ adapter.ExecutionFileProjector = (*productionTargetedVerifyWorkspaceProjector)(nil)
var _ adapter.ExecutionFileProjector = productionTargetedVerifyProjectorRouter{}
var _ mcpapi.PhaseRuntime = productionTargetedVerifyRuntimeRouter{}
var _ mcpapi.OrchestrationProposalRuntime = productionTargetedVerifyRuntimeRouter{}
