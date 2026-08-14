package coordination

import (
	"context"
	"errors"
	"sort"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type graphRuntime struct {
	projectID               kernel.ProjectID
	store                   graphRuntimeStore
	phaseController         PhaseController
	selectionRuntime        RuntimeSelectionRuntime
	schedulingStateProvider RuntimeSchedulingStateProvider
}

type graphRuntimeStore interface {
	loadRuntimeView(context.Context, kernel.ProjectID) (runtimeView, error)
	markCommandObserved(context.Context, kernel.ProjectID, phaseObservation) error
	completeCommandAndReleaseLease(context.Context, kernel.ProjectID, phaseObservation) error
	releaseOrphanLease(context.Context, kernel.ProjectID, kernel.LeaseID) error
	getOrCreateStopCommand(context.Context, kernel.ProjectID, kernel.Revision, phaseLease) (PhaseCommand, bool, error)
	claimLeaseAndAppendCommand(context.Context, kernel.ProjectID, kernel.Revision, PhaseEndpoint, CommandAction, string) (PhaseCommand, bool, error)
	markCommandAccepted(context.Context, kernel.ProjectID, string)
	scheduleRetry(context.Context, kernel.ProjectID, string)
	quarantineCommand(context.Context, kernel.ProjectID, string)
	rejectCommand(context.Context, kernel.ProjectID, PhaseCommand, error)
	recordEndpointDispatchRejection(context.Context, kernel.ProjectID, PhaseEndpointRef, int, kernel.BindingRef, error)
}

func newGraphRuntime(projectID kernel.ProjectID, store graphRuntimeStore, controller PhaseController) *graphRuntime {
	return &graphRuntime{
		projectID:               projectID,
		store:                   store,
		phaseController:         controller,
		selectionRuntime:        fixedCapacitySelectionRuntime{},
		schedulingStateProvider: fixedSchedulingStateProvider{state: RuntimeSchedulingState{Capacity: RuntimeCapacity{Desired: 1, Healthy: 1}}},
	}
}

func (r *graphRuntime) reconcile(ctx context.Context) error {
	view, err := r.store.loadRuntimeView(ctx, r.projectID)
	if err != nil {
		return err
	}
	view, err = r.foldObservations(ctx, view)
	if err != nil {
		return err
	}
	if err := r.repairDecisionPairs(ctx, &view); err != nil {
		return err
	}
	schedulingState, err := r.schedulingState(ctx, view)
	if err != nil {
		return err
	}

	deliver := make([]PhaseCommand, 0)
	for _, lease := range view.activeLeases() {
		if !r.mustStop(view, lease) {
			continue
		}
		cmd, deliverable, err := r.store.getOrCreateStopCommand(ctx, r.projectID, view.revision, lease)
		if err != nil {
			return err
		}
		view.suppressRun(lease.Endpoint, lease.Generation)
		if deliverable {
			deliver = append(deliver, cmd)
		}
	}

	for _, cmd := range view.pendingCommands() {
		if cmd.Action != CommandStop && view.runSuppressed(cmd.Endpoint, cmd.Generation) {
			continue
		}
		deliver = append(deliver, cmd)
	}

	candidates := runtimeCandidates(r.graphRunnable(view))
	for len(candidates) > 0 {
		selected, err := r.selectionRuntime.SelectRunnable(ctx, RuntimeSelectionRequest{
			Candidates: candidates,
			Capacity:   schedulingState.Capacity,
			Budget:     schedulingState.Budget,
		})
		if err != nil {
			return err
		}
		if len(selected) == 0 {
			break
		}
		selectedRefs := make(map[PhaseEndpointRef]struct{}, len(selected))
		claimedAny := false
		for _, endpoint := range selected {
			selectedRefs[endpoint.Ref] = struct{}{}
			cmd, claimed, err := r.claimRun(ctx, view, endpoint)
			if err != nil {
				if kernel.IsCode(err, kernel.CodeStaleCheckpoint) {
					r.store.recordEndpointDispatchRejection(ctx, r.projectID, endpoint.Ref, endpoint.Generation, endpoint.BindingRef, err)
					continue
				}
				return err
			}
			if claimed {
				claimedAny = true
				deliver = append(deliver, cmd)
			}
		}
		if claimedAny {
			break
		}
		candidates = removeSelectedCandidates(candidates, selectedRefs)
	}

	for _, cmd := range uniqueByCommandID(deliver) {
		r.deliver(ctx, cmd)
	}
	return nil
}

func (r *graphRuntime) schedulingState(ctx context.Context, view runtimeView) (RuntimeSchedulingState, error) {
	state, err := r.schedulingStateProvider.RuntimeSchedulingState(ctx)
	if err != nil {
		return RuntimeSchedulingState{}, err
	}
	state.Capacity.Active = len(view.activeLeases())
	return state, nil
}

func (r *graphRuntime) foldObservations(ctx context.Context, view runtimeView) (runtimeView, error) {
	for _, event := range view.unfoldedObservations() {
		switch event.Kind {
		case "PhaseInvocationStarted":
			if err := r.store.markCommandObserved(ctx, r.projectID, event); err != nil {
				return runtimeView{}, err
			}
		case "PhaseOutputSubmitted", "PhaseInvocationFailed":
			if err := r.store.completeCommandAndReleaseLease(ctx, r.projectID, event); err != nil {
				return runtimeView{}, err
			}
		case "PhaseInvocationStopped":
			if event.CheckpointRef == "" && !event.NonResumable {
				return runtimeView{}, kernel.IncompleteStopEvidence("stopped observation requires checkpoint_ref or non_resumable")
			}
			if err := r.store.completeCommandAndReleaseLease(ctx, r.projectID, event); err != nil {
				return runtimeView{}, err
			}
		}
	}
	return r.store.loadRuntimeView(ctx, r.projectID)
}

func (r *graphRuntime) repairDecisionPairs(ctx context.Context, view *runtimeView) error {
	for _, lease := range view.activeLeases() {
		if view.hasCommandForLease(lease.LeaseRef) {
			continue
		}
		if view.hasRemoteObservation(lease) {
			cmd, _, err := r.store.getOrCreateStopCommand(ctx, r.projectID, view.revision, lease)
			if err != nil {
				return err
			}
			view.commands[cmd.ID] = commandRecord{Command: cmd}
			continue
		}
		if err := r.store.releaseOrphanLease(ctx, r.projectID, lease.LeaseRef); err != nil {
			return err
		}
	}
	for id, record := range view.commands {
		if record.Command.Action == CommandStop || record.CompletedEventRef != "" || record.Quarantined || record.NotExecutable {
			continue
		}
		if !view.hasMatchingActiveLease(record.Command) {
			record.NotExecutable = true
			view.commands[id] = record
			r.store.quarantineCommand(ctx, r.projectID, id)
		}
	}
	refreshed, err := r.store.loadRuntimeView(ctx, r.projectID)
	if err != nil {
		return err
	}
	*view = refreshed
	return nil
}

func (r *graphRuntime) mustStop(view runtimeView, lease phaseLease) bool {
	endpoint, ok := view.endpoint(lease.Endpoint)
	if !ok {
		return true
	}
	task, ok := view.task(lease.Endpoint.TaskID)
	if !ok {
		return true
	}
	return task.Outcome != TaskActive ||
		endpoint.RunPolicy == RunHeld ||
		endpoint.Generation != lease.Generation ||
		endpoint.BindingRef != lease.BindingRef ||
		view.leaseExpired(lease)
}

func (r *graphRuntime) graphRunnable(view runtimeView) []PhaseEndpoint {
	candidates := make([]PhaseEndpoint, 0)
	for _, endpoint := range view.endpoints {
		if view.isRunnable(endpoint) {
			candidates = append(candidates, endpoint)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].Ref
		right := candidates[j].Ref
		if phaseOrder(left.EndpointID) == phaseOrder(right.EndpointID) {
			return left.TaskID < right.TaskID
		}
		return phaseOrder(left.EndpointID) > phaseOrder(right.EndpointID)
	})
	return candidates
}

func (r *graphRuntime) claimRun(ctx context.Context, view runtimeView, endpoint PhaseEndpoint) (PhaseCommand, bool, error) {
	binding := view.binding(endpoint.BindingRef)
	action := CommandStart
	if binding.NonResumable {
		return PhaseCommand{}, false, kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "binding is non-resumable", Recoverable: true}
	}
	if binding.CheckpointRef != "" {
		if !view.checkpointCompatible(endpoint.BindingRef, binding) {
			return PhaseCommand{}, false, kernel.Error{Code: kernel.CodeStaleCheckpoint, Message: "checkpoint is not compatible with binding", Recoverable: true}
		}
		action = CommandResume
	}
	return r.store.claimLeaseAndAppendCommand(ctx, r.projectID, view.revision, endpoint, action, view.revisionRef())
}

func (r *graphRuntime) deliver(ctx context.Context, cmd PhaseCommand) {
	if r.phaseController == nil {
		r.store.scheduleRetry(ctx, r.projectID, cmd.ID)
		return
	}
	err := r.phaseController.Apply(ctx, cmd)
	switch {
	case err == nil:
		r.store.markCommandAccepted(ctx, r.projectID, cmd.ID)
	case kernel.IsCode(err, kernel.CodeExecutorUnavailable):
		r.store.scheduleRetry(ctx, r.projectID, cmd.ID)
	case kernel.IsCode(err, kernel.CodeStaleCommand), kernel.IsCode(err, kernel.CodeLeaseConflict):
		r.store.rejectCommand(ctx, r.projectID, cmd, err)
	case kernel.IsCode(err, kernel.CodeStaleCheckpoint):
		r.store.rejectCommand(ctx, r.projectID, cmd, err)
	case kernel.IsCode(err, kernel.CodeCommandConflict):
		r.store.quarantineCommand(ctx, r.projectID, cmd.ID)
	default:
		r.store.scheduleRetry(ctx, r.projectID, cmd.ID)
	}
}

func selectRuntimeEndpoints(candidates []PhaseEndpoint, capacity int) []PhaseEndpoint {
	if capacity <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) <= capacity {
		return append([]PhaseEndpoint(nil), candidates...)
	}
	return append([]PhaseEndpoint(nil), candidates[:capacity]...)
}

func runtimeCandidates(endpoints []PhaseEndpoint) []RuntimeCandidate {
	candidates := make([]RuntimeCandidate, 0, len(endpoints))
	for _, endpoint := range endpoints {
		candidates = append(candidates, RuntimeCandidate{
			Endpoint:          endpoint,
			Runnable:          true,
			CapacityCost:      1,
			CapabilityMatched: true,
		})
	}
	return candidates
}

func runtimeCandidateEndpoints(candidates []RuntimeCandidate) []PhaseEndpoint {
	endpoints := make([]PhaseEndpoint, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Runnable {
			endpoints = append(endpoints, candidate.Endpoint)
		}
	}
	return endpoints
}

func removeSelectedCandidates(candidates []RuntimeCandidate, selected map[PhaseEndpointRef]struct{}) []RuntimeCandidate {
	remaining := candidates[:0]
	for _, candidate := range candidates {
		if _, ok := selected[candidate.Endpoint.Ref]; ok {
			continue
		}
		remaining = append(remaining, candidate)
	}
	return remaining
}

func uniqueByCommandID(commands []PhaseCommand) []PhaseCommand {
	seen := make(map[string]struct{}, len(commands))
	out := make([]PhaseCommand, 0, len(commands))
	for _, command := range commands {
		if _, ok := seen[command.ID]; ok {
			continue
		}
		seen[command.ID] = struct{}{}
		out = append(out, command)
	}
	return out
}

func commandError(code kernel.ErrorCode, message string) error {
	return kernel.Error{Code: code, Message: message, Recoverable: true}
}

func isRetryableCommandError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || kernel.IsCode(err, kernel.CodeExecutorUnavailable)
}
