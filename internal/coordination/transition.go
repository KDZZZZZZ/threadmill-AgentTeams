package coordination

import (
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func applyTransition(state *graphState, transition GraphTransition) error {
	switch transition.TargetKind {
	case TargetPhaseEndpoint:
		return applyEndpointTransition(state, transition)
	case TargetBlocker:
		return applyBlockerTransition(state, transition)
	case TargetTask:
		return applyTaskTransition(state, transition)
	default:
		return kernel.InvalidArgument("transition target kind is not allowed")
	}
}

func applyEndpointTransition(state *graphState, transition GraphTransition) error {
	endpoint, ok := state.endpoints[transition.Endpoint]
	if !ok {
		return kernel.InvalidArgument("phase endpoint transition references unknown endpoint")
	}
	if transition.Generation != endpoint.Generation {
		return kernel.TransitionRejected("phase endpoint transition generation does not match current endpoint generation")
	}
	switch transition.Action {
	case string(EndpointSubmitted):
		if endpoint.State != EndpointPending {
			return kernel.TransitionRejected("submitted transition requires pending endpoint")
		}
		if err := requireCompletionInputs(state, endpoint.Ref); err != nil {
			return err
		}
		endpoint.State = EndpointSubmitted
	case string(EndpointSatisfied):
		if endpoint.RunPolicy == RunHeld {
			return kernel.TransitionRejected("satisfied transition requires endpoint not held")
		}
		if endpoint.State != EndpointSubmitted {
			return kernel.TransitionRejected("satisfied transition requires submitted endpoint")
		}
		if err := requireResultForTransition(state, endpoint, transition, VerdictSatisfied); err != nil {
			return err
		}
		endpoint.State = EndpointSatisfied
	case string(EndpointRejected):
		if endpoint.RunPolicy == RunHeld {
			return kernel.TransitionRejected("rejected transition requires endpoint not held")
		}
		if endpoint.State != EndpointSubmitted {
			return kernel.TransitionRejected("rejected transition requires submitted endpoint")
		}
		if err := requireResultForTransition(state, endpoint, transition, VerdictRejected); err != nil {
			return err
		}
		endpoint.State = EndpointRejected
	case "reopened":
		if endpoint.State != EndpointRejected && endpoint.State != EndpointPending {
			return kernel.TransitionRejected("reopened transition requires rejected or pending endpoint")
		}
		if err := kernel.RequireID("new_binding_ref", transition.NewBindingRef); err != nil {
			return err
		}
		if transition.NewBindingRef == endpoint.BindingRef {
			return kernel.StaleBinding("reopened transition requires a new binding_ref")
		}
		endpoint.State = EndpointPending
		endpoint.Generation++
		endpoint.BindingRef = transition.NewBindingRef
		if transition.NewSpecRef != "" {
			endpoint.SpecRef = transition.NewSpecRef
		}
	case "held":
		endpoint.RunPolicy = RunHeld
	case "released":
		if endpoint.RunPolicy != RunHeld {
			return kernel.TransitionRejected("released transition requires held endpoint")
		}
		endpoint.RunPolicy = RunEnabled
	case "stopped":
		if endpoint.RunPolicy != RunHeld {
			return kernel.TransitionRejected("stopped transition requires held endpoint")
		}
		if err := requireStopEvidence(endpoint, transition); err != nil {
			return err
		}
		endpoint.State = EndpointPending
		endpoint.Generation++
		endpoint.BindingRef = transition.NewBindingRef
		if transition.NewSpecRef != "" {
			endpoint.SpecRef = transition.NewSpecRef
		}
	default:
		return kernel.InvalidArgument("phase endpoint transition action is not allowed")
	}
	state.endpoints[transition.Endpoint] = endpoint
	return nil
}

// requireCompletionInputs is the authoritative completion join. Start inputs
// decide whether Runtime may create an Invocation; completion inputs may
// arrive while that Invocation is already running and therefore gate the
// PhaseOutput submitted transition instead. They are never inferred from an
// agent summary or an undeclared artifact.
func requireCompletionInputs(state *graphState, target PhaseEndpointRef) error {
	for _, edge := range state.edges {
		if edge.To != target || edge.RequiredBy != RequiredByCompletion {
			continue
		}
		satisfied := false
		switch edge.Signal {
		case SignalPhaseSatisfied:
			source, ok := state.endpoints[edge.From]
			satisfied = ok && source.State == EndpointSatisfied
		case SignalTaskDone:
			source, ok := state.tasks[edge.From.TaskID]
			satisfied = ok && source.Outcome == TaskDone
		}
		if !satisfied {
			return kernel.TransitionRejected("submitted transition is waiting for a declared completion edge")
		}
	}
	for _, blocker := range state.blockers {
		if blocker.Target != target || blocker.RequiredBy != RequiredByCompletion {
			continue
		}
		switch blocker.State {
		case BlockerResolved, BlockerObsolete:
			continue
		case BlockerActive, BlockerDenied:
			return kernel.TransitionRejected("submitted transition is waiting for a declared completion blocker")
		}
	}
	return nil
}

// validateRuntimeBackedTransition prevents a normal pending endpoint from
// being arbitrarily rolled to a new generation. The only enabled-pending
// reopen path is an exact Runtime-authenticated failed invocation observation;
// rejected and operator-held endpoints retain their existing business paths.
func validateRuntimeBackedTransition(state graphState, runtime memoryRuntimeState, transition GraphTransition) error {
	if transition.TargetKind != TargetPhaseEndpoint {
		return nil
	}
	endpoint, ok := state.endpoints[transition.Endpoint]
	if !ok {
		return nil
	}
	if transition.Action == string(EndpointSubmitted) && endpoint.RunPolicy == RunHeld {
		if hasExactTerminalObservation(runtime, endpoint, phaseObservationOutput) {
			return nil
		}
		return kernel.TransitionRejected("held endpoint submission requires an exact PhaseOutputSubmitted observation")
	}
	if transition.Action != "reopened" || endpoint.State != EndpointPending || endpoint.RunPolicy == RunHeld {
		return nil
	}
	if len(transition.EvidenceRefs) == 0 {
		return kernel.TransitionRejected("failed invocation reopen requires evidence_refs")
	}
	for _, observation := range runtime.observations {
		if observation.Kind != phaseObservationFailed || observation.Endpoint != endpoint.Ref ||
			observation.Generation != endpoint.Generation || observation.BindingRef != endpoint.BindingRef ||
			observation.CommandID == "" || observation.LeaseRef == "" {
			continue
		}
		record, commandFound := runtime.commands[observation.CommandID]
		lease, leaseFound := runtime.leases[observation.LeaseRef]
		if !commandFound || !leaseFound || !record.Accepted || !isRunCommand(record.Command) {
			continue
		}
		if record.Command.Endpoint != observation.Endpoint || record.Command.Generation != observation.Generation ||
			record.Command.BindingRef != observation.BindingRef || record.Command.LeaseRef != observation.LeaseRef ||
			lease.Endpoint != observation.Endpoint || lease.Generation != observation.Generation || lease.BindingRef != observation.BindingRef {
			continue
		}
		return nil
	}
	return kernel.TransitionRejected("enabled pending reopen requires an exact PhaseInvocationFailed observation")
}

// hasExactTerminalObservation recognizes the winner of the Runtime's
// output-vs-stop race. A later hold must not discard an output that the Phase
// Controller already accepted and durably tied to the current command, lease,
// generation, and binding. The endpoint remains held, so no new execution is
// scheduled until Task Manager explicitly releases it.
func hasExactTerminalObservation(runtime memoryRuntimeState, endpoint PhaseEndpoint, kind string) bool {
	for _, observation := range runtime.observations {
		if observation.Kind != kind || observation.Endpoint != endpoint.Ref ||
			observation.Generation != endpoint.Generation || observation.BindingRef != endpoint.BindingRef ||
			observation.CommandID == "" || observation.LeaseRef == "" {
			continue
		}
		record, commandFound := runtime.commands[observation.CommandID]
		lease, leaseFound := runtime.leases[observation.LeaseRef]
		if !commandFound || !leaseFound || (record.Command.Action != CommandStart && record.Command.Action != CommandResume) {
			continue
		}
		if record.Command.Endpoint != observation.Endpoint || record.Command.Generation != observation.Generation ||
			record.Command.BindingRef != observation.BindingRef || record.Command.LeaseRef != observation.LeaseRef ||
			lease.Endpoint != observation.Endpoint || lease.Generation != observation.Generation || lease.BindingRef != observation.BindingRef {
			continue
		}
		return true
	}
	return false
}

func applyBlockerTransition(state *graphState, transition GraphTransition) error {
	blocker, ok := state.blockers[transition.BlockerID]
	if !ok {
		return kernel.InvalidArgument("blocker transition references unknown blocker")
	}
	switch transition.Action {
	case string(BlockerResolved):
		if blocker.State != BlockerActive {
			return kernel.TransitionRejected("resolved transition requires active blocker")
		}
		blocker.State = BlockerResolved
	case string(BlockerDenied):
		if blocker.State != BlockerActive {
			return kernel.TransitionRejected("denied transition requires active blocker")
		}
		blocker.State = BlockerDenied
	case string(BlockerObsolete):
		if blocker.State != BlockerActive {
			return kernel.TransitionRejected("obsolete transition requires active blocker")
		}
		blocker.State = BlockerObsolete
	default:
		return kernel.InvalidArgument("blocker transition action is not allowed")
	}
	state.blockers[transition.BlockerID] = blocker
	return nil
}

func applyTaskTransition(state *graphState, transition GraphTransition) error {
	task, ok := state.tasks[transition.TaskID]
	if !ok {
		return kernel.InvalidArgument("task transition references unknown task")
	}
	switch transition.Action {
	case string(TaskDone):
		if task.Outcome != TaskActive {
			return kernel.TransitionRejected("done transition requires active task")
		}
		if err := requireVerifySatisfied(state, transition.TaskID); err != nil {
			return err
		}
		task.Outcome = TaskDone
	case string(TaskCanceled):
		if task.Outcome != TaskActive {
			return kernel.TransitionRejected("canceled transition requires active task")
		}
		task.Outcome = TaskCanceled
	case string(TaskFailed):
		if task.Outcome != TaskActive {
			return kernel.TransitionRejected("failed transition requires active task")
		}
		task.Outcome = TaskFailed
	case "reopen_round":
		if task.Outcome != TaskActive {
			return kernel.TransitionRejected("reopen_round transition requires active task")
		}
		if err := reopenExecutionRound(state, transition); err != nil {
			return err
		}
	default:
		return kernel.InvalidArgument("task transition action is not allowed")
	}
	state.tasks[transition.TaskID] = task
	return nil
}

func reopenExecutionRound(state *graphState, transition GraphTransition) error {
	planRef := PhaseEndpointRef{TaskID: transition.TaskID, EndpointID: EndpointPlan}
	executeRef := PhaseEndpointRef{TaskID: transition.TaskID, EndpointID: EndpointExecute}
	verifyRef := PhaseEndpointRef{TaskID: transition.TaskID, EndpointID: EndpointVerify}
	plan, planOK := state.endpoints[planRef]
	execute, executeOK := state.endpoints[executeRef]
	verify, verifyOK := state.endpoints[verifyRef]
	if !planOK || !executeOK || !verifyOK {
		return kernel.InvalidGraph("reopen_round requires the fixed plan/execute/verify endpoint set")
	}
	if plan.State != EndpointSatisfied {
		return kernel.TransitionRejected("reopen_round requires satisfied plan endpoint")
	}
	if err := requireReopenableRoundEndpoint("execute", execute, transition.ExecuteBindingRef, false); err != nil {
		return err
	}
	// code_merge keeps Verify pending until the Merge Queue has durably
	// delivered a merged revision. If targeted verification proves that the
	// candidate cannot be merged safely, the only valid recovery is to roll a
	// fresh Execute+Verify round even though the withheld Verify never ran.
	if err := requireReopenableRoundEndpoint("verify", verify, transition.VerifyBindingRef, true); err != nil {
		return err
	}
	execute.State = EndpointPending
	execute.RunPolicy = RunEnabled
	execute.Generation++
	execute.BindingRef = transition.ExecuteBindingRef
	verify.State = EndpointPending
	verify.RunPolicy = RunEnabled
	verify.Generation++
	verify.BindingRef = transition.VerifyBindingRef
	state.endpoints[executeRef] = execute
	state.endpoints[verifyRef] = verify
	return nil
}

func requireReopenableRoundEndpoint(name string, endpoint PhaseEndpoint, bindingRef kernel.BindingRef, allowPending bool) error {
	switch endpoint.State {
	case EndpointSatisfied, EndpointRejected:
	case EndpointPending:
		if !allowPending {
			return kernel.TransitionRejected(fmt.Sprintf("reopen_round requires %s endpoint to be satisfied or rejected", name))
		}
	default:
		return kernel.TransitionRejected(fmt.Sprintf("reopen_round requires %s endpoint to be satisfied, rejected, or an explicitly withheld pending verify", name))
	}
	if endpoint.RunPolicy == RunHeld {
		return kernel.TransitionRejected(fmt.Sprintf("reopen_round requires %s endpoint not held", name))
	}
	if err := kernel.RequireID(name+"_binding_ref", bindingRef); err != nil {
		return err
	}
	if endpoint.BindingRef == bindingRef {
		return kernel.StaleBinding(fmt.Sprintf("reopen_round requires a new %s binding_ref", name))
	}
	return nil
}

func requireResultForTransition(state *graphState, endpoint PhaseEndpoint, transition GraphTransition, verdict Verdict) error {
	result := transition.Result
	if result.ID == "" {
		result.ID = fmt.Sprintf("%s:%s:%d:%s", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation, verdict)
	}
	if result.Endpoint != endpoint.Ref {
		return kernel.InvalidArgument("phase result endpoint must match transition endpoint")
	}
	if result.BindingRef != endpoint.BindingRef {
		return kernel.StaleBinding("phase result binding_ref must match current endpoint binding_ref")
	}
	if result.OutputRef == "" {
		return kernel.InvalidArgument("phase result output_ref is required")
	}
	result.Verdict = verdict
	if err := validateResult(result); err != nil {
		return err
	}
	if _, exists := state.results[result.ID]; exists {
		return kernel.TransitionRejected("phase result id already exists")
	}
	state.results[result.ID] = result
	return nil
}

func requireStopEvidence(endpoint PhaseEndpoint, transition GraphTransition) error {
	if err := kernel.RequireID("new_binding_ref", transition.NewBindingRef); err != nil {
		return err
	}
	if transition.NewBindingRef == endpoint.BindingRef {
		return kernel.StaleBinding("stopped transition requires a new binding_ref")
	}
	if len(transition.EvidenceRefs) == 0 {
		return kernel.IncompleteStopEvidence("stopped transition requires evidence_refs")
	}
	if transition.CheckpointRef == "" && !transition.NonResumable {
		return kernel.IncompleteStopEvidence("stopped transition requires checkpoint_ref or non_resumable")
	}
	return nil
}

func requireVerifySatisfied(state *graphState, taskID kernel.TaskID) error {
	verify, ok := state.endpoints[PhaseEndpointRef{TaskID: taskID, EndpointID: EndpointVerify}]
	if !ok {
		return kernel.InvalidArgument("task is missing verify endpoint")
	}
	if verify.State != EndpointSatisfied {
		return kernel.TransitionRejected("task done requires satisfied verify endpoint")
	}
	return nil
}
