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
		if endpoint.RunPolicy == RunHeld {
			return kernel.TransitionRejected("submitted transition requires endpoint not held")
		}
		if endpoint.State != EndpointPending {
			return kernel.TransitionRejected("submitted transition requires pending endpoint")
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
		if endpoint.State != EndpointRejected && !(endpoint.State == EndpointPending && endpoint.RunPolicy == RunHeld) {
			return kernel.TransitionRejected("reopened transition requires rejected endpoint or held pending endpoint")
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
	default:
		return kernel.InvalidArgument("task transition action is not allowed")
	}
	state.tasks[transition.TaskID] = task
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
