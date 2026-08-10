package coordination

import (
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type runtimeView struct {
	revision      kernel.Revision
	tasks         map[kernel.TaskID]Task
	endpoints     []PhaseEndpoint
	endpointByRef map[PhaseEndpointRef]PhaseEndpoint
	edges         []Edge
	blockers      []Blocker
	results       []PhaseResult
	commands      map[string]commandRecord
	leases        map[kernel.LeaseID]phaseLease
	observations  []phaseObservation
	bindings      map[kernel.BindingRef]bindingRuntimeInfo
	suppressed    map[string]struct{}
}

func newRuntimeView(project *projectState) runtimeView {
	snapshot := project.current.snapshot(project.latest)
	view := runtimeView{
		revision:      snapshot.Revision,
		tasks:         make(map[kernel.TaskID]Task, len(snapshot.Tasks)),
		endpoints:     append([]PhaseEndpoint(nil), snapshot.Endpoints...),
		endpointByRef: make(map[PhaseEndpointRef]PhaseEndpoint, len(snapshot.Endpoints)),
		edges:         cloneEdges(snapshot.Edges),
		blockers:      append([]Blocker(nil), snapshot.Blockers...),
		results:       append([]PhaseResult(nil), snapshot.Results...),
		commands:      make(map[string]commandRecord, len(project.runtime.commands)),
		leases:        make(map[kernel.LeaseID]phaseLease, len(project.runtime.leases)),
		observations:  append([]phaseObservation(nil), project.runtime.observations...),
		bindings:      make(map[kernel.BindingRef]bindingRuntimeInfo, len(project.runtime.bindings)),
		suppressed:    make(map[string]struct{}),
	}
	for _, task := range snapshot.Tasks {
		view.tasks[task.ID] = task
	}
	for _, endpoint := range snapshot.Endpoints {
		view.endpointByRef[endpoint.Ref] = endpoint
	}
	for id, record := range project.runtime.commands {
		view.commands[id] = record
	}
	for id, lease := range project.runtime.leases {
		view.leases[id] = lease
	}
	for ref, info := range project.runtime.bindings {
		view.bindings[ref] = info
	}
	return view
}

func (v runtimeView) revisionRef() string {
	return fmt.Sprintf("revision://%d", v.revision)
}

func (v runtimeView) task(id kernel.TaskID) (Task, bool) {
	task, ok := v.tasks[id]
	return task, ok
}

func (v runtimeView) endpoint(ref PhaseEndpointRef) (PhaseEndpoint, bool) {
	endpoint, ok := v.endpointByRef[ref]
	return endpoint, ok
}

func (v runtimeView) binding(ref kernel.BindingRef) bindingRuntimeInfo {
	return v.bindings[ref]
}

func (v runtimeView) checkpointCompatible(ref kernel.BindingRef, info bindingRuntimeInfo) bool {
	return ref != "" && info.CheckpointRef != "" && !info.NonResumable
}

func (v runtimeView) activeLeases() []phaseLease {
	leases := make([]phaseLease, 0)
	for _, lease := range v.leases {
		if lease.State == "active" {
			leases = append(leases, lease)
		}
	}
	return leases
}

func (v runtimeView) leaseExpired(lease phaseLease) bool {
	return lease.Expired
}

func (v runtimeView) pendingCommands() []PhaseCommand {
	commands := make([]PhaseCommand, 0)
	for _, record := range v.commands {
		if record.CompletedEventRef != "" || record.Quarantined || record.NotExecutable {
			continue
		}
		if isRunCommand(record.Command) && record.ObservedEventRef != "" {
			continue
		}
		commands = append(commands, record.Command)
	}
	return commands
}

func (v runtimeView) unfoldedObservations() []phaseObservation {
	events := make([]phaseObservation, 0)
	for _, observation := range v.observations {
		if !observation.Folded {
			events = append(events, observation)
		}
	}
	return events
}

func (v runtimeView) suppressRun(ref PhaseEndpointRef, generation int) {
	v.suppressed[suppressionKey(ref, generation)] = struct{}{}
}

func (v runtimeView) runSuppressed(ref PhaseEndpointRef, generation int) bool {
	_, ok := v.suppressed[suppressionKey(ref, generation)]
	return ok
}

func suppressionKey(ref PhaseEndpointRef, generation int) string {
	return fmt.Sprintf("%s/%s/%d", ref.TaskID, ref.EndpointID, generation)
}

func (v runtimeView) hasCommandForLease(leaseRef kernel.LeaseID) bool {
	for _, record := range v.commands {
		if record.Command.LeaseRef == leaseRef && !record.Quarantined && !record.NotExecutable {
			return true
		}
	}
	return false
}

func (v runtimeView) hasMatchingActiveLease(command PhaseCommand) bool {
	lease, ok := v.leases[command.LeaseRef]
	if !ok || lease.State != "active" {
		return false
	}
	return lease.Endpoint == command.Endpoint &&
		lease.Generation == command.Generation &&
		lease.BindingRef == command.BindingRef
}

func (v runtimeView) hasRemoteObservation(lease phaseLease) bool {
	for _, observation := range v.observations {
		if observation.LeaseRef == lease.LeaseRef &&
			observation.Endpoint == lease.Endpoint &&
			observation.Generation == lease.Generation &&
			observation.BindingRef == lease.BindingRef {
			return true
		}
	}
	return false
}

func (v runtimeView) isRunnable(endpoint PhaseEndpoint) bool {
	task, ok := v.task(endpoint.Ref.TaskID)
	if !ok || task.Outcome != TaskActive {
		return false
	}
	if endpoint.State != EndpointPending || endpoint.RunPolicy != RunEnabled {
		return false
	}
	if endpoint.BindingRef == "" || endpoint.SpecRef == "" {
		return false
	}
	if !v.builtinPredecessorSatisfied(endpoint.Ref) {
		return false
	}
	if !v.startEdgesAndBlockersSatisfied(endpoint.Ref) {
		return false
	}
	if v.hasActiveLease(endpoint.Ref, endpoint.Generation) || v.hasPendingRunCommand(endpoint.Ref, endpoint.Generation) {
		return false
	}
	return !v.hasTerminalObservation(endpoint.Ref, endpoint.Generation)
}

func (v runtimeView) builtinPredecessorSatisfied(ref PhaseEndpointRef) bool {
	switch ref.EndpointID {
	case EndpointPlan:
		return true
	case EndpointExecute:
		return v.endpointSatisfied(PhaseEndpointRef{TaskID: ref.TaskID, EndpointID: EndpointPlan})
	case EndpointVerify:
		return v.endpointSatisfied(PhaseEndpointRef{TaskID: ref.TaskID, EndpointID: EndpointExecute})
	default:
		return false
	}
}

func (v runtimeView) endpointSatisfied(ref PhaseEndpointRef) bool {
	endpoint, ok := v.endpoint(ref)
	return ok && endpoint.State == EndpointSatisfied
}

func (v runtimeView) startEdgesAndBlockersSatisfied(ref PhaseEndpointRef) bool {
	for _, edge := range v.edges {
		if edge.To != ref || edge.RequiredBy != RequiredByStart {
			continue
		}
		if !v.edgeSatisfied(edge) {
			return false
		}
	}
	for _, blocker := range v.blockers {
		if blocker.Target != ref || blocker.RequiredBy != RequiredByStart {
			continue
		}
		switch blocker.State {
		case BlockerActive, BlockerDenied:
			return false
		case BlockerResolved, BlockerObsolete:
		}
	}
	return true
}

func (v runtimeView) edgeSatisfied(edge Edge) bool {
	switch edge.Signal {
	case SignalPhaseSatisfied:
		return v.endpointSatisfied(edge.From)
	case SignalTaskDone:
		task, ok := v.task(edge.From.TaskID)
		return ok && task.Outcome == TaskDone
	default:
		return false
	}
}

func (v runtimeView) hasActiveLease(ref PhaseEndpointRef, generation int) bool {
	for _, lease := range v.leases {
		if lease.State == "active" && lease.Endpoint == ref && lease.Generation == generation {
			return true
		}
	}
	return false
}

func (v runtimeView) hasPendingRunCommand(ref PhaseEndpointRef, generation int) bool {
	for _, record := range v.commands {
		if record.CompletedEventRef != "" || record.Quarantined || record.NotExecutable {
			continue
		}
		command := record.Command
		if command.Endpoint == ref && command.Generation == generation && isRunCommand(command) && record.ObservedEventRef == "" {
			return true
		}
	}
	return false
}

func isRunCommand(command PhaseCommand) bool {
	return command.Action == CommandStart || command.Action == CommandResume
}

func (v runtimeView) hasTerminalObservation(ref PhaseEndpointRef, generation int) bool {
	for _, observation := range v.observations {
		if !observation.Folded || observation.Endpoint != ref || observation.Generation != generation || observation.CommandID == "" || observation.LeaseRef == "" || observation.BindingRef == "" {
			continue
		}
		record, commandExists := v.commands[observation.CommandID]
		lease, leaseExists := v.leases[observation.LeaseRef]
		if !commandExists || !leaseExists || record.CompletedEventRef != observation.ID ||
			record.Command.Endpoint != observation.Endpoint || record.Command.Generation != observation.Generation ||
			record.Command.BindingRef != observation.BindingRef || record.Command.LeaseRef != observation.LeaseRef ||
			lease.Endpoint != observation.Endpoint || lease.Generation != observation.Generation || lease.BindingRef != observation.BindingRef {
			continue
		}
		switch observation.Kind {
		case "PhaseOutputSubmitted", "PhaseInvocationFailed", "PhaseInvocationStopped":
			return true
		}
	}
	return false
}

func (p *projectState) endpointRunnableLocked(ref PhaseEndpointRef, generation int) bool {
	endpoint, ok := p.current.endpoints[ref]
	if !ok || endpoint.Generation != generation {
		return false
	}
	return newRuntimeView(p).isRunnable(endpoint)
}
