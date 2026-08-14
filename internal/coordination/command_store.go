package coordination

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

// defaultPhaseLeaseTTL matches the Phase Controller invocation TTL. The graph
// lease is the scheduler-side lifetime boundary for that same execution.
const defaultPhaseLeaseTTL = time.Hour

type commandRecord struct {
	Command           PhaseCommand
	Accepted          bool
	ObservedEventRef  string
	CompletedEventRef string
	RetryScheduled    bool
	Quarantined       bool
	NotExecutable     bool
}

type phaseLease struct {
	LeaseRef   kernel.LeaseID
	Endpoint   PhaseEndpointRef
	Generation int
	BindingRef kernel.BindingRef
	State      string
	ExpiresAt  time.Time
	Expired    bool
}

type phaseObservation struct {
	ID             string
	Kind           string
	CommandID      string
	Endpoint       PhaseEndpointRef
	Generation     int
	BindingRef     kernel.BindingRef
	LeaseRef       kernel.LeaseID
	CheckpointRef  string
	NonResumable   bool
	DispatchError  string
	DispatchTarget PhaseEndpointRef
	Folded         bool
}

type bindingRuntimeInfo struct {
	CheckpointRef string
	NonResumable  bool
}

type memoryRuntimeState struct {
	commands     map[string]commandRecord
	leases       map[kernel.LeaseID]phaseLease
	observations []phaseObservation
	bindings     map[kernel.BindingRef]bindingRuntimeInfo
}

func newMemoryRuntimeState() memoryRuntimeState {
	return memoryRuntimeState{
		commands: make(map[string]commandRecord),
		leases:   make(map[kernel.LeaseID]phaseLease),
		bindings: make(map[kernel.BindingRef]bindingRuntimeInfo),
	}
}

func (s *MemoryStore) loadRuntimeView(ctx context.Context, projectID kernel.ProjectID) (runtimeView, error) {
	if err := ctx.Err(); err != nil {
		return runtimeView{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	view := newRuntimeView(project)
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	for ref, lease := range view.leases {
		// A missing deadline is legacy unbounded state and therefore cannot be
		// trusted as a live execution after recovery.
		lease.Expired = lease.Expired || lease.ExpiresAt.IsZero() || !lease.ExpiresAt.After(now)
		view.leases[ref] = lease
	}
	return view, nil
}

func (s *MemoryStore) markCommandObserved(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[observation.CommandID]
	if !ok {
		return commandError(kernel.CodeStaleCommand, "observation command does not exist")
	}
	lease, err := matchingObservationLease(project, record.Command, observation)
	if err != nil {
		return err
	}
	if record.Command.Action != CommandStart && record.Command.Action != CommandResume {
		return commandError(kernel.CodeStaleCommand, "started observation must match a start or resume command")
	}
	if lease.State != "active" {
		return kernel.LeaseConflict("started observation lease is not active")
	}
	record.ObservedEventRef = observation.ID
	project.runtime.commands[observation.CommandID] = record
	s.markObservationFoldedLocked(project, observation.ID)
	return nil
}

func (s *MemoryStore) completeCommandAndReleaseLease(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[observation.CommandID]
	if !ok {
		return commandError(kernel.CodeStaleCommand, "terminal observation command does not exist")
	}
	lease, err := matchingObservationLease(project, record.Command, observation)
	if err != nil {
		return err
	}
	if observation.Kind == "PhaseInvocationStopped" {
		if record.Command.Action != CommandStop {
			return commandError(kernel.CodeStaleCommand, "stopped observation must match a stop command")
		}
	} else if record.Command.Action != CommandStart && record.Command.Action != CommandResume {
		return commandError(kernel.CodeStaleCommand, "terminal execution observation must match a start or resume command")
	}
	record.CompletedEventRef = observation.ID
	project.runtime.commands[observation.CommandID] = record
	lease.State = "released"
	project.runtime.leases[observation.LeaseRef] = lease
	s.markObservationFoldedLocked(project, observation.ID)
	return nil
}

func matchingObservationLease(project *projectState, command PhaseCommand, observation phaseObservation) (phaseLease, error) {
	if observation.CommandID == "" || observation.LeaseRef == "" || observation.Endpoint.TaskID == "" || observation.Endpoint.EndpointID == "" || observation.Generation <= 0 || observation.BindingRef == "" {
		return phaseLease{}, kernel.InvalidArgument("runtime observation requires command_id, lease_ref, endpoint, generation, and binding_ref")
	}
	if command.Endpoint != observation.Endpoint || command.Generation != observation.Generation || command.BindingRef != observation.BindingRef || command.LeaseRef != observation.LeaseRef {
		return phaseLease{}, commandError(kernel.CodeStaleCommand, "observation does not match command identity")
	}
	lease, ok := project.runtime.leases[observation.LeaseRef]
	if !ok {
		return phaseLease{}, kernel.LeaseConflict("observation lease does not exist")
	}
	if lease.Endpoint != observation.Endpoint || lease.Generation != observation.Generation || lease.BindingRef != observation.BindingRef {
		return phaseLease{}, kernel.LeaseConflict("observation does not match lease identity")
	}
	return lease, nil
}

func (s *MemoryStore) releaseOrphanLease(ctx context.Context, projectID kernel.ProjectID, leaseRef kernel.LeaseID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	project := s.ensureProject(projectID)
	lease, ok := project.runtime.leases[leaseRef]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "lease not found", Recoverable: true}
	}
	lease.State = "released"
	project.runtime.leases[leaseRef] = lease
	return nil
}

func (s *MemoryStore) markObservationFoldedLocked(project *projectState, eventID string) {
	for i := range project.runtime.observations {
		if project.runtime.observations[i].ID == eventID {
			project.runtime.observations[i].Folded = true
			return
		}
	}
}

func (s *MemoryStore) getOrCreateStopCommand(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision, lease phaseLease) (PhaseCommand, bool, error) {
	if err := ctx.Err(); err != nil {
		return PhaseCommand{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	commandID := fmt.Sprintf("cmd:stop:%s", lease.LeaseRef)
	if record, ok := project.runtime.commands[commandID]; ok {
		deliverable := record.CompletedEventRef == "" && !record.Quarantined && !record.NotExecutable
		return record.Command, deliverable, nil
	}
	command := PhaseCommand{
		ID:         commandID,
		Endpoint:   lease.Endpoint,
		Generation: lease.Generation,
		BindingRef: lease.BindingRef,
		LeaseRef:   lease.LeaseRef,
		Action:     CommandStop,
		CauseRef:   fmt.Sprintf("revision://%d", revision),
	}
	project.runtime.commands[command.ID] = commandRecord{Command: command}
	return command, true, nil
}

func (s *MemoryStore) claimLeaseAndAppendCommand(ctx context.Context, projectID kernel.ProjectID, revision kernel.Revision, endpoint PhaseEndpoint, action CommandAction, causeRef string) (PhaseCommand, bool, error) {
	if err := ctx.Err(); err != nil {
		return PhaseCommand{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	if err := kernel.CheckExpectedRevision(revision, project.latest); err != nil {
		return PhaseCommand{}, false, nil
	}
	if !project.endpointRunnableLocked(endpoint.Ref, endpoint.Generation) {
		return PhaseCommand{}, false, nil
	}
	leaseRef := kernel.LeaseID(fmt.Sprintf("lease:%s:%s:%d", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation))
	if lease, ok := project.runtime.leases[leaseRef]; ok && lease.State == "active" {
		return existingRunCommand(project, endpoint.Ref, endpoint.Generation), false, nil
	}
	commandID := fmt.Sprintf("cmd:run:%s:%s:%d", endpoint.Ref.TaskID, endpoint.Ref.EndpointID, endpoint.Generation)
	if record, ok := project.runtime.commands[commandID]; ok {
		if record.Command.Action != CommandStart && record.Command.Action != CommandResume {
			return PhaseCommand{}, false, kernel.Error{Code: kernel.CodeCommandConflict, Message: "run command id already has different content", Recoverable: false}
		}
		return record.Command, false, nil
	}
	command := PhaseCommand{
		ID:         commandID,
		Endpoint:   endpoint.Ref,
		Generation: endpoint.Generation,
		BindingRef: endpoint.BindingRef,
		LeaseRef:   leaseRef,
		Action:     action,
		CauseRef:   causeRef,
	}
	project.runtime.leases[leaseRef] = phaseLease{
		LeaseRef:   leaseRef,
		Endpoint:   endpoint.Ref,
		Generation: endpoint.Generation,
		BindingRef: endpoint.BindingRef,
		State:      "active",
		ExpiresAt:  s.leaseDeadline(),
	}
	project.runtime.commands[command.ID] = commandRecord{Command: command}
	return command, true, nil
}

func (s *MemoryStore) leaseDeadline() time.Time {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	return now.Add(defaultPhaseLeaseTTL)
}

func existingRunCommand(project *projectState, ref PhaseEndpointRef, generation int) PhaseCommand {
	for _, record := range project.runtime.commands {
		if record.Command.Endpoint == ref && record.Command.Generation == generation &&
			(record.Command.Action == CommandStart || record.Command.Action == CommandResume) {
			return record.Command
		}
	}
	return PhaseCommand{}
}

func (s *MemoryStore) markCommandAccepted(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[commandID]
	if !ok {
		return
	}
	record.Accepted = true
	record.RetryScheduled = false
	project.runtime.commands[commandID] = record
}

func (s *MemoryStore) scheduleRetry(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[commandID]
	if !ok {
		return
	}
	record.RetryScheduled = true
	project.runtime.commands[commandID] = record
}

func (s *MemoryStore) quarantineCommand(ctx context.Context, projectID kernel.ProjectID, commandID string) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[commandID]
	if !ok {
		return
	}
	record.Quarantined = true
	project.runtime.commands[commandID] = record
}

func (s *MemoryStore) rejectCommand(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, err error) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	record, ok := project.runtime.commands[command.ID]
	if !ok || record.Command != command {
		return
	}
	eventID := fmt.Sprintf("dispatch-rejection:%s:%s", command.ID, kernel.ErrorCodeOf(err))
	for _, observation := range project.runtime.observations {
		if observation.ID == eventID {
			return
		}
	}
	record.NotExecutable = true
	record.RetryScheduled = false
	record.CompletedEventRef = eventID
	project.runtime.commands[command.ID] = record
	if (command.Action == CommandStart || command.Action == CommandResume) && record.ObservedEventRef == "" {
		if lease, exists := project.runtime.leases[command.LeaseRef]; exists && lease.State == "active" && lease.Endpoint == command.Endpoint && lease.Generation == command.Generation && lease.BindingRef == command.BindingRef {
			lease.State = "released"
			project.runtime.leases[command.LeaseRef] = lease
		}
	}
	project.runtime.observations = append(project.runtime.observations, phaseObservation{
		ID:             eventID,
		Kind:           "DispatchRejected",
		CommandID:      command.ID,
		Endpoint:       command.Endpoint,
		Generation:     command.Generation,
		BindingRef:     command.BindingRef,
		LeaseRef:       command.LeaseRef,
		DispatchTarget: command.Endpoint,
		DispatchError:  string(kernel.ErrorCodeOf(err)),
		Folded:         true,
	})
}

func (s *MemoryStore) recordEndpointDispatchRejection(ctx context.Context, projectID kernel.ProjectID, endpoint PhaseEndpointRef, generation int, bindingRef kernel.BindingRef, err error) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	eventID := fmt.Sprintf("dispatch-rejection:%s:%s:%d:%s", endpoint.TaskID, endpoint.EndpointID, generation, kernel.ErrorCodeOf(err))
	for _, observation := range project.runtime.observations {
		if observation.ID == eventID {
			return
		}
	}
	project.runtime.observations = append(project.runtime.observations, phaseObservation{
		ID:             eventID,
		Kind:           "DispatchRejected",
		Endpoint:       endpoint,
		Generation:     generation,
		BindingRef:     bindingRef,
		DispatchTarget: endpoint,
		DispatchError:  string(kernel.ErrorCodeOf(err)),
		Folded:         true,
	})
}

func (s *MemoryStore) appendObservation(ctx context.Context, projectID kernel.ProjectID, observation phaseObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if observation.ID == "" {
		return kernel.InvalidArgument("observation.id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	if observation.Kind != "DispatchRejected" {
		record, ok := project.runtime.commands[observation.CommandID]
		if !ok {
			return commandError(kernel.CodeStaleCommand, "observation command does not exist")
		}
		if _, err := matchingObservationLease(project, record.Command, observation); err != nil {
			return err
		}
	}
	for _, existing := range project.runtime.observations {
		if existing.ID != observation.ID {
			continue
		}
		if samePhaseObservationPayload(existing, observation) {
			return nil
		}
		return kernel.IdempotencyConflict()
	}
	project.runtime.observations = append(project.runtime.observations, observation)
	return nil
}

func (s *MemoryStore) expireLease(ctx context.Context, projectID kernel.ProjectID, leaseRef kernel.LeaseID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	lease, ok := project.runtime.leases[leaseRef]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "lease not found", Recoverable: true}
	}
	lease.Expired = true
	project.runtime.leases[leaseRef] = lease
	return nil
}

func (s *MemoryStore) runtimeCommands(ctx context.Context, projectID kernel.ProjectID) []PhaseCommand {
	if ctx.Err() != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	commands := make([]PhaseCommand, 0, len(project.runtime.commands))
	for _, record := range project.runtime.commands {
		commands = append(commands, record.Command)
	}
	sort.Slice(commands, func(i, j int) bool { return commands[i].ID < commands[j].ID })
	return commands
}

func (s *MemoryStore) runtimeLeases(ctx context.Context, projectID kernel.ProjectID) []phaseLease {
	if ctx.Err() != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	leases := make([]phaseLease, 0, len(project.runtime.leases))
	for _, lease := range project.runtime.leases {
		leases = append(leases, lease)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].LeaseRef < leases[j].LeaseRef })
	return leases
}
