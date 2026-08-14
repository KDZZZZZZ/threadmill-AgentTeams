package coordination

import (
	"context"
	"fmt"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const (
	phaseObservationStarted = "PhaseInvocationStarted"
	phaseObservationOutput  = "PhaseOutputSubmitted"
	phaseObservationFailed  = "PhaseInvocationFailed"
	phaseObservationStopped = "PhaseInvocationStopped"
)

// PhaseObservationWriter is a process-internal port for phase execution
// evidence. It intentionally exposes only phase runtime observations, not graph
// CRUD or Task Manager transition APIs.
type PhaseObservationWriter interface {
	RecordPhaseInvocationStarted(context.Context, kernel.ProjectID, PhaseCommand) error
	RecordPhaseOutputSubmitted(context.Context, kernel.ProjectID, PhaseCommand) error
	RecordPhaseInvocationFailed(context.Context, kernel.ProjectID, PhaseCommand) error
	RecordPhaseInvocationStopped(context.Context, kernel.ProjectID, PhaseCommand, string, bool) error
}

func (s *MemoryStore) RecordPhaseInvocationStarted(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationStarted, "", false)
}

func (s *MemoryStore) RecordPhaseOutputSubmitted(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationOutput, "", false)
}

func (s *MemoryStore) RecordPhaseInvocationFailed(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationFailed, "", false)
}

func (s *MemoryStore) RecordPhaseInvocationStopped(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, checkpointRef string, nonResumable bool) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationStopped, checkpointRef, nonResumable)
}

func (s *PostgresStore) RecordPhaseInvocationStarted(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationStarted, "", false)
}

func (s *PostgresStore) RecordPhaseOutputSubmitted(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationOutput, "", false)
}

func (s *PostgresStore) RecordPhaseInvocationFailed(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationFailed, "", false)
}

func (s *PostgresStore) RecordPhaseInvocationStopped(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, checkpointRef string, nonResumable bool) error {
	return s.appendPhaseObservation(ctx, projectID, command, phaseObservationStopped, checkpointRef, nonResumable)
}

func (s *MemoryStore) appendPhaseObservation(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, kind, checkpointRef string, nonResumable bool) error {
	observation, err := newPhaseObservation(kind, command, checkpointRef, nonResumable)
	if err != nil {
		return err
	}
	return s.appendObservation(ctx, projectID, observation)
}

func (s *PostgresStore) appendPhaseObservation(ctx context.Context, projectID kernel.ProjectID, command PhaseCommand, kind, checkpointRef string, nonResumable bool) error {
	observation, err := newPhaseObservation(kind, command, checkpointRef, nonResumable)
	if err != nil {
		return err
	}
	return s.appendObservation(ctx, projectID, observation)
}

func newPhaseObservation(kind string, command PhaseCommand, checkpointRef string, nonResumable bool) (phaseObservation, error) {
	if err := validatePhaseObservationCommand(kind, command, checkpointRef, nonResumable); err != nil {
		return phaseObservation{}, err
	}
	return phaseObservation{
		ID:            deterministicPhaseObservationID(kind, command.ID),
		Kind:          kind,
		CommandID:     command.ID,
		Endpoint:      command.Endpoint,
		Generation:    command.Generation,
		BindingRef:    command.BindingRef,
		LeaseRef:      command.LeaseRef,
		CheckpointRef: checkpointRef,
		NonResumable:  nonResumable,
	}, nil
}

func validatePhaseObservationCommand(kind string, command PhaseCommand, checkpointRef string, nonResumable bool) error {
	if command.ID == "" {
		return kernel.InvalidArgument("phase observation command_id is required")
	}
	if command.Endpoint.TaskID == "" || command.Endpoint.EndpointID == "" || command.Generation <= 0 || command.BindingRef == "" || command.LeaseRef == "" {
		return kernel.InvalidArgument("phase observation command identity is incomplete")
	}
	switch kind {
	case phaseObservationStarted:
		if command.Action != CommandStart && command.Action != CommandResume {
			return commandError(kernel.CodeStaleCommand, "started observation requires start or resume command")
		}
		if checkpointRef != "" || nonResumable {
			return kernel.InvalidArgument("started observation cannot carry stop evidence")
		}
	case phaseObservationOutput, phaseObservationFailed:
		if command.Action != CommandStart && command.Action != CommandResume {
			return commandError(kernel.CodeStaleCommand, "terminal execution observation requires start or resume command")
		}
		if checkpointRef != "" || nonResumable {
			return kernel.InvalidArgument("terminal execution observation cannot carry stop evidence")
		}
	case phaseObservationStopped:
		if command.Action != CommandStop {
			return commandError(kernel.CodeStaleCommand, "stopped observation requires stop command")
		}
		if checkpointRef == "" && !nonResumable {
			return kernel.IncompleteStopEvidence("stopped observation requires checkpoint_ref or non_resumable")
		}
	default:
		return kernel.InvalidArgument("unsupported phase observation kind")
	}
	return nil
}

func deterministicPhaseObservationID(kind, commandID string) string {
	return fmt.Sprintf("phase-observation:%s:%s", kind, commandID)
}

var _ PhaseObservationWriter = (*MemoryStore)(nil)
var _ PhaseObservationWriter = (*PostgresStore)(nil)
