package phaseagent

// StartPhaseInput is the Runtime-injected start contract for one controlled
// Phase Agent invocation. It contains references and an input projection, not
// ownership of the underlying Task, graphs, Workspace, or Context.
type StartPhaseInput struct {
	InvocationID string           `json:"invocation_id"`
	Endpoint     PhaseEndpointRef `json:"endpoint"`
	Generation   int              `json:"generation"`
	BindingRef   string           `json:"binding_ref"`
	Inputs       PhaseInputSet    `json:"inputs"`
}

// StopPhaseInput and ResumePhaseInput are Runtime-to-Agent lifecycle callback
// DTOs. They are deliberately not methods on the Agent-facing Runtime port.
type StopPhaseInput struct {
	InvocationID string `json:"invocation_id"`
	CommandID    string `json:"command_id"`
	Reason       string `json:"reason"`
}

// StopPhaseAck records only a controlled, opaque recovery-state reference.
// It is not a PhaseOutput and does not advance endpoint state.
type StopPhaseAck struct {
	ResumeStateRef string `json:"resume_state_ref"`
}

// ResumePhaseInput starts a new generation after a control-plane stop. Both
// references are opaque; no hidden reasoning, model session, or model state is
// carried by this domain model.
type ResumePhaseInput struct {
	Start         StartPhaseInput `json:"start"`
	CheckpointRef string          `json:"checkpoint_ref"`
}

// Validate checks only local shape. Binding resolution, input freshness,
// permissions, checkpoints, and revision compatibility belong to Runtime.
func (in StartPhaseInput) Validate() error {
	if in.InvocationID == "" {
		return invalidStartPhaseInputError("invocation_id is required")
	}
	if in.Generation <= 0 {
		return invalidStartPhaseInputError("generation must be greater than zero")
	}
	if in.BindingRef == "" {
		return invalidStartPhaseInputError("binding_ref is required")
	}
	return in.Endpoint.Validate()
}

// Validate checks only the resume callback's local shape. Runtime validates
// checkpoint provenance and compatibility before calling the Host.
func (in ResumePhaseInput) Validate() error {
	if err := in.Start.Validate(); err != nil {
		return err
	}
	if in.CheckpointRef == "" {
		return invalidResumePhaseInputError("checkpoint_ref is required")
	}
	return nil
}

type invalidStartPhaseInputError string

func (e invalidStartPhaseInputError) Error() string { return string(e) }

type invalidResumePhaseInputError string

func (e invalidResumePhaseInputError) Error() string { return string(e) }
