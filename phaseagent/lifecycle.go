package phaseagent

// InvocationState is the transient state of a locally hosted Phase Agent
// invocation. It is not a Coordination Graph endpoint state or a new durable
// business entity.
type InvocationState string

const (
	InvocationRunning  InvocationState = "running"
	InvocationWaiting  InvocationState = "waiting"
	InvocationStopping InvocationState = "stopping"
	InvocationStopped  InvocationState = "stopped"
	InvocationFinished InvocationState = "finished"
	InvocationFailed   InvocationState = "failed"
)

// InvocationContext is a Runner-local, non-persistent session snapshot. Inputs
// may evolve after awaitInputs; the immutable execution binding remains solely
// in Start.BindingRef and is intentionally not expanded here.
type InvocationContext struct {
	Start  StartPhaseInput `json:"start"`
	State  InvocationState `json:"state"`
	Inputs PhaseInputSet   `json:"inputs"`
}

// NewInvocationContext creates the initial local session for a fresh host
// start. Runtime owns persistence, leases, and the actual start event.
func NewInvocationContext(input StartPhaseInput) (InvocationContext, error) {
	if err := input.Validate(); err != nil {
		return InvocationContext{}, err
	}
	return InvocationContext{
		Start:  input,
		State:  InvocationRunning,
		Inputs: input.Inputs,
	}, nil
}

// ResumeState is the safe, explicit progress payload a Phase Agent may flush
// when stopped. It contains no hidden reasoning, provider/session identity, or
// worker memory. A Runtime registers its serialized form as an artifact and
// exposes only the resulting reference through StopPhaseAck.
type ResumeState struct {
	CompletedWork    []string `json:"completed_work"`
	PendingWork      []string `json:"pending_work"`
	ConsumedInputIDs []string `json:"consumed_input_ids"`
	NextSafeStep     string   `json:"next_safe_step"`
}

// MergeInputWaitResult applies Runtime's latest input-wait view to a local
// PhaseInputSet. Existing and new deliveries are deduplicated by InputID;
// conflicting deliveries for the same InputID are rejected rather than being
// silently overwritten. Pending is always replaced by Runtime's latest view.
// TerminalReason deliberately has no delivery representation and is ignored.
func MergeInputWaitResult(current PhaseInputSet, update InputWaitResult) (PhaseInputSet, error) {
	merged := PhaseInputSet{
		InputRevision: update.InputRevision,
		Required:      append([]InputRequirement(nil), current.Required...),
		Delivered:     append([]InputDelivery(nil), current.Delivered...),
		Pending:       append([]PendingInput(nil), update.Pending...),
	}

	byInputID := make(map[string]InputDelivery, len(merged.Delivered))
	for _, delivery := range merged.Delivered {
		if previous, found := byInputID[delivery.InputID]; found && !sameInputDelivery(previous, delivery) {
			return PhaseInputSet{}, inputDeliveryConflictError(delivery.InputID)
		}
		byInputID[delivery.InputID] = delivery
	}
	for _, delivery := range update.Delivered {
		if previous, found := byInputID[delivery.InputID]; found {
			if !sameInputDelivery(previous, delivery) {
				return PhaseInputSet{}, inputDeliveryConflictError(delivery.InputID)
			}
			continue
		}
		merged.Delivered = append(merged.Delivered, delivery)
		byInputID[delivery.InputID] = delivery
	}
	return merged, nil
}

func sameInputDelivery(left, right InputDelivery) bool {
	return left.InputID == right.InputID &&
		left.FromEndpoint == right.FromEndpoint &&
		left.PhaseOutputRef == right.PhaseOutputRef &&
		left.SourceRevision == right.SourceRevision &&
		sameStrings(left.ArtifactRefs, right.ArtifactRefs)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

type inputDeliveryConflictError string

func (e inputDeliveryConflictError) Error() string {
	return "conflicting input delivery for input ID \"" + string(e) + "\""
}
