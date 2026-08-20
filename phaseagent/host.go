package phaseagent

import "context"

// Host is the inbound lifecycle boundary used by an Agent Runtime to invoke a
// Phase Agent. Phase Agents do not call these methods themselves.
//
// Start and Resume acknowledge host acceptance only; neither indicates that a
// phase has completed. Stop is a recoverable control-plane callback and returns
// only a reference to explicit, controlled resume state.
type Host interface {
	Start(ctx context.Context, input StartPhaseInput) error
	Stop(ctx context.Context, input StopPhaseInput) (StopPhaseAck, error)
	Resume(ctx context.Context, input ResumePhaseInput) error
}
