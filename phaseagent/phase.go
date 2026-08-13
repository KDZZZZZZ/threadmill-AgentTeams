// Package phaseagent defines the domain types exchanged between a Phase Agent
// and its host Runtime. It deliberately contains no Runtime, transport, graph,
// artifact-store, or AgentTeams implementation.
package phaseagent

// Phase identifies one of the three fixed work phases in a Task round.
type Phase string

const (
	PhasePlan    Phase = "plan"
	PhaseExecute Phase = "execute"
	PhaseVerify  Phase = "verify"
)

// Valid reports whether p is a Phase Agent work phase.
func (p Phase) Valid() bool {
	switch p {
	case PhasePlan, PhaseExecute, PhaseVerify:
		return true
	default:
		return false
	}
}

// Validate rejects values outside the fixed Phase vocabulary.
func (p Phase) Validate() error {
	if !p.Valid() {
		return invalidPhaseError(p)
	}
	return nil
}

// PhaseEndpointRef identifies one fixed Phase Endpoint in the Coordination
// Graph. Its field semantics are authoritative in coordination-graph.md §2.1.
// This minimal definition lives here until a shared Coordination Graph domain
// package exists.
type PhaseEndpointRef struct {
	TaskID     string `json:"task_id"`
	EndpointID string `json:"endpoint_id"`
}

// Validate checks the fixed endpoint vocabulary. Existence and Task ownership
// are Coordination Graph / Runtime responsibilities.
func (ref PhaseEndpointRef) Validate() error {
	if !Phase(ref.EndpointID).Valid() {
		return invalidEndpointIDError(ref.EndpointID)
	}
	return nil
}

type invalidPhaseError Phase

func (e invalidPhaseError) Error() string {
	return "invalid phase \"" + string(e) + "\""
}

type invalidEndpointIDError string

func (e invalidEndpointIDError) Error() string {
	return "invalid endpoint ID \"" + string(e) + "\""
}
