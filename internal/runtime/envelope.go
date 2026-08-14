package runtime

import (
	"encoding/json"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

// Envelope is generated from persisted Invocation state. Agent-supplied
// payloads never populate or override these authority fields.
type Envelope struct {
	InvocationID     kernel.InvocationID     `json:"invocation_id"`
	ActorPrincipalID kernel.ActorPrincipalID `json:"actor_principal_id"`
	ProjectID        kernel.ProjectID        `json:"project_id"`
	TaskID           kernel.TaskID           `json:"task_id,omitempty"`
	EndpointID       kernel.EndpointID       `json:"endpoint_id,omitempty"`
	Generation       uint64                  `json:"generation,omitempty"`
	Role             auth.Role               `json:"role"`
	Operation        string                  `json:"operation,omitempty"`
	BindingRef       kernel.BindingRef       `json:"binding_ref,omitempty"`
	LeaseID          kernel.LeaseID          `json:"lease_id,omitempty"`
	EffectiveTools   []auth.Tool             `json:"effective_tools"`
}

func EnvelopeFromInvocation(invocation Invocation) Envelope {
	return Envelope{
		InvocationID:     invocation.ID,
		ActorPrincipalID: invocation.ActorPrincipalID,
		ProjectID:        invocation.ProjectID,
		TaskID:           invocation.TaskID,
		EndpointID:       invocation.EndpointID,
		Generation:       invocation.Generation,
		Role:             invocation.Role,
		Operation:        invocation.Operation,
		BindingRef:       invocation.BindingRef,
		LeaseID:          invocation.LeaseID,
		EffectiveTools:   append([]auth.Tool(nil), invocation.EffectiveTools...),
	}
}

func (e Envelope) JSON() (string, error) {
	encoded, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
