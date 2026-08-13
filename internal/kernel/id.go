package kernel

import "strings"

// StrongID is the shared validation contract for persisted Threadmill IDs.
// Concrete types keep Task, Phase, Invocation, principal, and lease identities
// from being accidentally interchanged at module seams.
type StrongID interface {
	~string
}

type (
	ProjectID        string
	TaskID           string
	EndpointID       string
	InvocationID     string
	ActorPrincipalID string
	SessionID        string
	CapabilityID     string
	BindingRef       string
	LeaseID          string
	IdempotencyKey   string
)

// IDScope identifies the authority boundary in which an idempotency key is
// unique. Callers should include at least project + operation + actor/invocation.
type IDScope string

func IsZeroID[T StrongID](id T) bool {
	return strings.TrimSpace(string(id)) == ""
}

func RequireID[T StrongID](name string, id T) error {
	if IsZeroID(id) {
		return InvalidArgument(name + " is required")
	}
	return nil
}
