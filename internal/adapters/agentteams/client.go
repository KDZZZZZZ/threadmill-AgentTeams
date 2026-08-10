package agentteams

import (
	"context"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type HostKind string

const (
	HostManager HostKind = "manager"
	HostWorker  HostKind = "worker"
)

type HostStatus struct {
	Ref              string
	Kind             HostKind
	Phase            string
	LastHeartbeat    time.Time
	Capacity         int
	ActiveExecutions int
	Capabilities     []string
}

type PreparedInvocation struct {
	InvocationID         kernel.InvocationID
	ProjectID            kernel.ProjectID
	Role                 auth.Role
	Operation            string
	RoomID               string
	Spec                 string
	RuntimeConfigRef     string
	EnvelopeRef          string
	RequiredCapabilities []string
}

type HostPreparation struct {
	HostRef          string
	InvocationID     kernel.InvocationID
	Role             auth.Role
	Operation        string
	RuntimeConfigRef string
	EnvelopeRef      string
}

type DelegateTaskRequest struct {
	ProjectID kernel.ProjectID
	TaskID    string
	HostRef   string
	RoomID    string
	Spec      string
}

type TaskSnapshot struct {
	TaskID     string
	ProjectID  kernel.ProjectID
	HostRef    string
	Status     string
	EventID    string
	ResultPath string
}

type TaskCheck struct {
	Task             TaskSnapshot
	ResultStatus     string
	Summary          string
	Deliverables     []string
	Effective        bool
	ValidationErrors []string
}

type RawObservation struct {
	Cursor          string
	ProviderEventID string
	TaskID          string
	HostRef         string
	Kind            string
	Payload         map[string]string
	ObservedAt      time.Time
}

// Client is the minimal AgentTeams/QwenPaw/taskflow provider seam. It does not
// expose projectflow, AgentTeams DAGs, Matrix messages, or any Threadmill graph.
type Client interface {
	ListHosts(context.Context) ([]HostStatus, error)
	PrepareHost(context.Context, HostPreparation) error
	DelegateTask(context.Context, DelegateTaskRequest) (TaskSnapshot, error)
	ReleaseHostSlot(context.Context, string, string) error
	RevokeInvocation(context.Context, string, kernel.InvocationID) error
	ForceStopHost(context.Context, string) error
	CancelTask(context.Context, string, string) error
	CheckTask(context.Context, string) (TaskCheck, error)
	ReadObservations(context.Context, string) ([]RawObservation, error)
}

type InvocationSource interface {
	LoadPreparedInvocation(context.Context, string) (PreparedInvocation, error)
}
