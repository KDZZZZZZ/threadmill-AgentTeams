package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

type Authenticator interface {
	AuthenticateOperatorSession(context.Context, string, kernel.ProjectID) (auth.Principal, auth.SessionRecord, error)
}

type StateChangeGuard interface {
	Check(*http.Request, auth.SessionRecord) error
}

type RequirementCommandPort interface {
	SubmitRequirement(context.Context, auth.Principal, RequirementCreateRequest) (RequirementCreateResponse, error)
}

type CapacityPort interface {
	GetCapacity(context.Context, auth.Principal, kernel.ProjectID) (CapacityState, error)
	AdjustCapacity(context.Context, auth.Principal, CapacityAdjustmentRequest) (CapacityAdjustmentResponse, error)
}

type HumanDecisionPort interface {
	SubmitHumanDecision(context.Context, auth.Principal, HumanDecisionRequest) (HumanDecisionResponse, error)
}

type ManagerPort interface {
	SubmitManagerMessage(context.Context, auth.Principal, ManagerMessageRequest) (ManagerMessageResponse, error)
	Conversation(context.Context, auth.Principal, string, string) (ManagerConversation, error)
}

type QueryPort interface {
	Task(context.Context, auth.Principal, kernel.TaskID) (TaskProjection, error)
	ProjectSnapshot(context.Context, auth.Principal, kernel.ProjectID, kernel.Revision) (CoordinationSnapshot, error)
	InspectEndpoint(context.Context, auth.Principal, coordination.PhaseEndpointRef, int) (EndpointInspector, error)
}

// ReadinessPort is the AP03 dependency-health seam behind the OpenAPI
// /readyz projection. It reports dependency facts and has no control methods.
type ReadinessPort interface {
	Readiness(context.Context) ReadinessStatus
}

type RequirementCreateRequest struct {
	RequestID      string           `json:"request_id"`
	ProjectID      kernel.ProjectID `json:"project_id"`
	ConversationID string           `json:"conversation_id,omitempty"`
	Body           string           `json:"body"`
	Motivation     string           `json:"motivation,omitempty"`
	Constraints    []string         `json:"constraints,omitempty"`
	Acceptance     []string         `json:"acceptance,omitempty"`
	Source         *InputSource     `json:"source,omitempty"`
}

type InputSource struct {
	Kind        string `json:"kind,omitempty"`
	ExternalRef string `json:"external_ref,omitempty"`
}

type RequirementCreateResponse struct {
	RequirementID   string              `json:"requirement_id"`
	ManagerInputRef string              `json:"manager_input_ref"`
	InvocationRef   kernel.InvocationID `json:"invocation_ref"`
	ConversationID  string              `json:"conversation_id,omitempty"`
	Status          string              `json:"status"`
}

type CapacityState = uiprojection.CapacityState

type CapacityAdjustmentRequest struct {
	RequestID          string           `json:"request_id"`
	ProjectID          kernel.ProjectID `json:"project_id"`
	ExpectedRevision   int              `json:"expected_revision"`
	DesiredConcurrency int              `json:"desired_concurrency"`
}

type CapacityAdjustmentResponse struct {
	CommandRef string        `json:"command_ref"`
	Capacity   CapacityState `json:"capacity"`
}

type HumanDecisionRequest struct {
	RequestID             string           `json:"request_id"`
	ProjectID             kernel.ProjectID `json:"project_id"`
	Target                DecisionTarget   `json:"target"`
	Decision              string           `json:"decision"`
	Reason                string           `json:"reason"`
	ExpectedGraphRevision *kernel.Revision `json:"expected_graph_revision,omitempty"`
	EvidenceRefs          []ArtifactRef    `json:"evidence_refs,omitempty"`
}

type HumanDecisionResponse struct {
	HumanDecisionRef string              `json:"human_decision_ref"`
	ManagerInputRef  string              `json:"manager_input_ref"`
	InvocationRef    kernel.InvocationID `json:"invocation_ref"`
	Status           string              `json:"status"`
}

type DecisionTarget struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref"`
}

type ManagerMessageRequest struct {
	RequestID             string                         `json:"request_id"`
	ProjectID             kernel.ProjectID               `json:"project_id"`
	ConversationID        string                         `json:"conversation_id"`
	Body                  string                         `json:"body"`
	SelectedEndpoint      *coordination.PhaseEndpointRef `json:"selected_endpoint,omitempty"`
	ObservedGraphRevision *kernel.Revision               `json:"observed_graph_revision,omitempty"`
}

type ManagerMessageResponse struct {
	ManagerInputRef string              `json:"manager_input_ref"`
	InvocationRef   kernel.InvocationID `json:"invocation_ref"`
	ConversationID  string              `json:"conversation_id,omitempty"`
	Status          string              `json:"status"`
}

type ArtifactRef struct {
	ArtifactID         string              `json:"artifact_id"`
	ObjectKey          string              `json:"object_key,omitempty"`
	SHA256             string              `json:"sha256"`
	MediaType          string              `json:"media_type"`
	SizeBytes          int64               `json:"size_bytes"`
	ACL                []string            `json:"acl,omitempty"`
	SourceEventID      string              `json:"source_event_id,omitempty"`
	SourceInvocationID kernel.InvocationID `json:"source_invocation_id,omitempty"`
}

type TaskProjection struct {
	TaskID         kernel.TaskID           `json:"task_id"`
	ProjectID      kernel.ProjectID        `json:"project_id"`
	Title          string                  `json:"title,omitempty"`
	Status         string                  `json:"status"`
	GraphRevision  kernel.Revision         `json:"graph_revision"`
	ContractRef    string                  `json:"contract_ref,omitempty"`
	DeliveryPolicy string                  `json:"delivery_policy"`
	Endpoints      []EndpointProjection    `json:"endpoints"`
	Blockers       []BlockerProjection     `json:"blockers,omitempty"`
	Decisions      []DecisionProjection    `json:"decisions,omitempty"`
	Delivery       *TaskDeliveryProjection `json:"delivery,omitempty"`
}

type EndpointProjection struct {
	TaskID              kernel.TaskID           `json:"task_id"`
	EndpointID          coordination.EndpointID `json:"endpoint_id"`
	Generation          int                     `json:"generation"`
	State               string                  `json:"state"`
	RunPolicy           string                  `json:"run_policy"`
	BindingRef          string                  `json:"binding_ref,omitempty"`
	LatestInvocationRef kernel.InvocationID     `json:"latest_invocation_ref,omitempty"`
	DeliverySpec        *DeliverySpec           `json:"delivery_spec,omitempty"`
	ReportSpec          *ReportSpec             `json:"report_spec,omitempty"`
}

type BlockerProjection struct {
	BlockerID string                        `json:"blocker_id"`
	Target    coordination.PhaseEndpointRef `json:"target"`
	State     string                        `json:"state"`
	Reason    string                        `json:"reason,omitempty"`
}

type DecisionProjection struct {
	DecisionRef string         `json:"decision_ref"`
	Target      DecisionTarget `json:"target"`
	Action      string         `json:"action"`
	Disposition string         `json:"disposition"`
	CreatedAt   time.Time      `json:"created_at"`
}

type TaskDeliveryProjection struct {
	MergeState   string        `json:"merge_state,omitempty"`
	DeliveryRefs []ArtifactRef `json:"delivery_refs,omitempty"`
	ReportRef    *ArtifactRef  `json:"report_ref,omitempty"`
	EvidenceRefs []ArtifactRef `json:"evidence_refs,omitempty"`
}

type DeliverySpec struct {
	Kind                  string    `json:"kind"`
	RequiredArtifactKinds []string  `json:"required_artifact_kinds"`
	WriteSet              *WriteSet `json:"write_set,omitempty"`
	Acceptance            []string  `json:"acceptance,omitempty"`
}

type ReportSpec struct {
	RequiredSections    []string `json:"required_sections"`
	MustIncludeEvidence bool     `json:"must_include_evidence,omitempty"`
	MaxLength           int      `json:"max_length,omitempty"`
}

type WriteSet struct {
	Mode        string   `json:"mode"`
	Paths       []string `json:"paths"`
	AllowedDirs []string `json:"allowed_dirs,omitempty"`
	DeniedDirs  []string `json:"denied_dirs,omitempty"`
}

// UI query DTOs are aliases of the single rebuildable projection model. The
// transport does not own or duplicate a second graph/context representation.
type CoordinationSnapshot = uiprojection.CoordinationSnapshot
type TaskSummary = uiprojection.TaskSummary
type GraphNode = uiprojection.GraphNode
type GraphEdge = uiprojection.GraphEdge
type EndpointInspector = uiprojection.EndpointInspector
type InvocationProjection = uiprojection.InvocationProjection
type SubscriptionProjection = uiprojection.SubscriptionProjection
type ContextSliceView = uiprojection.ContextSliceView
type ContextNodeView = uiprojection.ContextNodeView
type OmittedContext = uiprojection.OmittedContext
type TaskMemoryBufferView = uiprojection.TaskMemoryBufferView

type ManagerConversation struct {
	ConversationID string                     `json:"conversation_id"`
	ProjectID      kernel.ProjectID           `json:"project_id"`
	Messages       []ManagerConversationEntry `json:"messages"`
	Cursor         string                     `json:"cursor"`
}

type ManagerConversationEntry struct {
	EntryID         string          `json:"entry_id"`
	Kind            string          `json:"kind"`
	CreatedAt       time.Time       `json:"created_at"`
	ManagerInputRef string          `json:"manager_input_ref,omitempty"`
	DecisionRef     string          `json:"decision_ref,omitempty"`
	GraphRevision   kernel.Revision `json:"graph_revision,omitempty"`
	Body            string          `json:"body,omitempty"`
	Disposition     string          `json:"disposition,omitempty"`
}

type HealthStatus struct {
	Status string `json:"status"`
}

type ReadinessStatus struct {
	Status       string                `json:"status"`
	Dependencies []DependencyReadiness `json:"dependencies"`
}

type DependencyReadiness struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
