package uiprojection

import (
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const InvocationProviderAgentTeams = "agentteams_qwenpaw_taskflow"

// CapacityState is a presentation projection over scheduler.Capacity. Waiting
// is derived from logical Invocation state and is never written back to the
// scheduler ledger.
type CapacityState struct {
	ProjectID          kernel.ProjectID `json:"project_id"`
	Revision           int              `json:"revision"`
	DesiredConcurrency int              `json:"desired_concurrency"`
	HealthyCapacity    int              `json:"healthy_capacity"`
	ActiveInvocations  int              `json:"active_invocations"`
	WaitingInvocations int              `json:"waiting_invocations"`
	DegradedReason     string           `json:"degraded_reason,omitempty"`
	UpdatedAt          time.Time        `json:"updated_at"`
}

// CoordinationSnapshot is rebuilt from the authoritative Coordination Graph,
// scheduler capacity, Invocation store, and Event Log cursor. It is deliberately
// a read DTO: no graph mutation methods accept it.
type CoordinationSnapshot struct {
	ProjectID kernel.ProjectID `json:"project_id"`
	Revision  kernel.Revision  `json:"revision"`
	Cursor    string           `json:"cursor"`
	Tasks     []TaskSummary    `json:"tasks"`
	Nodes     []GraphNode      `json:"nodes"`
	Edges     []GraphEdge      `json:"edges"`
	Capacity  CapacityState    `json:"capacity"`
}

type TaskSummary struct {
	TaskID kernel.TaskID `json:"task_id"`
	Status string        `json:"status"`
}

type GraphNode struct {
	ID                  string                 `json:"id"`
	Kind                string                 `json:"kind"`
	Label               string                 `json:"label"`
	TaskID              kernel.TaskID          `json:"task_id"`
	EndpointID          kernel.EndpointID      `json:"endpoint_id"`
	Generation          int                    `json:"generation"`
	State               string                 `json:"state"`
	RunPolicy           coordination.RunPolicy `json:"run_policy"`
	BindingRef          kernel.BindingRef      `json:"binding_ref,omitempty"`
	LatestInvocationRef kernel.InvocationID    `json:"latest_invocation_ref,omitempty"`
}

type GraphEdge struct {
	ID            string                        `json:"id"`
	From          coordination.PhaseEndpointRef `json:"from"`
	To            coordination.PhaseEndpointRef `json:"to"`
	RequiredBy    coordination.RequiredBy       `json:"required_by"`
	State         string                        `json:"state"`
	ArtifactKinds []string                      `json:"artifact_kinds,omitempty"`
}

// EndpointInspector keeps the three Context areas separate. Invocation and
// its materialized Context Slice are absent when the endpoint has never run;
// the projection never fabricates placeholder Invocation IDs or Context refs.
type EndpointInspector struct {
	Endpoint         coordination.PhaseEndpointRef `json:"endpoint"`
	Generation       int                           `json:"generation"`
	GraphRevision    kernel.Revision               `json:"graph_revision"`
	Invocation       *InvocationProjection         `json:"invocation,omitempty"`
	Subscriptions    []SubscriptionProjection      `json:"subscriptions"`
	ContextSlice     *ContextSliceView             `json:"context_slice,omitempty"`
	TaskMemoryBuffer *TaskMemoryBufferView         `json:"task_memory_buffer,omitempty"`
}

type InvocationProjection struct {
	InvocationID        kernel.InvocationID `json:"invocation_id"`
	Provider            string              `json:"provider,omitempty"`
	Status              string              `json:"status"`
	StartedAt           *time.Time          `json:"started_at,omitempty"`
	EndedAt             *time.Time          `json:"ended_at,omitempty"`
	InputRevision       string              `json:"input_revision,omitempty"`
	WorkspaceRef        string              `json:"workspace_ref,omitempty"`
	ContextSliceRef     string              `json:"context_slice_ref,omitempty"`
	TaskMemoryBufferRef string              `json:"task_memory_buffer_ref,omitempty"`
}

type SubscriptionProjection struct {
	SubscriptionID string   `json:"subscription_id"`
	SubgraphIDs    []string `json:"subgraph_ids"`
	Active         bool     `json:"active"`
	Source         string   `json:"source,omitempty"`
}

type ContextSliceView struct {
	ContextSliceRef string            `json:"context_slice_ref"`
	Revision        string            `json:"revision"`
	Nodes           []ContextNodeView `json:"nodes"`
	Frontier        []string          `json:"frontier,omitempty"`
	Omitted         []OmittedContext  `json:"omitted"`
}

type ContextNodeView struct {
	NodeID      string   `json:"node_id"`
	Kind        string   `json:"kind"`
	Statement   string   `json:"statement"`
	Status      string   `json:"status,omitempty"`
	SourceRefs  []string `json:"source_refs"`
	SubgraphIDs []string `json:"subgraph_ids,omitempty"`
}

type OmittedContext struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type TaskMemoryBufferView struct {
	TaskMemoryBufferRef string                                 `json:"task_memory_buffer_ref"`
	Candidates          []contextgraph.TaskMemoryCandidateView `json:"candidates"`
	Omitted             []OmittedContext                       `json:"omitted,omitempty"`
}
