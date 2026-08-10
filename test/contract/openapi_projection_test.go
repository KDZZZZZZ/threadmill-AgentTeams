package contract_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/contextgraph"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/transport/httpapi"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
	"github.com/getkin/kin-openapi/openapi3"
)

func TestOpenAPIProjectionSchemasAcceptCanonicalDTOs(t *testing.T) {
	t.Parallel()
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile(filepath.Join("..", "..", "api", "openapi", "threadmill-v1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}

	now := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	ref := coordination.PhaseEndpointRef{TaskID: "task-1", EndpointID: coordination.EndpointExecute}
	assertSchemaAccepts(t, document, "CoordinationSnapshot", uiprojection.CoordinationSnapshot{
		ProjectID: "project-1",
		Revision:  7,
		Cursor:    "42",
		Tasks:     []uiprojection.TaskSummary{{TaskID: ref.TaskID, Status: "running"}},
		Nodes: []uiprojection.GraphNode{{
			ID:                  "task-1/execute",
			Kind:                "endpoint",
			Label:               "execute",
			TaskID:              ref.TaskID,
			EndpointID:          ref.EndpointID,
			Generation:          2,
			State:               "running",
			RunPolicy:           coordination.RunEnabled,
			BindingRef:          "binding-2",
			LatestInvocationRef: "inv-2",
		}},
		Edges: []uiprojection.GraphEdge{},
		Capacity: uiprojection.CapacityState{
			ProjectID:          "project-1",
			Revision:           3,
			DesiredConcurrency: 4,
			HealthyCapacity:    3,
			ActiveInvocations:  2,
			WaitingInvocations: 1,
			UpdatedAt:          now,
		},
	})

	assertSchemaAccepts(t, document, "EndpointInspector", uiprojection.EndpointInspector{
		Endpoint:      ref,
		Generation:    2,
		GraphRevision: 7,
		Subscriptions: []uiprojection.SubscriptionProjection{},
	})

	invocation := uiprojection.InvocationProjection{
		InvocationID:        "inv-2",
		Provider:            uiprojection.InvocationProviderAgentTeams,
		Status:              "running",
		StartedAt:           &now,
		ContextSliceRef:     "slice-2",
		TaskMemoryBufferRef: "buffer-1",
	}
	contextSlice := uiprojection.ContextSliceView{
		ContextSliceRef: "slice-2",
		Revision:        "19",
		Nodes: []uiprojection.ContextNodeView{{
			NodeID:      "node-1",
			Kind:        "fact",
			Statement:   "project fact",
			Status:      "accepted",
			SourceRefs:  []string{"artifact-1"},
			SubgraphIDs: []string{"general-1"},
		}},
		Frontier: []string{"node:next"},
		Omitted:  []uiprojection.OmittedContext{},
	}
	memory := uiprojection.TaskMemoryBufferView{
		TaskMemoryBufferRef: "buffer-1",
		Candidates: []contextgraph.TaskMemoryCandidateView{{
			CandidateID: "candidate-1",
			Candidate: contextgraph.MemoryCandidate{
				Statement:   "candidate fact",
				Kind:        "fact",
				SourceRefs:  []string{"artifact-1"},
				SubgraphIDs: []string{"general-1"},
			},
		}},
		Omitted: []uiprojection.OmittedContext{},
	}
	assertSchemaAccepts(t, document, "EndpointInspector", uiprojection.EndpointInspector{
		Endpoint:         ref,
		Generation:       2,
		GraphRevision:    kernel.Revision(7),
		Invocation:       &invocation,
		Subscriptions:    []uiprojection.SubscriptionProjection{{SubscriptionID: "sub-1", SubgraphIDs: []string{"general-1"}, Active: true, Source: "search"}},
		ContextSlice:     &contextSlice,
		TaskMemoryBuffer: &memory,
	})

	assertSchemaAccepts(t, document, "Error", kernel.Error{Code: kernel.CodeRevisionConflict, Message: "revision changed", Recoverable: true})
	uiEvent := uiprojection.UIEvent{
		EventID:    "evt-42",
		Cursor:     "42",
		Type:       "capacity.updated",
		OccurredAt: now,
		ProjectID:  "project-1",
		Payload: map[string]any{
			"project_id":          "project-1",
			"revision":            3,
			"desired_concurrency": 4,
			"healthy_capacity":    3,
			"active_invocations":  2,
			"waiting_invocations": 1,
			"updated_at":          now,
		},
	}
	assertSchemaAccepts(t, document, "UiEvent", uiEvent)
	assertSchemaAccepts(t, document, "EventPage", uiprojection.EventPage{Events: []uiprojection.UIEvent{uiEvent}, NextCursor: "42"})
	assertSchemaAccepts(t, document, "HealthStatus", httpapi.HealthStatus{Status: "ok"})
	assertSchemaAccepts(t, document, "ReadinessStatus", httpapi.ReadinessStatus{
		Status: "not_ready",
		Dependencies: []httpapi.DependencyReadiness{
			{Name: "agentteams", Status: "unavailable", Message: "host offline"},
		},
	})
}

func assertSchemaAccepts(t *testing.T, document *openapi3.T, name string, value any) {
	t.Helper()
	schemaRef, ok := document.Components.Schemas[name]
	if !ok || schemaRef == nil || schemaRef.Value == nil {
		t.Fatalf("OpenAPI schema %q is missing", name)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := schemaRef.Value.VisitJSON(decoded); err != nil {
		t.Fatalf("%s rejected canonical JSON %s: %v", name, raw, err)
	}
}
