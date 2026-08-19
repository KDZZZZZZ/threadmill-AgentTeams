package runtime

import (
	"context"
	"errors"
	"reflect"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// RehydratedExecutionPackage is the complete agent-visible logical material
// for a new physical carrier. It deliberately excludes Runtime synchronization
// revisions, credentials, transport headers, controller authentication, and
// model/provider session state.
type RehydratedExecutionPackage struct {
	TaskID           string                          `json:"task_id"`
	InvocationID     string                          `json:"invocation_id"`
	Generation       int                             `json:"generation"`
	ExecutionEpoch   ExecutionEpoch                  `json:"execution_epoch"`
	Endpoint         phaseagent.PhaseEndpointRef     `json:"endpoint"`
	BindingRef       string                          `json:"binding_ref"`
	InputRevision    string                          `json:"input_revision"`
	Inputs           phaseagent.PhaseInputSet        `json:"inputs"`
	NewlyDelivered   []phaseagent.InputDelivery      `json:"newly_delivered_inputs,omitempty"`
	TaskContract     string                          `json:"task_contract"`
	PhaseInstruction string                          `json:"phase_instruction"`
	Context          AgentVisibleContext             `json:"context"`
	TaskMemory       phaseagent.TaskMemoryBufferView `json:"task_memory"`
	Workspace        AgentVisibleWorkspace           `json:"workspace"`
	ArtifactRefs     []artifacts.ArtifactRef         `json:"artifact_refs,omitempty"`
	EventRefs        []string                        `json:"event_refs,omitempty"`
	EvidenceRefs     []string                        `json:"evidence_refs,omitempty"`
}

type AgentVisibleContext struct {
	SliceRef    string `json:"slice_ref"`
	BaselineRef string `json:"baseline_ref,omitempty"`
	Content     string `json:"content"`
}

type AgentVisibleWorkspace struct {
	Ref         string   `json:"ref"`
	Revision    string   `json:"revision,omitempty"`
	AllowedDirs []string `json:"allowed_dirs"`
}

// RehydratedExecutionPackageMaterializer resolves Runtime-owned references
// into the restricted view that may be projected to TeamHarness and a future
// fresh QwenPaw agent session.
type RehydratedExecutionPackageMaterializer interface {
	MaterializeRehydratedExecution(context.Context, RehydrationPlan) (RehydratedExecutionPackage, error)
}

func ValidateRehydratedExecutionPackage(plan RehydrationPlan, value RehydratedExecutionPackage) error {
	if value.TaskID != plan.TaskID || value.InvocationID != plan.InvocationID || value.Generation != plan.Generation || value.ExecutionEpoch != plan.NextExecutionEpoch || value.Endpoint != plan.Endpoint {
		return errors.New("rehydrated execution package identity does not match plan")
	}
	if value.BindingRef != plan.NewBindingRef || value.InputRevision != plan.NewInputRevision || value.Inputs.InputRevision != plan.NewInputRevision {
		return errors.New("rehydrated execution package binding does not match plan")
	}
	if value.TaskContract == "" || value.PhaseInstruction == "" {
		return errors.New("rehydrated execution package task contract and phase instruction are required")
	}
	if value.Context.SliceRef != plan.Context.SliceRef || value.Context.BaselineRef != plan.Context.BaselineRef || !reflect.DeepEqual(value.TaskMemory, plan.TaskMemory.View) {
		return errors.New("rehydrated execution package context or task memory does not match plan")
	}
	if !reflect.DeepEqual(value.Inputs, plan.Inputs) || !reflect.DeepEqual(value.NewlyDelivered, plan.NewlyDelivered) {
		return errors.New("rehydrated execution package inputs do not match plan")
	}
	for _, newlyDelivered := range value.NewlyDelivered {
		found := false
		for _, delivered := range value.Inputs.Delivered {
			if reflect.DeepEqual(newlyDelivered, delivered) {
				found = true
				break
			}
		}
		if !found {
			return errors.New("newly delivered input is not present in the complete input set")
		}
	}
	if value.Workspace.Ref != plan.Workspace.Ref || !allowedDirsWithin(value.Workspace.AllowedDirs, plan.Workspace.AllowedDirs) {
		return errors.New("rehydrated execution package workspace is incompatible with plan")
	}
	if !reflect.DeepEqual(value.ArtifactRefs, plan.ArtifactRefs) || !reflect.DeepEqual(value.EventRefs, plan.EventRefs) || !reflect.DeepEqual(value.EvidenceRefs, plan.EvidenceRefs) {
		return errors.New("rehydrated execution package continuation references do not match plan")
	}
	return nil
}
