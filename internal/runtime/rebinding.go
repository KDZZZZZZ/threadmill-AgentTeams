package runtime

import (
	"context"
	"errors"
)

// ContinuationBinding is the immutable BindingRef transition required when
// new input revision arrives for an awaited logical Invocation. It never
// mutates the previous BindingRef and does not create a physical worker.
type ContinuationBinding struct {
	InvocationID       string         `json:"invocation_id"`
	Generation         int            `json:"generation"`
	ExecutionEpoch     ExecutionEpoch `json:"execution_epoch"`
	PreviousBindingRef string         `json:"previous_binding_ref"`
	PreviousRevision   string         `json:"previous_input_revision"`
	BindingRef         string         `json:"binding_ref"`
	InputRevision      string         `json:"input_revision"`
}

// InputContinuationRebinder lets a Runtime-owned binding service create a new
// immutable BindingRef for current inputs. It intentionally has no worker or
// checkpoint responsibility.
type InputContinuationRebinder interface {
	RebindInputsForContinuation(context.Context, ContinuationBinding) (ContinuationBinding, error)
}

// ContinuationBindingResolver reads immutable binding history after a Runtime
// process is reopened. It exposes only logical identifiers and revisions.
type ContinuationBindingResolver interface {
	ResolveContinuationBinding(context.Context, string) (ContinuationBinding, bool, error)
}

// ValidateContinuationRebind enforces the await-specific invariant: new
// inputs require a new BindingRef while logical InvocationID and generation
// remain stable. Checkpoint resume is deliberately outside this contract.
func ValidateContinuationRebind(binding ContinuationBinding) error {
	if binding.InvocationID == "" || binding.Generation <= 0 || binding.ExecutionEpoch <= 0 {
		return errors.New("continuation binding invocation_id, generation, and execution_epoch are required")
	}
	if binding.PreviousBindingRef == "" || binding.PreviousRevision == "" || binding.BindingRef == "" || binding.InputRevision == "" {
		return errors.New("continuation binding refs and input revisions are required")
	}
	if binding.PreviousBindingRef == binding.BindingRef {
		return errors.New("continuation binding must create a new binding_ref")
	}
	if binding.PreviousRevision == binding.InputRevision {
		return errors.New("continuation binding must use a new input_revision")
	}
	return nil
}
