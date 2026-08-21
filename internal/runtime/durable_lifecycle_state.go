package runtime

import (
	"errors"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

// DurableLifecycleState is the Runtime-owned composition of the durable
// logical-state stores used by the M4 await/rehydration/completion lifecycle.
//
// Every field is sourced from one RuntimeStateRepository. This prevents a
// caller from accidentally recovering WaitingRecord from SQLite while keeping
// inputs, bindings, receipts, or physical epoch history in a process-local
// store. It deliberately contains no execution capability, credential, or
// agent-session material; those remain disposable physical resources.
type DurableLifecycleState struct {
	Repository         RuntimeStateRepository
	Waiting            WaitingStore
	Continuations      DurableContinuationStore
	Inputs             DurablePhaseInputStore
	PhysicalExecutions PhysicalExecutionStore
	Receipts           executionreceipt.Store
	Outputs            PhaseOutputStore
	Mutations          LifecycleMutationStore
}

// NewDurableLifecycleState wires every M4 logical-state authority to the same
// durable repository. Coordinators continue to receive their narrow store
// interfaces, so this does not expand phaseagent's public contract.
func NewDurableLifecycleState(repository RuntimeStateRepository) (DurableLifecycleState, error) {
	if repository == nil {
		return DurableLifecycleState{}, errors.New("runtime state repository is required")
	}
	state := DurableLifecycleState{
		Repository:         repository,
		Waiting:            repository.WaitingStore(),
		Continuations:      repository.ContinuationStore(),
		Inputs:             repository.InputStore(),
		PhysicalExecutions: repository.PhysicalExecutionStore(),
		Receipts:           repository.ReceiptStore(),
		Outputs:            repository.PhaseOutputStore(),
		Mutations:          repository.LifecycleMutations(),
	}
	if state.Waiting == nil || state.Continuations == nil || state.Inputs == nil || state.PhysicalExecutions == nil || state.Receipts == nil || state.Outputs == nil || state.Mutations == nil {
		return DurableLifecycleState{}, errors.New("runtime state repository returned an incomplete lifecycle authority")
	}
	return state, nil
}
