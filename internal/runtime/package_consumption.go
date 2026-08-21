package runtime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// PackageConsumptionCoordinator validates an agent-originated receipt against
// token-bound identity and the authoritative physical carrier record.
type PackageConsumptionCoordinator struct {
	Store              executionreceipt.Store
	PhysicalExecutions PhysicalExecutionStore
	Mutations          LifecycleMutationStore
	PollInterval       time.Duration
}

func (c *PackageConsumptionCoordinator) ConfirmPackageConsumption(ctx context.Context, binding phasemcp.InvocationBinding, submission executionreceipt.Submission) (executionreceipt.Receipt, error) {
	if c == nil || c.Store == nil || c.PhysicalExecutions == nil {
		return executionreceipt.Receipt{}, errors.New("package consumption authority is unavailable")
	}
	if !submission.Consumed || submission.PackageDigest == "" {
		return executionreceipt.Receipt{}, errors.New("consumed package digest is required")
	}
	if binding.TaskID == "" || binding.InvocationID == "" || binding.Generation <= 0 || binding.ExecutionEpoch <= 0 || binding.BindingRef == "" || binding.InputRevision == "" {
		return executionreceipt.Receipt{}, errors.New("execution token lacks rehydration authority")
	}
	key := PhysicalExecutionKey{TaskID: binding.TaskID, InvocationID: binding.InvocationID, Generation: binding.Generation, ExecutionEpoch: ExecutionEpoch(binding.ExecutionEpoch)}
	interval := c.PollInterval
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}
	for {
		execution, found, err := c.PhysicalExecutions.Get(ctx, key)
		if err != nil {
			return executionreceipt.Receipt{}, err
		}
		if found && execution.AgentPackageDigest != "" && execution.AgentSessionRef != "" {
			if execution.State != PhysicalExecutionAccepted || execution.Teardown.TokenRevoked || execution.Teardown.WorkerDeleted {
				return executionreceipt.Receipt{}, errors.New("physical execution cannot accept package consumption")
			}
			if execution.BindingRef != binding.BindingRef || execution.InputRevision != binding.InputRevision {
				return executionreceipt.Receipt{}, errors.New("execution binding does not match physical carrier")
			}
			if submission.PackageDigest != execution.AgentPackageDigest {
				return executionreceipt.Receipt{}, errors.New("package digest does not match authoritative execution package")
			}
			if !strings.HasPrefix(execution.AgentSessionRef, "matrix:") || (submission.SessionIdentity != "" && submission.SessionIdentity != execution.AgentSessionRef) {
				return executionreceipt.Receipt{}, errors.New("session identity does not match authoritative Matrix session")
			}
			candidate := executionreceipt.Receipt{
				TaskID: binding.TaskID, InvocationID: binding.InvocationID, Generation: binding.Generation,
				ExecutionEpoch: binding.ExecutionEpoch, BindingRef: binding.BindingRef, InputRevision: binding.InputRevision,
				PackageDigest: submission.PackageDigest, SessionIdentity: execution.AgentSessionRef, Consumed: true,
			}
			if c.Mutations != nil {
				receipt, _, _, err := c.Mutations.RecordPackageConsumption(ctx, candidate, key, execution.Revision)
				return receipt, err
			}
			receipt, _, err := c.Store.PutIfAbsent(ctx, candidate)
			return receipt, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return executionreceipt.Receipt{}, ctx.Err()
		case <-timer.C:
		}
	}
}
