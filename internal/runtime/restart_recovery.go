package runtime

import (
	"context"
	"errors"
	"time"
)

// RecoveryConvergence classifies one supervisor observation. It is an
// operational result, not a second source of lifecycle authority.
type RecoveryConvergence string

const (
	RecoveryStableTerminal RecoveryConvergence = "stable_terminal"
	RecoveryStableActive   RecoveryConvergence = "stable_active"
	RecoveryProgressed     RecoveryConvergence = "progressed"
	RecoveryRetryable      RecoveryConvergence = "retryable"
	RecoveryContention     RecoveryConvergence = "contention"
	RecoveryFailClosed     RecoveryConvergence = "fail_closed"
	RecoveryUnsupported    RecoveryConvergence = "unsupported"
)

// RestartRecoveryItem is a secret-free report for one durable identity.
type RestartRecoveryItem struct {
	Key         WaitingKey
	Result      RecoveryReconciliationResult
	Convergence RecoveryConvergence
	Err         error
}

// RestartRecoveryReport contains a bounded startup pass. It deliberately does
// not retain claims, tokens, credentials, sessions, paths, or controller
// configuration.
type RestartRecoveryReport struct {
	Iterations int
	Items      []RestartRecoveryItem
}

// RestartRecoverySupervisor discovers candidates from Runtime SQLite and
// delegates every decision to the claim-fenced C4-5 coordinator. It has no
// global leader lock: concurrent supervisors are serialized per invocation by
// RecoveryClaim, and contention is a normal non-error convergence result.
type RestartRecoverySupervisor struct {
	Repository     RuntimeStateRepository
	Coordinator    *RecoveryCoordinator
	BatchSize      int
	MaxIterations  int
	RetryBackoff   time.Duration
	MaxRetryPerKey int
}

func (s RestartRecoverySupervisor) validate() error {
	if s.Repository == nil || s.Coordinator == nil || s.Coordinator.Repository != s.Repository {
		return errors.New("restart recovery supervisor requires one shared repository and coordinator")
	}
	if s.BatchSize <= 0 || s.MaxIterations <= 0 || s.MaxRetryPerKey <= 0 {
		return errors.New("restart recovery supervisor batch, iteration, and retry limits are required")
	}
	if s.RetryBackoff < 0 {
		return errors.New("restart recovery retry backoff cannot be negative")
	}
	return nil
}

// RecoverStartup runs a bounded recovery pass before normal scheduling is
// admitted. External state is consulted only inside ReconcileInvocation after
// discovery selected a durable identity. A caller may invoke it again after a
// controlled delay; C4-6 intentionally is not a background daemon.
func (s RestartRecoverySupervisor) RecoverStartup(ctx context.Context) (RestartRecoveryReport, error) {
	if err := s.validate(); err != nil {
		return RestartRecoveryReport{}, err
	}
	report := RestartRecoveryReport{}
	retries := map[WaitingKey]int{}
	for iteration := 0; iteration < s.MaxIterations; iteration++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		keys, err := s.Repository.Recovery().ListRecoveryCandidates(ctx, s.BatchSize)
		if err != nil {
			return report, err
		}
		report.Iterations++
		if len(keys) == 0 {
			return report, nil
		}
		pending := false
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			result, reconcileErr := s.Coordinator.ReconcileInvocation(ctx, key)
			item := RestartRecoveryItem{Key: key, Result: result, Err: reconcileErr, Convergence: classifyRecoveryOutcome(result, reconcileErr)}
			report.Items = append(report.Items, item)
			switch item.Convergence {
			case RecoveryProgressed:
				pending = true
			case RecoveryRetryable:
				retries[key]++
				pending = retries[key] < s.MaxRetryPerKey
			}
		}
		if !pending {
			return report, nil
		}
		if s.RetryBackoff > 0 {
			select {
			case <-ctx.Done():
				return report, ctx.Err()
			case <-time.After(s.RetryBackoff):
			}
		}
	}
	return report, nil
}

func classifyRecoveryOutcome(result RecoveryReconciliationResult, err error) RecoveryConvergence {
	if errors.Is(err, ErrRecoveryClaimed) {
		return RecoveryContention
	}
	if err != nil {
		if errors.Is(err, ErrRecoveryDispositionUnsupported) {
			return RecoveryUnsupported
		}
		return RecoveryFailClosed
	}
	switch result.Action {
	case RecoveryActionTerminalNoOp:
		return RecoveryStableTerminal
	case RecoveryActionCarrierActive, RecoveryActionAwaitingInput, RecoveryActionNoDurableInvocation:
		return RecoveryStableActive
	case RecoveryActionRetryRequired:
		return RecoveryRetryable
	case RecoveryActionTerminalCleanup, RecoveryActionReplacementReserved, RecoveryActionReplacementActive:
		return RecoveryProgressed
	default:
		return RecoveryFailClosed
	}
}
