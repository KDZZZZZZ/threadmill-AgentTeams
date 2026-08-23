package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRecoveryCoordinatorReconcileInvocationTerminalAndActiveActions(t *testing.T) {
	ctx := context.Background()
	t.Run("terminal cleanup converges then becomes no-op", func(t *testing.T) {
		repo, key, physical := acceptedTerminalRecoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer repo.Close()
		ports := &recoveryCleanupPorts{}
		coordinator := newRecoveryCoordinator(repo, "recoverer", ports, nil)
		result, err := coordinator.ReconcileInvocation(ctx, key)
		if err != nil || result.Action != RecoveryActionTerminalNoOp || result.PhysicalState != PhysicalExecutionTerminated {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		if ports.count(TeardownStepTask) != 1 || countRuntimeEvents(t, repo, "PhaseOutputSubmitted") != 1 {
			t.Fatalf("terminal recovery did not preserve output/event: calls=%d events=%d", ports.count(TeardownStepTask), countRuntimeEvents(t, repo, "PhaseOutputSubmitted"))
		}
		if got := currentPhysical(t, repo, key, physical.ExecutionEpoch); got.State != PhysicalExecutionTerminated {
			t.Fatalf("physical=%#v", got)
		}
		if result, err = coordinator.ReconcileInvocation(ctx, key); err != nil || result.Action != RecoveryActionTerminalNoOp || ports.count(TeardownStepTask) != 1 {
			t.Fatalf("repeat result=%#v err=%v calls=%d", result, err, ports.count(TeardownStepTask))
		}
	})
	t.Run("healthy observed carrier is no mutation", func(t *testing.T) {
		repo, key, physical := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer repo.Close()
		coordinator := replacementCoordinator(repo, "recoverer", PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified, Task: ObservedTaskInProgress})
		result, err := coordinator.ReconcileInvocation(ctx, key)
		if err != nil || result.Action != RecoveryActionCarrierActive || result.ExecutionEpoch != physical.ExecutionEpoch {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		if got := currentPhysical(t, repo, key, physical.ExecutionEpoch); got.State != PhysicalExecutionRunning || countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 0 {
			t.Fatalf("healthy carrier mutated: %#v", got)
		}
	})
	t.Run("unknown observation is retry without replacement", func(t *testing.T) {
		repo, key, physical := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer repo.Close()
		coordinator := replacementCoordinator(repo, "recoverer", PhysicalExecutionObservation{Worker: ObservedWorkerUnknown})
		result, err := coordinator.ReconcileInvocation(ctx, key)
		if err != nil || result.Action != RecoveryActionRetryRequired || !result.Retryable {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		if got := currentPhysical(t, repo, key, physical.ExecutionEpoch); got.State != PhysicalExecutionRunning || countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 0 {
			t.Fatalf("unknown observation mutated: %#v", got)
		}
	})
}

func TestRecoveryCoordinatorReconcileInvocationReservesOneLostCarrierEpoch(t *testing.T) {
	ctx := context.Background()
	repo, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	start := make(chan struct{})
	results := make(chan RecoveryReconciliationResult, 12)
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			coordinator := replacementCoordinator(repo, "recoverer-"+string(rune('a'+i)), PhysicalExecutionObservation{Worker: ObservedWorkerNotFound})
			result, err := coordinator.ReconcileInvocation(ctx, key)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrRecoveryClaimed) && !errors.Is(err, ErrRecoveryDispositionUnsupported) {
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	for result := range results {
		if result.Action != RecoveryActionReplacementReserved || result.ExecutionEpoch != old.ExecutionEpoch+1 {
			t.Fatalf("result=%#v", result)
		}
	}
	if countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 1 {
		t.Fatal("concurrent reconciliation allocated more than one successor")
	}
	reserved := currentPhysical(t, repo, key, old.ExecutionEpoch+1)
	if reserved.State != PhysicalExecutionReserved || reserved.ReplacesExecutionEpoch != old.ExecutionEpoch {
		t.Fatalf("reserved=%#v", reserved)
	}
}

func TestRecoveryCoordinatorReconcileInvocationConsumesC45AReservation(t *testing.T) {
	ctx := context.Background()
	repo, key, provisioner, issuer, ports := replacementReprovisionFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer repo.Close()
	scheduleReplacementReceipt(t, repo, issuer, ports)
	reprovision := &ReplacementReprovisionCoordinator{Repository: repo, Provisioner: provisioner, Surfaces: ExecutionSurfaces{Runtime: noopRuntime{}, ContextReader: noopReader{}, ContextAgent: noopAgent{}}, OwnerID: "recoverer", ClaimTTL: time.Minute}
	coordinator := newRecoveryCoordinator(repo, "recoverer", &recoveryCleanupPorts{}, nil)
	coordinator.Reprovision = reprovision
	result, err := coordinator.ReconcileInvocation(ctx, key)
	if err != nil || result.Action != RecoveryActionReplacementActive || result.PhysicalState != PhysicalExecutionRunning || result.ExecutionEpoch != 3 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 1 || countRuntimeEvents(t, repo, "PhysicalExecutionActivated") != 1 || countRuntimeEvents(t, repo, "PackageConsumptionRecorded") != 1 {
		t.Fatal("reservation was not consumed exactly once through C4-5A")
	}
	// A retry sees the verified fresh carrier, not another reservation or a
	// second carrier.  The C4-5A provisioner is never called again.
	coordinator.Observer = &observingPhysicalExecution{value: PhysicalExecutionObservation{Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified}}
	result, err = coordinator.ReconcileInvocation(ctx, key)
	if err != nil || result.Action != RecoveryActionCarrierActive || countRuntimeEvents(t, repo, "PhysicalExecutionActivated") != 1 {
		t.Fatalf("retry result=%#v err=%v activated=%d", result, err, countRuntimeEvents(t, repo, "PhysicalExecutionActivated"))
	}
}

func TestRecoveryCoordinatorReconcileInvocationColdReopenReusesReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	repo, key, old := activeRecoveryObservationFixture(t, path)
	coordinator := replacementCoordinator(repo, "recoverer-a", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound})
	if result, err := coordinator.ReconcileInvocation(ctx, key); err != nil || result.Action != RecoveryActionReplacementReserved {
		t.Fatalf("first result=%#v err=%v", result, err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	repo, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	// No C4-5A seam is supplied here: a reopened coordinator must preserve the
	// reservation and fail closed rather than allocate another physical epoch.
	coordinator = newRecoveryCoordinator(repo, "recoverer-b", &recoveryCleanupPorts{}, nil)
	result, err := coordinator.ReconcileInvocation(ctx, key)
	if !errors.Is(err, ErrRecoveryDispositionUnsupported) || result.Action != RecoveryActionFailClosed {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if got := currentPhysical(t, repo, key, old.ExecutionEpoch+1); got.State != PhysicalExecutionReserved || countRuntimeEvents(t, repo, "ReplacementEpochAllocated") != 1 {
		t.Fatalf("reopened reservation=%#v events=%d", got, countRuntimeEvents(t, repo, "ReplacementEpochAllocated"))
	}
}
