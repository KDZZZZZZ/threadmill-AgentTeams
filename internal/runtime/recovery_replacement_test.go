package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
)

func replacementCoordinator(r RuntimeStateRepository, owner string, observation PhysicalExecutionObservation) *RecoveryCoordinator {
	c := newRecoveryCoordinator(r, owner, &recoveryCleanupPorts{}, nil)
	c.Observer = &observingPhysicalExecution{value: observation}
	return c
}

func TestRecoveryReplacementFencesLostCarrierAndReservesHistoryMaximum(t *testing.T) {
	ctx := context.Background()
	r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	// Waiting remains at epoch 2, but physical history proves that epochs 1
	// and 4 were already used. Allocation must use max(history)+1, not +1.
	for _, epoch := range []ExecutionEpoch{1, 4} {
		if _, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: epoch, BindingRef: "historic", InputRevision: "historic", State: PhysicalExecutionTerminated}); err != nil {
			t.Fatal(err)
		}
	}
	c := replacementCoordinator(r, "runtime-a", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound})
	plan, err := c.PrepareLostCarrierReplacement(ctx, key)
	if err != nil || plan.OldExecutionEpoch != old.ExecutionEpoch || plan.NewExecutionEpoch != 5 || !plan.RequiresFreshCarrier || !plan.RequiresFreshReceipt || plan.BindingRef != old.BindingRef || plan.InputRevision != old.InputRevision {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	if got := currentPhysical(t, r, key, old.ExecutionEpoch); got.State != PhysicalExecutionFenced {
		t.Fatalf("old physical was not fenced: %#v", got)
	}
	reserved := currentPhysical(t, r, key, 5)
	if reserved.State != PhysicalExecutionReserved || reserved.ReplacesExecutionEpoch != old.ExecutionEpoch || !reserved.RequiresFreshPackageReceipt || reserved.WorkerID != "" || reserved.AgentSessionRef != "" {
		t.Fatalf("reservation=%#v", reserved)
	}
	waiting, found, err := r.WaitingStore().Get(ctx, key)
	if err != nil || !found || waiting.State != AwaitStateRehydrating || waiting.ExecutionEpoch != 5 || waiting.PreviousBindingRef != old.BindingRef || waiting.InputRevision != old.InputRevision {
		t.Fatalf("waiting=%#v found=%t err=%v", waiting, found, err)
	}
	if countRuntimeEvents(t, r, "PhysicalExecutionFenced") != 1 || countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 1 {
		t.Fatal("replacement events were not recorded exactly once")
	}
}

func TestRecoveryReplacementIsIdempotentAcrossColdReopenAndFencesLateOldMutation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, old := activeRecoveryObservationFixture(t, path)
	c := replacementCoordinator(r, "runtime-a", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound})
	first, err := c.PrepareLostCarrierReplacement(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c = replacementCoordinator(r, "runtime-b", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound})
	second, err := c.PrepareLostCarrierReplacement(ctx, key)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("first=%#v second=%#v err=%v", first, second, err)
	}
	if countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 1 {
		t.Fatal("retry allocated a second epoch")
	}
	late := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: old.BindingRef, InputRevision: old.InputRevision, ExecutionEpoch: old.ExecutionEpoch}
	if _, _, _, err = r.LifecycleMutations().AcceptPhaseOutput(ctx, late, key, 1); !errors.Is(err, ErrStaleCompletion) && !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("late old output err=%v", err)
	}
}

func TestRecoveryReplacementRejectsHealthyUnknownAndTerminatingObservations(t *testing.T) {
	ctx := context.Background()
	for name, observation := range map[string]PhysicalExecutionObservation{
		"healthy":            {Worker: ObservedWorkerReady, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified},
		"unknown":            {Worker: ObservedWorkerUnknown},
		"terminating":        {Worker: ObservedWorkerTerminating, Identity: ObservedCarrierIdentityVerified},
		"identity mismatch":  {Worker: ObservedWorkerReady, Identity: ObservedCarrierIdentityMismatch, Runtime: ObservedRuntimeApplied, MCP: ObservedMCPApplied},
		"generation pending": {Worker: ObservedWorkerReady, Runtime: ObservedRuntimeGenerationPending, MCP: ObservedMCPApplied, Identity: ObservedCarrierIdentityVerified},
	} {
		t.Run(name, func(t *testing.T) {
			r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
			defer r.Close()
			if _, err := replacementCoordinator(r, "runtime-a", observation).PrepareLostCarrierReplacement(ctx, key); !errors.Is(err, ErrRecoveryDispositionUnsupported) {
				t.Fatalf("err=%v", err)
			}
			if got := currentPhysical(t, r, key, old.ExecutionEpoch); got.State != PhysicalExecutionRunning || countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 0 {
				t.Fatalf("unexpected replacement: %#v", got)
			}
		})
	}
}

func TestRecoveryReplacementFailedCarrierPreservesOldReceiptAndArtifacts(t *testing.T) {
	ctx := context.Background()
	r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
	defer r.Close()
	old.State = PhysicalExecutionFailed
	if _, swapped, err := r.PhysicalExecutionStore().CompareAndSwap(ctx, old.Key(), old.Revision, old); err != nil || !swapped {
		t.Fatalf("failed state swapped=%t err=%v", swapped, err)
	}
	old = currentPhysical(t, r, key, old.ExecutionEpoch)
	if _, _, err := r.ReceiptStore().PutIfAbsent(ctx, executionreceipt.Receipt{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(old.ExecutionEpoch), BindingRef: old.BindingRef, InputRevision: old.InputRevision, PackageDigest: "digest", SessionIdentity: old.AgentSessionRef, Consumed: true}); err != nil {
		t.Fatal(err)
	}
	plan, err := replacementCoordinator(r, "runtime-a", PhysicalExecutionObservation{}).PrepareLostCarrierReplacement(ctx, key)
	if err != nil || plan.NewExecutionEpoch != 3 || !plan.RequiresFreshReceipt {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	if _, found, err := r.ReceiptStore().Get(ctx, executionreceipt.Key{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(old.ExecutionEpoch)}); err != nil || !found {
		t.Fatalf("old receipt found=%t err=%v", found, err)
	}
	if len(plan.ArtifactRefs) == 0 {
		t.Fatal("logical artifact refs were not preserved")
	}
}

func TestRecoveryReplacementRejectsStaleClaimAndAcceptedOutput(t *testing.T) {
	ctx := context.Background()
	t.Run("stale claim", func(t *testing.T) {
		r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		snapshot, err := r.Recovery().LoadRecoverySnapshot(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := r.Recovery().AcquireRecoveryClaim(ctx, key, old.ExecutionEpoch, "runtime-a", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.Recovery().ReleaseRecoveryClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if _, _, err = r.LifecycleMutations().FenceAndAllocateReplacement(ctx, claim, snapshot.Fingerprint(), CarrierRecoveryReplaceLost); !errors.Is(err, ErrRecoveryClaimLost) {
			t.Fatalf("err=%v", err)
		}
		if got := currentPhysical(t, r, key, old.ExecutionEpoch); got.State != PhysicalExecutionRunning {
			t.Fatalf("stale claim fenced old=%#v", got)
		}
	})
	t.Run("accepted output", func(t *testing.T) {
		r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		waiting, _, _ := r.WaitingStore().Get(ctx, key)
		output := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: old.BindingRef, InputRevision: old.InputRevision, ExecutionEpoch: old.ExecutionEpoch}
		if _, _, created, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, output, key, waiting.Revision); err != nil || !created {
			t.Fatalf("created=%t err=%v", created, err)
		}
		if _, err := replacementCoordinator(r, "runtime-a", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound}).PrepareLostCarrierReplacement(ctx, key); !errors.Is(err, ErrRecoveryDispositionUnsupported) {
			t.Fatalf("err=%v", err)
		}
		if countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 0 {
			t.Fatal("accepted output was replaced")
		}
	})
}

func TestRecoveryReplacementClaimSnapshotOutboxAndConcurrencyFences(t *testing.T) {
	ctx := context.Background()
	t.Run("stale observation", func(t *testing.T) {
		r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		o := &observingPhysicalExecution{value: PhysicalExecutionObservation{Worker: ObservedWorkerNotFound}, after: func() {
			value := currentPhysical(t, r, key, old.ExecutionEpoch)
			_, _, _ = r.PhysicalExecutionStore().CompareAndSwap(context.Background(), value.Key(), value.Revision, value)
		}}
		c := newRecoveryCoordinator(r, "runtime-a", &recoveryCleanupPorts{}, nil)
		c.Observer = o
		if _, err := c.PrepareLostCarrierReplacement(ctx, key); !errors.Is(err, ErrRecoverySnapshotStale) {
			t.Fatalf("err=%v", err)
		}
		if got := currentPhysical(t, r, key, old.ExecutionEpoch); got.State != PhysicalExecutionRunning || countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 0 {
			t.Fatalf("partial mutation=%#v", got)
		}
	})
	t.Run("outbox rollback", func(t *testing.T) {
		r, key, old := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		if _, err := r.db.Exec("CREATE TRIGGER reject_replacement BEFORE INSERT ON runtime_events WHEN NEW.event_type='ReplacementEpochAllocated' BEGIN SELECT RAISE(ABORT, 'outbox unavailable'); END"); err != nil {
			t.Fatal(err)
		}
		if _, err := replacementCoordinator(r, "runtime-a", PhysicalExecutionObservation{Worker: ObservedWorkerNotFound}).PrepareLostCarrierReplacement(ctx, key); err == nil {
			t.Fatal("expected outbox failure")
		}
		if got := currentPhysical(t, r, key, old.ExecutionEpoch); got.State != PhysicalExecutionRunning {
			t.Fatalf("fence survived rollback=%#v", got)
		}
		if _, found, err := r.PhysicalExecutionStore().Get(ctx, PhysicalExecutionKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: old.ExecutionEpoch + 1}); err != nil || found {
			t.Fatalf("reservation found=%t err=%v", found, err)
		}
		if countRuntimeEvents(t, r, "PhysicalExecutionFenced") != 0 || countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 0 {
			t.Fatal("outbox failure left success events")
		}
	})
	t.Run("twenty concurrent actors allocate once", func(t *testing.T) {
		r, key, _ := activeRecoveryObservationFixture(t, filepath.Join(t.TempDir(), "runtime.db"))
		defer r.Close()
		start := make(chan struct{})
		var wg sync.WaitGroup
		plans := make(chan RecoveryReplacementPlan, 20)
		errs := make(chan error, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				plan, err := replacementCoordinator(r, "runtime-"+string(rune('a'+i)), PhysicalExecutionObservation{Worker: ObservedWorkerNotFound}).PrepareLostCarrierReplacement(ctx, key)
				if err != nil {
					errs <- err
				} else {
					plans <- plan
				}
			}(i)
		}
		close(start)
		wg.Wait()
		close(plans)
		close(errs)
		for err := range errs {
			if !errors.Is(err, ErrRecoveryClaimed) {
				t.Fatalf("unexpected err=%v", err)
			}
		}
		for plan := range plans {
			if plan.NewExecutionEpoch != 3 {
				t.Fatalf("plan=%#v", plan)
			}
		}
		if countRuntimeEvents(t, r, "ReplacementEpochAllocated") != 1 {
			t.Fatal("more than one allocation")
		}
	})
}
