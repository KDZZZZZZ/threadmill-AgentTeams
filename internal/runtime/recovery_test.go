package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/executionreceipt"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

type fixedRecoveryClock struct{ now time.Time }

func (c *fixedRecoveryClock) Now() time.Time { return c.now }

func recoveryFixture(t *testing.T, path string, state AwaitState, physicalState PhysicalExecutionState) (*SQLiteRuntimeStateRepository, WaitingKey, WaitingRecord, PhysicalExecution) {
	t.Helper()
	ctx := context.Background()
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	key := WaitingKey{TaskID: "task", InvocationID: "invocation", Generation: 2}
	if err = r.ContinuationStore().Put(ctx, "continuation-r5", ContinuationMaterial{Endpoint: phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: string(phaseagent.PhaseExecute)}, WorkspaceRef: "workspace", ContextSliceRef: "context", TaskMemoryBufferRef: "memory", ArtifactRefs: []artifacts.ArtifactRef{"artifact-existing"}}); err != nil {
		t.Fatal(err)
	}
	if err = r.InputStore().Put(ctx, key, StoredPhaseInputSet{Inputs: phaseagent.PhaseInputSet{InputRevision: "input-r5"}, AwaitConditionSatisfied: true}); err != nil {
		t.Fatal(err)
	}
	waiting, err := r.WaitingStore().Create(ctx, WaitingRecord{Key: key, ExecutionEpoch: 2, Endpoint: phaseagent.PhaseEndpointRef{TaskID: key.TaskID, EndpointID: string(phaseagent.PhaseExecute)}, PreviousBindingRef: "binding-b2", InputRevision: "input-r5", ContinuationRef: "continuation-r5", WorkspaceRef: "workspace", AllowedDirs: []string{"out"}, ContextSliceRef: "context", TaskMemoryBufferRef: "memory", State: state})
	if err != nil {
		t.Fatal(err)
	}
	physical, err := r.PhysicalExecutionStore().Create(ctx, PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, BindingRef: "binding-b2", InputRevision: "input-r5", State: physicalState})
	if err != nil {
		t.Fatal(err)
	}
	return r, key, waiting, physical
}

func TestRecoveryClaimLeaseFenceAndColdReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, _, _ := recoveryFixture(t, path, AwaitStateWaiting, PhysicalExecutionTerminated)
	clock := &fixedRecoveryClock{now: time.Date(2026, 8, 22, 1, 2, 3, 0, time.UTC)}
	r.clock = clock
	recovery := r.Recovery()
	first, err := recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-a", time.Minute)
	if err != nil || first.Fence != 1 || first.Revision != 1 {
		t.Fatalf("first claim=%#v err=%v", first, err)
	}
	if same, err := recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-a", time.Minute); err != nil || same != first {
		t.Fatalf("same owner retry=%#v err=%v", same, err)
	}
	if _, err := recovery.AcquireRecoveryClaim(ctx, key, 3, "runtime-a", time.Minute); !errors.Is(err, ErrRecoveryClaimLost) {
		t.Fatalf("same owner changed observed epoch err=%v", err)
	}
	if _, err := recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-b", time.Minute); !errors.Is(err, ErrRecoveryClaimed) {
		t.Fatalf("active takeover err=%v", err)
	}
	renewed, err := recovery.RenewRecoveryClaim(ctx, first, time.Minute)
	if err != nil || renewed.Fence != first.Fence || renewed.Revision != first.Revision+1 {
		t.Fatalf("renewed=%#v err=%v", renewed, err)
	}
	if err = recovery.AssertRecoveryClaim(ctx, first); !errors.Is(err, ErrRecoveryClaimLost) {
		t.Fatalf("old revision asserted after renew: %v", err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.clock = clock
	recovery = r.Recovery()
	stored, found, err := recovery.GetRecoveryClaim(ctx, key)
	if err != nil || !found || stored != renewed {
		t.Fatalf("reopened claim=%#v found=%t err=%v", stored, found, err)
	}
	if _, err = recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-b", time.Minute); !errors.Is(err, ErrRecoveryClaimed) {
		t.Fatalf("active reopen takeover err=%v", err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	takeover, err := recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-b", time.Minute)
	if err != nil || takeover.Fence != renewed.Fence+1 || takeover.Revision != renewed.Revision+1 {
		t.Fatalf("takeover=%#v err=%v", takeover, err)
	}
	if _, err = recovery.RenewRecoveryClaim(ctx, renewed, time.Minute); !errors.Is(err, ErrRecoveryClaimLost) {
		t.Fatalf("old owner renewed after takeover: %v", err)
	}
	if err = recovery.ReleaseRecoveryClaim(ctx, renewed); !errors.Is(err, ErrRecoveryClaimLost) {
		t.Fatalf("old owner released after takeover: %v", err)
	}
	if err = recovery.AssertRecoveryClaim(ctx, renewed); !errors.Is(err, ErrRecoveryClaimLost) {
		t.Fatalf("old fence asserted after takeover: %v", err)
	}
	if err = recovery.ReleaseRecoveryClaim(ctx, takeover); err != nil {
		t.Fatal(err)
	}
	reacquired, err := recovery.AcquireRecoveryClaim(ctx, key, 2, "runtime-c", time.Minute)
	if err != nil || reacquired.Fence != takeover.Fence+1 {
		t.Fatalf("reacquired=%#v err=%v", reacquired, err)
	}
}

func TestRecoveryClaimConcurrentAcquireAndTakeoverHaveSingleWinner(t *testing.T) {
	ctx := context.Background()
	r, key, _, _ := recoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"), AwaitStateWaiting, PhysicalExecutionTerminated)
	defer r.Close()
	clock := &fixedRecoveryClock{now: time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)}
	r.clock = clock
	claimRound := func(prefix string, epoch ExecutionEpoch) (RecoveryClaim, int, int) {
		start := make(chan struct{})
		claims := make(chan RecoveryClaim, 20)
		errs := make(chan error, 20)
		var group sync.WaitGroup
		for n := 0; n < 20; n++ {
			group.Add(1)
			go func(n int) {
				defer group.Done()
				<-start
				claim, err := r.Recovery().AcquireRecoveryClaim(ctx, key, epoch, prefix+"-"+string(rune('a'+n)), time.Minute)
				if err == nil {
					claims <- claim
				} else {
					errs <- err
				}
			}(n)
		}
		close(start)
		group.Wait()
		close(claims)
		close(errs)
		var winner RecoveryClaim
		winners, losers := 0, 0
		for claim := range claims {
			winner, winners = claim, winners+1
		}
		for err := range errs {
			if !errors.Is(err, ErrRecoveryClaimed) {
				t.Fatalf("unexpected concurrent claim error: %v", err)
			}
			losers++
		}
		return winner, winners, losers
	}
	first, winners, losers := claimRound("initial", 2)
	if winners != 1 || losers != 19 || first.Fence != 1 {
		t.Fatalf("initial winners=%d losers=%d claim=%#v", winners, losers, first)
	}
	_, winners, losers = claimRound("active", 2)
	if winners != 0 || losers != 20 {
		t.Fatalf("active winners=%d losers=%d", winners, losers)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	second, winners, losers := claimRound("takeover", 2)
	if winners != 1 || losers != 19 || second.Fence != 2 {
		t.Fatalf("takeover winners=%d losers=%d claim=%#v", winners, losers, second)
	}
}

func TestRecoverySnapshotIsConsistentAndClassifierIsDeterministic(t *testing.T) {
	ctx := context.Background()
	r, key, waiting, physical := recoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"), AwaitStateRunning, PhysicalExecutionRunning)
	defer r.Close()
	if _, _, err := r.ReceiptStore().PutIfAbsent(ctx, executionreceipt.Receipt{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: int64(physical.ExecutionEpoch), BindingRef: physical.BindingRef, InputRevision: physical.InputRevision, PackageDigest: "digest", SessionIdentity: "matrix:room", Consumed: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil || snapshot.Waiting == nil || snapshot.LatestInputs == nil || snapshot.Continuation == nil || snapshot.CurrentPhysical == nil || snapshot.Receipt == nil || len(snapshot.ArtifactRefs) != 1 || snapshot.WaitingRevision != waiting.Revision || snapshot.PhysicalRevision != physical.Revision || snapshot.CurrentExecutionEpoch != 2 || snapshot.BindingRef != "binding-b2" || snapshot.InputRevision != "input-r5" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	if fingerprint := snapshot.Fingerprint(); fingerprint.ExecutionEpoch != 2 || fingerprint.WaitingRevision != waiting.Revision || fingerprint.PhysicalRevision != physical.Revision {
		t.Fatalf("fingerprint=%#v", fingerprint)
	}
	if got, err := ClassifyRecoverySnapshot(snapshot); err != nil || got != RecoveryCarrierConsumedNoOutput {
		t.Fatalf("consumed classification=%q err=%v", got, err)
	}
}

func TestRecoveryClassificationDoesNotDependOnOutboxDispatch(t *testing.T) {
	ctx := context.Background()
	r, key, waiting, physical := recoveryFixture(t, filepath.Join(t.TempDir(), "runtime.db"), AwaitStateRunning, PhysicalExecutionRunning)
	defer r.Close()
	candidate := PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, BindingRef: physical.BindingRef, InputRevision: physical.InputRevision, ExecutionEpoch: physical.ExecutionEpoch, Output: phaseagent.PhaseOutput{ReportRef: "artifact-report"}}
	if _, _, created, err := r.LifecycleMutations().AcceptPhaseOutput(ctx, candidate, key, waiting.Revision); err != nil || !created {
		t.Fatalf("accept output created=%t err=%v", created, err)
	}
	// No dispatcher or consumer cursor is created. The authoritative output and
	// terminal Waiting state alone determine recovery classification.
	if cursor, err := r.EventOutbox().ConsumerCursor(ctx, "recovery-audit"); err != nil || cursor.LastAckedSequence != 0 {
		t.Fatalf("unexpected outbox progress=%#v err=%v", cursor, err)
	}
	snapshot, err := r.Recovery().LoadRecoverySnapshot(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ClassifyRecoverySnapshot(snapshot); err != nil || got != RecoveryContinueTerminalTeardown {
		t.Fatalf("classification=%q err=%v", got, err)
	}
}

func TestRecoveryClassifierTableAndContradictionsFailClosed(t *testing.T) {
	key := WaitingKey{TaskID: "task", InvocationID: "invocation", Generation: 2}
	baseWaiting := func(state AwaitState) *WaitingRecord {
		return &WaitingRecord{Key: key, ExecutionEpoch: 2, State: state}
	}
	inputs := &StoredPhaseInputSet{Inputs: phaseagent.PhaseInputSet{InputRevision: "r5"}}
	continuation := &ContinuationMaterial{}
	physical := func(state PhysicalExecutionState) *PhysicalExecution {
		return &PhysicalExecution{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, State: state}
	}
	output := &PhaseOutputRecord{Key: PhaseOutputKey{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation}, ExecutionEpoch: 2}
	cases := []struct {
		name string
		s    RecoverySnapshot
		want RecoveryDisposition
		err  bool
	}{
		{"brand new", RecoverySnapshot{Key: key}, RecoveryNoDurableInvocation, false},
		{"waiting", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateWaiting), LatestInputs: inputs, Continuation: continuation}, RecoveryAwaitingInput, false},
		{"preparing await", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStatePreparingAwait), LatestInputs: inputs, Continuation: continuation}, RecoveryRelinquishmentIncomplete, false},
		{"relinquishing", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRelinquishing), LatestInputs: inputs, Continuation: continuation}, RecoveryRelinquishmentIncomplete, false},
		{"rehydrating", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRehydrating), LatestInputs: inputs, Continuation: continuation}, RecoveryRehydrationIncomplete, false},
		{"provisioning", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRehydrating), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionProvisioning)}, RecoveryCarrierProvisioningIncomplete, false},
		{"accepted", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRehydrating), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionAccepted)}, RecoveryCarrierProvisioningIncomplete, false},
		{"active", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionRunning)}, RecoveryCarrierActiveNeedsObservation, false},
		{"receipt", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionRunning), Receipt: &executionreceipt.Receipt{TaskID: key.TaskID, InvocationID: key.InvocationID, Generation: key.Generation, ExecutionEpoch: 2, Consumed: true}}, RecoveryCarrierConsumedNoOutput, false},
		{"artifact not output", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation, ArtifactRefs: []artifacts.ArtifactRef{"artifact"}, CurrentPhysical: physical(PhysicalExecutionRunning)}, RecoveryCarrierActiveNeedsObservation, false},
		{"terminal teardown", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateTerminal), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionTearingDown), Output: output}, RecoveryContinueTerminalTeardown, false},
		{"terminal no-op", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateTerminal), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionTerminated), Output: output}, RecoveryTerminalNoOp, false},
		{"failed", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionFailed)}, RecoveryFailedPhysicalExecutionNeedsDecision, false},
		{"output without terminal", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation, CurrentPhysical: physical(PhysicalExecutionRunning), Output: output}, "", true},
		{"running without carrier", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateRunning), LatestInputs: inputs, Continuation: continuation}, "", true},
		{"waiting without continuation", RecoverySnapshot{Key: key, Waiting: baseWaiting(AwaitStateWaiting), LatestInputs: inputs}, "", true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := ClassifyRecoverySnapshot(test.s)
			if test.err {
				if !errors.Is(err, ErrRecoverySnapshotInconsistent) {
					t.Fatalf("classification=%q err=%v", got, err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("classification=%q want=%q err=%v", got, test.want, err)
			}
		})
	}
}

func TestRecoverySchemaV3MigratesWithoutChangingLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runtime.db")
	r, key, _, _ := recoveryFixture(t, path, AwaitStateWaiting, PhysicalExecutionTerminated)
	if _, err := r.db.Exec("DROP TABLE runtime_recovery_claims"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.Exec("UPDATE runtime_schema_version SET version=3"); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSQLiteRuntimeStateRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if version, err := r.SchemaVersion(ctx); err != nil || version != latestRuntimeSchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	if snapshot, err := r.Recovery().LoadRecoverySnapshot(ctx, key); err != nil || snapshot.Waiting == nil || snapshot.CurrentPhysical == nil {
		t.Fatalf("migrated snapshot=%#v err=%v", snapshot, err)
	}
}
