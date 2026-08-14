package mergequeue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

func TestReconcilerClaimRenewalFailureCancelsVerifyAndFailsClosed(t *testing.T) {
	h := newHarness(t)
	store := &renewalTestStore{
		Store:   h.store,
		ttl:     60 * time.Millisecond,
		failAt:  2,
		failErr: kernel.LeaseConflict("test claim renewal lost ownership"),
	}
	verifyErr := errors.New("targeted verifier stopped after lease loss")
	verifier := &cancelReportingVerifier{entered: make(chan struct{}), err: verifyErr}
	h.reconciler = NewReconciler(store, h.workspaces, verifier, GitBackend{TempParent: t.TempDir()}, h.artifacts, h.events)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-renew-failure", binding)
	// Claim renewal is exercised only on the conflict-only targeted Verify
	// path. A clean main drift is intentionally merged mechanically.
	pushChange(t, h.repo, "workspace/a.txt", "main conflicting change\n")
	mainBefore := gitOut(t, h.repo, "rev-parse", "refs/heads/main")

	done := make(chan reconcileOutcome, 1)
	go func() {
		candidate, claimed, err := h.reconciler.ReconcileOne(context.Background(), h.repo)
		done <- reconcileOutcome{candidate: candidate, claimed: claimed, err: err}
	}()
	waitClosed(t, verifier.entered, "targeted verifier did not start")

	got := waitReconcile(t, done)
	if !got.claimed || !kernel.IsCode(got.err, kernel.CodeLeaseConflict) {
		t.Fatalf("renewal failure = candidate:%#v claimed:%v err:%v, want lease_conflict", got.candidate, got.claimed, got.err)
	}
	if !errors.Is(got.err, verifyErr) {
		t.Fatalf("renewal failure masked verifier error: %v", got.err)
	}
	if store.calls.Load() < 2 {
		t.Fatalf("RenewClaim calls = %d, want initial renew plus heartbeat", store.calls.Load())
	}
	if mainAfter := gitOut(t, h.repo, "rev-parse", "refs/heads/main"); mainAfter != mainBefore {
		t.Fatalf("lost claim changed main from %s to %s", mainBefore, mainAfter)
	}
	stored, err := h.store.Get(context.Background(), "candidate-renew-failure")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != StatusTargetedVerify || stored.MergedRevision != "" {
		t.Fatalf("lost claim advanced candidate: %#v", stored)
	}
}

func TestReconcilerCancellationStopsClaimRenewal(t *testing.T) {
	h := newHarness(t)
	store := &renewalTestStore{Store: h.store, ttl: 60 * time.Millisecond}
	verifier := &fakeVerifier{
		result:  h.verifier.result,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	h.reconciler = NewReconciler(store, h.workspaces, verifier, GitBackend{TempParent: t.TempDir()}, h.artifacts, h.events)
	binding := h.workspace(t, "task-a", 1, "workspace/a.txt", "candidate\n")
	h.enqueue(t, "candidate-cancel-renewal", binding)
	// Manufacture a real conflict; clean drift does not invoke the verifier.
	pushChange(t, h.repo, "workspace/a.txt", "main conflicting change\n")
	mainBefore := gitOut(t, h.repo, "rev-parse", "refs/heads/main")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan reconcileOutcome, 1)
	go func() {
		candidate, claimed, err := h.reconciler.ReconcileOne(ctx, h.repo)
		done <- reconcileOutcome{candidate: candidate, claimed: claimed, err: err}
	}()
	waitClosed(t, verifier.entered, "targeted verifier did not start")
	waitRenewCalls(t, store, 2)
	cancel()

	got := waitReconcile(t, done)
	if !got.claimed || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancelled reconcile = candidate:%#v claimed:%v err:%v, want context canceled", got.candidate, got.claimed, got.err)
	}
	callsAfterReturn := store.calls.Load()
	time.Sleep(3 * store.ttl)
	if calls := store.calls.Load(); calls != callsAfterReturn {
		t.Fatalf("RenewClaim continued after reconcile returned: before=%d after=%d", callsAfterReturn, calls)
	}
	if mainAfter := gitOut(t, h.repo, "rev-parse", "refs/heads/main"); mainAfter != mainBefore {
		t.Fatalf("cancelled reconcile changed main from %s to %s", mainBefore, mainAfter)
	}
}

func TestClaimRenewalDelayRenewsEarlyAndCapsClockSkew(t *testing.T) {
	now := time.Now()
	short := claimRenewalDelay(now.Add(9 * time.Second))
	if short < 2*time.Second || short > 4*time.Second {
		t.Fatalf("short renewal delay = %v, want approximately one third of TTL", short)
	}
	if skewed := claimRenewalDelay(now.Add(24 * time.Hour)); skewed != 30*time.Second {
		t.Fatalf("clock-skew renewal delay = %v, want 30s cap", skewed)
	}
	if expired := claimRenewalDelay(now.Add(-time.Second)); expired != 0 {
		t.Fatalf("expired renewal delay = %v, want immediate", expired)
	}
}

type reconcileOutcome struct {
	candidate Candidate
	claimed   bool
	err       error
}

type renewalTestStore struct {
	Store
	ttl     time.Duration
	failAt  int32
	failErr error
	calls   atomic.Int32
}

func (s *renewalTestStore) ClaimNext(ctx context.Context, repository string) (Claim, bool, error) {
	claim, claimed, err := s.Store.ClaimNext(ctx, repository)
	if claimed {
		claim.ExpiresAt = time.Now().Add(s.ttl)
	}
	return claim, claimed, err
}

func (s *renewalTestStore) RenewClaim(ctx context.Context, claim Claim) (Claim, error) {
	call := s.calls.Add(1)
	if s.failAt > 0 && call >= s.failAt {
		return Claim{}, s.failErr
	}
	renewed, err := s.Store.RenewClaim(ctx, claim)
	if err != nil {
		return Claim{}, err
	}
	renewed.ExpiresAt = time.Now().Add(s.ttl)
	return renewed, nil
}

type cancelReportingVerifier struct {
	entered chan struct{}
	err     error
}

func (v *cancelReportingVerifier) Verify(ctx context.Context, _ TargetedVerifyRequest) (TargetedVerifyResult, error) {
	close(v.entered)
	<-ctx.Done()
	return TargetedVerifyResult{}, v.err
}

func waitClosed(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

func waitRenewCalls(t *testing.T, store *renewalTestStore, minimum int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for store.calls.Load() < minimum {
		if time.Now().After(deadline) {
			t.Fatalf("RenewClaim calls = %d, want at least %d", store.calls.Load(), minimum)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitReconcile(t *testing.T, done <-chan reconcileOutcome) reconcileOutcome {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile did not finish")
		return reconcileOutcome{}
	}
}
