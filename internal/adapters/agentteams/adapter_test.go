package agentteams_test

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams/fake"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestDispatchIsStableAndCreatesOnlyOneEffectiveTask(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://inv-a/1", prepared("inv-a", auth.RoleExecutor, ""))

	const workers = 100
	var wg sync.WaitGroup
	results := make(chan adapter.AgentTeamsExecutionRef, workers)
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.Dispatch(context.Background(), "execution://inv-a/1")
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Dispatch failed: %v", err)
	}
	var first adapter.AgentTeamsExecutionRef
	for result := range results {
		if first.AgentTeamsTaskID == "" {
			first = result
		}
		if result != first {
			t.Fatalf("execution ref = %#v, want stable %#v", result, first)
		}
	}
	if host.TaskCount() != 1 {
		t.Fatalf("AgentTeams task count = %d, want 1", host.TaskCount())
	}
	if preparations := host.Preparations(); len(preparations) != 1 {
		t.Fatalf("PrepareHost calls = %d, want 1", len(preparations))
	}
	before := host.DelegateCalls()
	replayed, err := service.Dispatch(context.Background(), "execution://inv-a/1")
	if err != nil || replayed != first {
		t.Fatalf("Dispatch replay = %#v, %v", replayed, err)
	}
	if host.DelegateCalls() != before {
		t.Fatal("completed Dispatch replay called taskflow again")
	}
}

func TestDispatchRecoversWhenDelegateResponseWasLost(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://ambiguous/1", prepared("inv-ambiguous", auth.RoleExecutor, ""))
	host.SetDelegateResponseError(errors.New("response lost"))
	if _, err := service.Dispatch(context.Background(), "execution://ambiguous/1"); err == nil {
		t.Fatal("ambiguous Dispatch unexpectedly succeeded")
	}
	if host.TaskCount() != 1 {
		t.Fatalf("ambiguous call created %d tasks, want 1", host.TaskCount())
	}
	if host.ActiveExecutions("worker-a") != 1 {
		t.Fatalf("active executions after ambiguous Dispatch = %d, want 1", host.ActiveExecutions("worker-a"))
	}
	host.SetDelegateResponseError(nil)
	result, err := service.Dispatch(context.Background(), "execution://ambiguous/1")
	if err != nil {
		t.Fatalf("Dispatch recovery failed: %v", err)
	}
	if result.AgentTeamsTaskID == "" || host.TaskCount() != 1 {
		t.Fatalf("recovered result/task count = %#v/%d", result, host.TaskCount())
	}
	if host.ActiveExecutions("worker-a") != 1 {
		t.Fatalf("active executions after ambiguous recovery = %d, want 1", host.ActiveExecutions("worker-a"))
	}
}

func TestDifferentInvocationDispatchDoesNotOversellCapacityOneHost(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host := fake.NewClient()
	host.AddHost(adapter.HostStatus{
		Ref:           "worker-one",
		Kind:          adapter.HostWorker,
		Phase:         "Running",
		LastHeartbeat: now,
		Capacity:      1,
		Capabilities:  []string{"shell"},
	})
	source := fake.NewInvocationSource()
	service, err := adapter.NewAdapter(host, source, host, adapter.NewMemoryExecutionStore(), func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 100
	var wg sync.WaitGroup
	results := make(chan adapter.AgentTeamsExecutionRef, workers)
	errs := make(chan error, workers)
	for i := range workers {
		ref := "execution://capacity/" + strconv.Itoa(i)
		source.Put(ref, prepared("inv-capacity-"+strconv.Itoa(i), auth.RoleExecutor, ""))
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.Dispatch(context.Background(), ref)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	successes := 0
	var execution adapter.AgentTeamsExecutionRef
	for result := range results {
		successes++
		execution = result
	}
	unavailable := 0
	for err := range errs {
		if !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
			t.Fatalf("Dispatch error = %v, want executor_unavailable", err)
		}
		unavailable++
	}
	if successes != 1 || unavailable != workers-1 {
		t.Fatalf("Dispatch outcomes successes=%d unavailable=%d, want 1/%d", successes, unavailable, workers-1)
	}
	if host.TaskCount() != 1 || host.ActiveExecutions("worker-one") != 1 {
		t.Fatalf("task/active counts = %d/%d, want 1/1", host.TaskCount(), host.ActiveExecutions("worker-one"))
	}
	if err := host.SetResult(execution.AgentTeamsTaskID, adapter.TaskCheck{ResultStatus: "SUCCESS", Effective: true}, []byte("done"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Collect(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if host.ActiveExecutions("worker-one") != 0 {
		t.Fatalf("active executions after terminal Collect = %d, want 0", host.ActiveExecutions("worker-one"))
	}
}

func TestDispatchSelectsHealthyMatchingCapacityAndRoleHost(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host := fake.NewClient()
	host.AddHost(adapter.HostStatus{Ref: "manager-a", Kind: adapter.HostManager, Phase: "Running", LastHeartbeat: now, Capacity: 2, Capabilities: []string{"mcp"}})
	host.AddHost(adapter.HostStatus{Ref: "worker-full", Kind: adapter.HostWorker, Phase: "Running", LastHeartbeat: now, Capacity: 1, ActiveExecutions: 1, Capabilities: []string{"shell"}})
	host.AddHost(adapter.HostStatus{Ref: "worker-stale", Kind: adapter.HostWorker, Phase: "Running", LastHeartbeat: now.Add(-time.Minute), Capacity: 2, Capabilities: []string{"shell"}})
	host.AddHost(adapter.HostStatus{Ref: "worker-good", Kind: adapter.HostWorker, Phase: "Running", LastHeartbeat: now, Capacity: 2, Capabilities: []string{"shell"}})
	source := fake.NewInvocationSource()
	executor := prepared("inv-executor", auth.RoleExecutor, "")
	executor.RequiredCapabilities = []string{"shell"}
	source.Put("execution://executor/1", executor)
	contextInvocation := prepared("inv-context", auth.RoleContext, "retrieve")
	contextInvocation.RequiredCapabilities = []string{"mcp"}
	source.Put("execution://context/1", contextInvocation)
	service, err := adapter.NewAdapter(host, source, host, adapter.NewMemoryExecutionStore(), func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	workerExecution, err := service.Dispatch(context.Background(), "execution://executor/1")
	if err != nil || workerExecution.HostRef != "worker-good" {
		t.Fatalf("worker Dispatch = %#v, %v", workerExecution, err)
	}
	managerExecution, err := service.Dispatch(context.Background(), "execution://context/1")
	if err != nil || managerExecution.HostRef != "manager-a" {
		t.Fatalf("manager Dispatch = %#v, %v", managerExecution, err)
	}

	emptyHost := fake.NewClient()
	emptyService, err := adapter.NewAdapter(emptyHost, source, emptyHost, adapter.NewMemoryExecutionStore(), func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emptyService.Dispatch(context.Background(), "execution://executor/1"); !kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
		t.Fatalf("Dispatch without capacity = %v, want executor_unavailable", err)
	}
}

func TestTerminateSupportsThreeModesAndFencesBeforeCancel(t *testing.T) {
	for _, mode := range []string{"release_wait", "recoverable_stop", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
			host, source, service := newHarness(t, now)
			ref := "execution://terminate/" + mode
			source.Put(ref, prepared("inv-"+mode, auth.RoleExecutor, ""))
			execution, err := service.Dispatch(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Terminate(context.Background(), execution, mode); err != nil {
				t.Fatal(err)
			}
			if host.ActiveExecutions("worker-a") != 0 {
				t.Fatalf("active executions after Terminate = %d, want 0", host.ActiveExecutions("worker-a"))
			}
			if err := service.Terminate(context.Background(), execution, mode); err != nil {
				t.Fatalf("idempotent Terminate failed: %v", err)
			}
			calls := host.Calls()
			revoke := indexWithPrefix(calls, "revoke:")
			cancel := indexWithPrefix(calls, "cancel:")
			if revoke < 0 || cancel < 0 || revoke >= cancel {
				t.Fatalf("fencing/cancel order = %v", calls)
			}
			if err := service.Terminate(context.Background(), execution, "cancel"); mode != "cancel" && !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
				t.Fatalf("different termination mode = %v, want idempotency_conflict", err)
			}
			if mode == "release_wait" {
				return
			}
			if _, err := service.Dispatch(context.Background(), ref); !kernel.IsCode(err, kernel.CodeStaleCommand) {
				t.Fatalf("redispatch stopped/cancelled execution = %v, want stale_command", err)
			}
		})
	}
}

func TestReleaseWaitRedispatchCreatesNewAttemptForSameInvocation(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	ref := "execution://wait/1"
	source.Put(ref, prepared("inv-wait", auth.RoleExecutor, ""))

	first, err := service.Dispatch(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Terminate(context.Background(), first, "release_wait"); err != nil {
		t.Fatal(err)
	}
	if host.ActiveExecutions("worker-a") != 0 {
		t.Fatalf("active executions after release_wait = %d, want 0", host.ActiveExecutions("worker-a"))
	}

	const workers = 50
	var wg sync.WaitGroup
	results := make(chan adapter.AgentTeamsExecutionRef, workers)
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := service.Dispatch(context.Background(), ref)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("release_wait redispatch failed: %v", err)
	}
	var second adapter.AgentTeamsExecutionRef
	for result := range results {
		if second.AgentTeamsTaskID == "" {
			second = result
		}
		if result != second {
			t.Fatalf("redispatch result = %#v, want stable second attempt %#v", result, second)
		}
	}
	if second.InvocationID != first.InvocationID {
		t.Fatalf("logical invocation changed: first=%#v second=%#v", first, second)
	}
	if second.AgentTeamsTaskID == first.AgentTeamsTaskID {
		t.Fatalf("release_wait reused AgentTeams task ID %q", second.AgentTeamsTaskID)
	}
	if host.TaskCount() != 2 {
		t.Fatalf("AgentTeams task count after redispatch = %d, want 2", host.TaskCount())
	}
	if preparations := host.Preparations(); len(preparations) != 2 {
		t.Fatalf("PrepareHost calls after redispatch = %d, want 2", len(preparations))
	}
	if host.ActiveExecutions("worker-a") != 1 {
		t.Fatalf("active executions after redispatch = %d, want 1", host.ActiveExecutions("worker-a"))
	}
	if err := service.Terminate(context.Background(), first, "release_wait"); err != nil {
		t.Fatalf("old release_wait termination should remain idempotent: %v", err)
	}
}

func TestDelayedOldAttemptMarkDispatchedDoesNotPolluteLatestAttempt(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host := fake.NewClient()
	host.AddHost(adapter.HostStatus{
		Ref:           "worker-a",
		Kind:          adapter.HostWorker,
		Phase:         "Running",
		LastHeartbeat: now,
		Capacity:      8,
		Capabilities:  []string{"shell"},
	})
	source := fake.NewInvocationSource()
	store := adapter.NewMemoryExecutionStore()
	ref := "execution://delayed-mark/1"
	source.Put(ref, prepared("inv-delayed-mark", auth.RoleExecutor, ""))

	service, err := adapter.NewAdapter(host, source, host, store, func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Dispatch(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Terminate(context.Background(), first, "release_wait"); err != nil {
		t.Fatal(err)
	}

	secondClient := newBlockingDelegateClient(host)
	secondService, err := adapter.NewAdapter(secondClient, source, host, store, func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := secondService.Dispatch(context.Background(), ref)
		secondDone <- err
	}()
	secondRequest := secondClient.wait(t)
	second := adapter.AgentTeamsExecutionRef{
		InvocationID:     first.InvocationID,
		AgentTeamsTaskID: secondRequest.TaskID,
		HostRef:          secondRequest.HostRef,
	}
	if err := service.Terminate(context.Background(), second, "release_wait"); err != nil {
		t.Fatal(err)
	}

	thirdClient := newBlockingDelegateClient(host)
	thirdService, err := adapter.NewAdapter(thirdClient, source, host, store, func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	thirdDone := make(chan error, 1)
	go func() {
		_, err := thirdService.Dispatch(context.Background(), ref)
		thirdDone <- err
	}()
	thirdRequest := thirdClient.wait(t)

	secondClient.release()
	if err := <-secondDone; !kernel.IsCode(err, kernel.CodeStaleCommand) {
		t.Fatalf("delayed second attempt Dispatch error = %v, want stale_command", err)
	}

	prepareErr := errors.New("latest attempt is still reserved")
	poisonService, err := adapter.NewAdapter(
		poisonPrepareClient{Client: host, err: prepareErr},
		source,
		host,
		store,
		func() time.Time { return now },
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := poisonService.Dispatch(context.Background(), ref); !errors.Is(err, prepareErr) {
		t.Fatalf("Dispatch after delayed old mark = %v, want reserved latest attempt to call PrepareHost", err)
	}

	thirdClient.release()
	if err := <-thirdDone; err != nil {
		t.Fatalf("third attempt Dispatch failed: %v", err)
	}
	if thirdRequest.TaskID == secondRequest.TaskID {
		t.Fatalf("third attempt reused second task ID %q", thirdRequest.TaskID)
	}
}

func TestTerminateForceStopsHostWhenRevocationCannotBeProved(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://fence-failure/1", prepared("inv-fence", auth.RoleExecutor, ""))
	execution, err := service.Dispatch(context.Background(), "execution://fence-failure/1")
	if err != nil {
		t.Fatal(err)
	}
	host.FailRevoke = errors.New("revoke failed")
	if err := service.Terminate(context.Background(), execution, "cancel"); err == nil {
		t.Fatal("Terminate succeeded despite failed revocation")
	}
	calls := host.Calls()
	if indexWithPrefix(calls, "force-stop:") < 0 || indexWithPrefix(calls, "cancel:") >= 0 {
		t.Fatalf("failed fencing calls = %v", calls)
	}
	if host.ActiveExecutions("worker-a") != 0 {
		t.Fatalf("active executions after force stop = %d, want 0", host.ActiveExecutions("worker-a"))
	}
}

func TestCollectKeepsSuccessAndSpoofedBindingUntrusted(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://collect/1", prepared("inv-collect", auth.RoleExecutor, ""))
	execution, err := service.Dispatch(context.Background(), "execution://collect/1")
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"phase_output":{"binding":{"task_id":"spoofed"}}}`)
	if err := host.SetResult(execution.AgentTeamsTaskID, adapter.TaskCheck{
		ResultStatus: "SUCCESS",
		Summary:      "provider says success",
		Deliverables: []string{"shared/tasks/" + execution.AgentTeamsTaskID + "/deliverables/result.json"},
		Effective:    true,
	}, document, now); err != nil {
		t.Fatal(err)
	}
	result, err := service.Collect(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultStatus != "SUCCESS" || !result.Effective || string(result.ResultDocument) != string(document) {
		t.Fatalf("untrusted result was reinterpreted: %#v", result)
	}
	if host.ActiveExecutions("worker-a") != 0 {
		t.Fatalf("active executions after terminal Collect = %d, want 0", host.ActiveExecutions("worker-a"))
	}
	typ := reflect.TypeOf(result)
	for _, forbidden := range []string{"BindingRef", "Generation", "LeaseRef", "Verdict"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Fatalf("UntrustedExecutionResult exposes trusted field %s", forbidden)
		}
	}
}

func TestObserveUsesCursorAndStableKeysForReplayAndDuplicates(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://observe/1", prepared("inv-observe", auth.RoleExecutor, ""))
	execution, err := service.Dispatch(context.Background(), "execution://observe/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Ack(execution.AgentTeamsTaskID, now); err != nil {
		t.Fatal(err)
	}
	host.Emit(adapter.RawObservation{
		ProviderEventID: "ack:" + execution.AgentTeamsTaskID,
		TaskID:          execution.AgentTeamsTaskID,
		HostRef:         execution.HostRef,
		Kind:            "execution_acked",
		ObservedAt:      now,
	})
	observations, err := service.Observe(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].Cursor != "3" || observations[0].InvocationID != execution.InvocationID {
		t.Fatalf("deduplicated observations = %#v", observations)
	}
	replayed, err := service.Observe(context.Background(), "1")
	if err != nil || !reflect.DeepEqual(replayed, observations) {
		t.Fatalf("cursor replay = %#v, %v; want %#v", replayed, err, observations)
	}
	empty, err := service.Observe(context.Background(), "3")
	if err != nil || len(empty) != 0 {
		t.Fatalf("cursor tail = %#v, %v", empty, err)
	}
	if err := host.SetHostPhase("worker-a", "Failed", now); err != nil {
		t.Fatal(err)
	}
	hostEvents, err := service.Observe(context.Background(), "3")
	if err != nil || len(hostEvents) != 1 || hostEvents[0].InvocationID != "" {
		t.Fatalf("host observation = %#v, %v", hostEvents, err)
	}

	host.Emit(adapter.RawObservation{ProviderEventID: "collision", HostRef: "worker-a", Kind: "heartbeat", Payload: map[string]string{"state": "up"}, ObservedAt: now})
	host.Emit(adapter.RawObservation{ProviderEventID: "collision", HostRef: "worker-a", Kind: "heartbeat", Payload: map[string]string{"state": "down"}, ObservedAt: now})
	if _, err := service.Observe(context.Background(), "4"); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("observation key conflict = %v, want idempotency_conflict", err)
	}
}

func TestObserveAdvancesCursorForOnlyForeignTaskEvent(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, _, service := newHarness(t, now)
	host.Emit(adapter.RawObservation{
		ProviderEventID: "foreign-only",
		TaskID:          "foreign-task",
		HostRef:         "worker-a",
		Kind:            "execution_acked",
		ObservedAt:      now,
	})

	observations, err := service.Observe(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].Kind != "cursor_advance" || observations[0].Cursor != "1" || observations[0].AgentTeamsTaskID != "" || observations[0].InvocationID != "" {
		t.Fatalf("foreign cursor advance = %#v", observations)
	}
	replayed, err := service.Observe(context.Background(), observations[0].Cursor)
	if err != nil || len(replayed) != 0 {
		t.Fatalf("foreign cursor replay = %#v, %v; want empty", replayed, err)
	}
}

func TestObserveMixedEventsAdvancesPastTrailingForeignTaskEvent(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://observe-mixed/1", prepared("inv-observe-mixed", auth.RoleExecutor, ""))
	execution, err := service.Dispatch(context.Background(), "execution://observe-mixed/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Ack(execution.AgentTeamsTaskID, now); err != nil {
		t.Fatal(err)
	}
	host.Emit(adapter.RawObservation{
		ProviderEventID: "foreign-after-known",
		TaskID:          "foreign-task",
		HostRef:         "worker-a",
		Kind:            "execution_submitted",
		ObservedAt:      now.Add(time.Second),
	})

	observations, err := service.Observe(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 || observations[0].Kind != "execution_acked" || observations[1].Kind != "cursor_advance" || observations[1].Cursor != "3" {
		t.Fatalf("mixed observations = %#v", observations)
	}
	replayed, err := service.Observe(context.Background(), observations[1].Cursor)
	if err != nil || len(replayed) != 0 {
		t.Fatalf("mixed cursor replay = %#v, %v; want empty", replayed, err)
	}
}

func TestObserveRestartReplayKeepsForeignCursorAdvanceStable(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	host, source, service := newHarness(t, now)
	source.Put("execution://observe-restart/1", prepared("inv-observe-restart", auth.RoleExecutor, ""))
	execution, err := service.Dispatch(context.Background(), "execution://observe-restart/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Ack(execution.AgentTeamsTaskID, now); err != nil {
		t.Fatal(err)
	}
	host.Emit(adapter.RawObservation{
		ProviderEventID: "foreign-restart",
		TaskID:          "foreign-task",
		HostRef:         "worker-a",
		Kind:            "execution_failed",
		ObservedAt:      now.Add(time.Second),
	})
	first, err := service.Observe(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}

	restarted, err := adapter.NewAdapter(host, source, host, adapter.NewMemoryExecutionStore(), func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	source.Put("execution://observe-restart/1", prepared("inv-observe-restart", auth.RoleExecutor, ""))
	_, err = restarted.Dispatch(context.Background(), "execution://observe-restart/1")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Observe(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("restart replay = %#v, want %#v", replayed, first)
	}
}

func TestAdapterSurfaceHasNoGraphOrContextDependency(t *testing.T) {
	typ := reflect.TypeOf((*adapter.AgentTeamsHostAdapter)(nil)).Elem()
	if typ.NumMethod() != 4 {
		t.Fatalf("AgentTeamsHostAdapter has %d methods, want Dispatch/Terminate/Collect/Observe", typ.NumMethod())
	}
	packages, err := parser.ParseDir(token.NewFileSet(), ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, imported := range file.Imports {
				path := strings.Trim(imported.Path.Value, `"`)
				if strings.HasSuffix(path, "/coordination") || strings.HasSuffix(path, "/contextgraph") {
					t.Fatalf("AgentTeams adapter imports forbidden domain repository %s", path)
				}
			}
		}
	}

	baseline, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "5002_agentteams_execution.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	baselineText := string(baseline)
	for _, required := range []string{"invocation_ref TEXT PRIMARY KEY", "agentteams_task_id TEXT NOT NULL UNIQUE", "release_wait", "recoverable_stop", "cancel"} {
		if !strings.Contains(baselineText, required) {
			t.Errorf("5002 execution baseline migration is missing %q", required)
		}
	}
	for _, forbidden := range []string{"attempt INTEGER", "PRIMARY KEY (invocation_ref, attempt)", "agentteams_execution_refs_active_invocation_idx"} {
		if strings.Contains(baselineText, forbidden) {
			t.Errorf("5002 execution baseline migration must not contain %q", forbidden)
		}
	}

	attemptsUp, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "5003_agentteams_execution_attempts.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	attemptsUpText := string(attemptsUp)
	for _, required := range []string{"ADD COLUMN IF NOT EXISTS attempt INTEGER", "SET attempt = 1", "ALTER COLUMN attempt SET NOT NULL", "agentteams_execution_refs_attempt_check CHECK (attempt > 0)", "PRIMARY KEY (invocation_ref, attempt)", "agentteams_execution_refs_active_invocation_idx", "WHERE state IN ('reserved', 'dispatched')", "ON agentteams_execution_refs (invocation_id, attempt, created_at)"} {
		if !strings.Contains(attemptsUpText, required) {
			t.Errorf("5003 execution attempts migration is missing %q", required)
		}
	}

	attemptsDown, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "5003_agentteams_execution_attempts.down.sql"))
	if err != nil {
		t.Fatal(err)
	}
	attemptsDownText := string(attemptsDown)
	for _, required := range []string{"cannot downgrade agentteams_execution_refs with non-baseline attempts", "HAVING count(*) > 1", "DROP COLUMN IF EXISTS attempt", "PRIMARY KEY (invocation_ref)", "ON agentteams_execution_refs (invocation_id, created_at)"} {
		if !strings.Contains(attemptsDownText, required) {
			t.Errorf("5003 execution attempts down migration is missing %q", required)
		}
	}
}

func newHarness(t *testing.T, now time.Time) (*fake.Client, *fake.InvocationSource, *adapter.Adapter) {
	t.Helper()
	host := fake.NewClient()
	host.AddHost(adapter.HostStatus{
		Ref:           "worker-a",
		Kind:          adapter.HostWorker,
		Phase:         "Running",
		LastHeartbeat: now,
		Capacity:      8,
		Capabilities:  []string{"shell"},
	})
	source := fake.NewInvocationSource()
	service, err := adapter.NewAdapter(host, source, host, adapter.NewMemoryExecutionStore(), func() time.Time { return now }, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return host, source, service
}

func prepared(invocationID string, role auth.Role, operation string) adapter.PreparedInvocation {
	return adapter.PreparedInvocation{
		InvocationID:     kernel.InvocationID(invocationID),
		ProjectID:        "project-a",
		Role:             role,
		Operation:        operation,
		RoomID:           "!room:example.test",
		Spec:             "bounded execution spec",
		RuntimeConfigRef: "runtime-config://" + invocationID,
		EnvelopeRef:      "envelope://" + invocationID,
	}
}

func indexWithPrefix(values []string, prefix string) int {
	for index, value := range values {
		if strings.HasPrefix(value, prefix) {
			return index
		}
	}
	return -1
}

type blockingDelegateClient struct {
	adapter.Client
	started chan adapter.DelegateTaskRequest
	blocked chan struct{}
}

func newBlockingDelegateClient(client adapter.Client) *blockingDelegateClient {
	return &blockingDelegateClient{
		Client:  client,
		started: make(chan adapter.DelegateTaskRequest, 1),
		blocked: make(chan struct{}),
	}
}

func (c *blockingDelegateClient) DelegateTask(ctx context.Context, request adapter.DelegateTaskRequest) (adapter.TaskSnapshot, error) {
	task, err := c.Client.DelegateTask(ctx, request)
	if err != nil {
		return adapter.TaskSnapshot{}, err
	}
	c.started <- request
	select {
	case <-c.blocked:
		return task, nil
	case <-ctx.Done():
		return adapter.TaskSnapshot{}, ctx.Err()
	}
}

func (c *blockingDelegateClient) wait(t *testing.T) adapter.DelegateTaskRequest {
	t.Helper()
	select {
	case request := <-c.started:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DelegateTask")
		return adapter.DelegateTaskRequest{}
	}
}

func (c *blockingDelegateClient) release() {
	close(c.blocked)
}

type poisonPrepareClient struct {
	adapter.Client
	err error
}

func (c poisonPrepareClient) PrepareHost(context.Context, adapter.HostPreparation) error {
	return c.err
}
