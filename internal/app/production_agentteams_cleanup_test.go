package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
)

func TestProductionExecutionAbandonedRecognizesIdleWorkerRestart(t *testing.T) {
	now := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)
	started := now.Add(-time.Minute)
	gap := 10 * time.Second
	tests := []struct {
		name     string
		activity agentteams.HostActivity
		started  time.Time
		want     bool
	}{
		{name: "restart reset timestamps", activity: agentteams.HostActivity{Status: "idle"}, started: started, want: true},
		{name: "old idle run without finish", activity: agentteams.HostActivity{Status: "idle", LastRunAt: now.Add(-30 * time.Second)}, started: started, want: true},
		{name: "recent idle run", activity: agentteams.HostActivity{Status: "idle", LastRunAt: now.Add(-5 * time.Second)}, started: started, want: false},
		{name: "provider still running", activity: agentteams.HostActivity{Status: "running"}, started: started, want: false},
		{name: "running task", activity: agentteams.HostActivity{Status: "idle", RunningTaskCount: 1}, started: started, want: false},
		{name: "finished execution", activity: agentteams.HostActivity{Status: "idle", LastFinishAt: now.Add(-30 * time.Second)}, started: started, want: true},
		{name: "finish predates execution", activity: agentteams.HostActivity{Status: "idle", LastFinishAt: started.Add(-time.Second)}, started: started, want: false},
		{name: "execution inside gap", activity: agentteams.HostActivity{Status: "idle"}, started: now.Add(-5 * time.Second), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := productionExecutionAbandoned(test.activity, test.started, now, gap); got != test.want {
				t.Fatalf("productionExecutionAbandoned() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestProductionPhaseExecutionAbandonedAllowsQwenPawColdStart(t *testing.T) {
	now := time.Date(2026, 8, 12, 4, 0, 0, 0, time.UTC)
	if productionPhaseExecutionAbandoned(agentteams.HostActivity{Status: "idle"}, now.Add(-30*time.Second), now) {
		t.Fatal("30-second idle cold start was treated as abandoned")
	}
	if productionPhaseExecutionAbandoned(agentteams.HostActivity{Status: "idle"}, now.Add(-time.Minute), now) {
		t.Fatal("one-minute idle verifier gap was treated as abandoned")
	}
	if !productionPhaseExecutionAbandoned(agentteams.HostActivity{Status: "idle"}, now.Add(-productionPhaseExecutionQuiescenceGap-time.Second), now) {
		t.Fatal("restarted idle carrier beyond the quiescence grace was not treated as abandoned")
	}
	if !productionPhaseExecutionAbandoned(agentteams.HostActivity{Status: "idle", LastFinishAt: now.Add(-productionPhaseExecutionQuiescenceGap - time.Second)}, now.Add(-10*time.Minute), now) {
		t.Fatal("finished idle execution beyond the cold-start grace was not treated as abandoned")
	}
}

func TestProductionPhaseMonitorRequiresObservedActivityBeforeIdleFailure(t *testing.T) {
	now := time.Date(2026, 8, 12, 4, 0, 0, 0, time.UTC)
	target := productionPhaseExecutionTarget{
		execution: agentteams.AgentTeamsExecutionRef{AgentTeamsTaskID: "provider-task-a"},
		startedAt: now.Add(-30 * time.Second),
	}
	monitor := &productionPhaseExecutionMonitor{
		activeAt: make(map[string]time.Time), idleSince: make(map[string]time.Time),
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now) {
		t.Fatal("cold idle execution was abandoned before the quiescence grace")
	}
	target.startedAt = now.Add(-productionPhaseExecutionColdQuiescenceGap - time.Second)
	if !monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now) {
		t.Fatal("cold idle execution beyond the recovery grace was not abandoned")
	}
	target.startedAt = now.Add(-30 * time.Second)
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "running", RunningTaskCount: 1}, now) {
		t.Fatal("running execution was abandoned")
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(10*time.Second)) {
		t.Fatal("recently idle execution was abandoned before quiescence gap")
	}
	if monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(time.Minute)) {
		t.Fatal("one-minute pause between verifier turns was treated as abandoned")
	}
	if !monitor.executionAbandoned(target, agentteams.HostActivity{Status: "idle"}, now.Add(productionPhaseExecutionQuiescenceGap+11*time.Second)) {
		t.Fatal("continuously idle execution with observed prior activity was not abandoned")
	}

	historical := productionPhaseExecutionTarget{
		execution: agentteams.AgentTeamsExecutionRef{AgentTeamsTaskID: "provider-task-history"},
		startedAt: now.Add(-10 * time.Minute),
	}
	activity := agentteams.HostActivity{
		Status: "idle", LastRunAt: now.Add(-productionPhaseExecutionQuiescenceGap), LastFinishAt: now.Add(-productionPhaseExecutionQuiescenceGap - time.Second),
	}
	if !monitor.executionAbandoned(historical, activity, now) {
		t.Fatal("historical run timestamp kept an already quiescent execution alive")
	}
}

func TestProductionTaskManagerCleanupUsesActivityBeforeTerminalProbe(t *testing.T) {
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	terminator := &taskManagerCleanupProbeTerminator{
		activity: agentteams.HostActivity{
			Status:       "idle",
			LastFinishAt: now.Add(-30 * time.Second),
		},
		terminalErr: context.DeadlineExceeded,
	}
	cleanup := &productionTaskManagerExecutionCleanup{
		terminator: terminator,
		now:        func() time.Time { return now },
	}
	abandoned, err := cleanup.activeDispatchedExecutionAbandoned(
		context.Background(),
		productionTaskManagerExecutionCleanupTarget{
			execution: agentteams.AgentTeamsExecutionRef{InvocationID: "inv-idle", AgentTeamsTaskID: "task-idle"},
			startedAt: now.Add(-time.Minute),
		},
	)
	if err != nil {
		t.Fatalf("activeDispatchedExecutionAbandoned() error = %v", err)
	}
	if !abandoned {
		t.Fatal("idle completed activity did not mark running Task Manager execution abandoned")
	}
	if terminator.activityCalls != 1 {
		t.Fatalf("activity calls = %d, want 1", terminator.activityCalls)
	}
	if terminator.terminalCalls != 0 {
		t.Fatalf("terminal calls = %d, want 0 when activity already proves abandonment", terminator.terminalCalls)
	}
}

func TestProductionTaskManagerCleanupBoundsTerminalProbe(t *testing.T) {
	now := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	terminator := &taskManagerCleanupProbeTerminator{
		activity:      agentteams.HostActivity{Status: "running", RunningTaskCount: 1},
		blockTerminal: true,
	}
	cleanup := &productionTaskManagerExecutionCleanup{
		terminator: terminator,
		now:        func() time.Time { return now },
	}
	ctx, cancel := context.WithTimeout(context.Background(), productionTaskManagerCleanupTimeout)
	defer cancel()
	started := time.Now()
	abandoned, err := cleanup.activeDispatchedExecutionAbandoned(
		ctx,
		productionTaskManagerExecutionCleanupTarget{
			execution: agentteams.AgentTeamsExecutionRef{InvocationID: "inv-running", AgentTeamsTaskID: "task-running"},
			startedAt: now.Add(-time.Minute),
		},
	)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("activeDispatchedExecutionAbandoned() error = %v, want context deadline exceeded", err)
	}
	if abandoned {
		t.Fatal("running activity was marked abandoned after terminal probe timeout")
	}
	if terminator.activityCalls != 1 || terminator.terminalCalls != 1 {
		t.Fatalf("probe calls activity=%d terminal=%d, want 1/1", terminator.activityCalls, terminator.terminalCalls)
	}
	if elapsed >= productionTaskManagerCleanupTimeout/2 {
		t.Fatalf("terminal probe elapsed %s, want less than half cleanup budget %s", elapsed, productionTaskManagerCleanupTimeout)
	}
}

type taskManagerCleanupProbeTerminator struct {
	activity      agentteams.HostActivity
	activityErr   error
	terminal      bool
	terminalErr   error
	blockTerminal bool
	activityCalls int
	terminalCalls int
}

func (t *taskManagerCleanupProbeTerminator) FinalizeExecution(context.Context, agentteams.AgentTeamsExecutionRef, string) error {
	return nil
}

func (t *taskManagerCleanupProbeTerminator) Terminate(context.Context, agentteams.AgentTeamsExecutionRef, string) error {
	return nil
}

func (t *taskManagerCleanupProbeTerminator) FenceExecution(context.Context, agentteams.AgentTeamsExecutionRef) error {
	return nil
}

func (t *taskManagerCleanupProbeTerminator) ExecutionTerminal(ctx context.Context, _ agentteams.AgentTeamsExecutionRef) (bool, error) {
	t.terminalCalls++
	if t.blockTerminal {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return t.terminal, t.terminalErr
}

func (t *taskManagerCleanupProbeTerminator) ExecutionActivity(context.Context, agentteams.AgentTeamsExecutionRef) (agentteams.HostActivity, error) {
	t.activityCalls++
	return t.activity, t.activityErr
}

var _ productionAgentTeamsTerminator = (*taskManagerCleanupProbeTerminator)(nil)
var _ productionAgentTeamsActivityObserver = (*taskManagerCleanupProbeTerminator)(nil)
