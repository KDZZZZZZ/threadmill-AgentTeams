package agentteams

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	phasemcp "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mcp/phase"
)

// TeamHarnessExecutionHost implements ExecutionHost by mapping a fresh
// Threadmill execution onto TeamHarness taskflow's delegate/check lifecycle.
// The TaskflowClient transport is deliberately separate: M1 does not select a
// permanent Go-to-Python/MCP transport or invoke a shell command.
type TeamHarnessExecutionHost struct {
	taskflow TaskflowClient
	routes   TaskflowRouteResolver
	workers  WorkerSelector
	taskIDs  InvocationTaskMap
	ids      TaskIDGenerator
	mcp      MCPInjector
	evidence ExecutionEvidenceIngestor

	// PollInterval bounds check_task polling. It is an observation interval,
	// not a scheduler; every wait is interruptible through ctx.
	PollInterval time.Duration
}

var _ ExecutionHost = (*TeamHarnessExecutionHost)(nil)

// NewTeamHarnessExecutionHost creates the fresh-invocation host. The caller
// supplies integration-specific routing, worker selection, and taskflow
// transport because TeamHarness has no Threadmill-aware scheduler.
func NewTeamHarnessExecutionHost(taskflow TaskflowClient, routes TaskflowRouteResolver, workers WorkerSelector, taskIDs InvocationTaskMap, ids TaskIDGenerator, mcp MCPInjector, evidence ...ExecutionEvidenceIngestor) (*TeamHarnessExecutionHost, error) {
	if taskflow == nil {
		return nil, contractError("taskflow client is required")
	}
	if routes == nil {
		return nil, contractError("taskflow route resolver is required")
	}
	if workers == nil {
		return nil, contractError("worker selector is required")
	}
	if taskIDs == nil {
		return nil, contractError("invocation task map is required")
	}
	if ids == nil {
		return nil, contractError("task id generator is required")
	}
	if mcp == nil {
		return nil, contractError("mcp injector is required")
	}
	host := &TeamHarnessExecutionHost{
		taskflow:     taskflow,
		routes:       routes,
		workers:      workers,
		taskIDs:      taskIDs,
		ids:          ids,
		mcp:          mcp,
		PollInterval: time.Second,
	}
	if len(evidence) > 0 {
		host.evidence = evidence[0]
	}
	return host, nil
}

// Execute supports one fresh delegate -> acknowledge -> submit observation.
// It neither calls submit_task nor turns a submitted TeamHarness task into a
// Threadmill PhaseOutput.
func (h *TeamHarnessExecutionHost) Execute(ctx context.Context, request HostExecutionRequest) (HostExecutionOutcome, error) {
	if err := ctx.Err(); err != nil {
		return HostExecutionOutcome{Status: HostExecutionCancelled, Summary: err.Error()}, nil
	}
	route, err := h.routes.ResolveTaskflowRoute(ctx, request)
	if err != nil {
		return HostExecutionOutcome{}, err
	}
	worker, err := h.workers.SelectWorker(ctx, request)
	if err != nil {
		return HostExecutionOutcome{}, err
	}
	taskID, err := h.ids.NewTaskID(ctx, request)
	if err != nil {
		return HostExecutionOutcome{}, err
	}
	if err := h.mcp.InjectPhaseMCP(ctx, taskID, request.Envelope.MCPBinding); err != nil {
		return HostExecutionOutcome{}, err
	}
	payload := BuildDelegateTaskRequest(request, route, worker, taskID)
	if err := h.taskflow.DelegateTask(ctx, payload); err != nil {
		return HostExecutionOutcome{}, err
	}
	if err := h.taskIDs.Record(ctx, request.InvocationID, taskID); err != nil {
		return HostExecutionOutcome{}, err
	}
	return h.observe(ctx, taskID, request)
}

// MCPInjector configures the host-side Threadmill MCP client before taskflow
// delegation. QwenPaw's /api/mcp and policy API are the future implementation
// target, but M2 keeps the Go-to-Python transport explicit and replaceable.
// The opaque token is not written to TeamHarness spec.md or task metadata.
type MCPInjector interface {
	InjectPhaseMCP(ctx context.Context, executionID string, binding phasemcp.ExecutionBinding) error
}

func (h *TeamHarnessExecutionHost) observe(ctx context.Context, taskID string, request HostExecutionRequest) (HostExecutionOutcome, error) {
	interval := h.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	acknowledged := false
	for {
		if err := ctx.Err(); err != nil {
			return HostExecutionOutcome{ExecutionID: taskID, Status: HostExecutionCancelled, Summary: err.Error()}, nil
		}
		snapshot, err := h.taskflow.CheckTask(ctx, taskID)
		if err != nil {
			return HostExecutionOutcome{}, err
		}
		switch snapshot.Status {
		case TeamHarnessTaskAssigned, TeamHarnessTaskPrepared:
			// Assignment is evidence only; wait for worker ack/submit.
		case TeamHarnessTaskInProgress:
			acknowledged = true
		case TeamHarnessTaskSubmitted:
			if h.evidence != nil {
				if _, err := h.evidence.IngestExecutionEvidence(ctx, request, snapshot); err != nil {
					return HostExecutionOutcome{}, err
				}
			}
			return HostExecutionOutcome{ExecutionID: taskID, Status: HostExecutionCompleted, Summary: snapshot.Summary, Acknowledged: acknowledged || snapshot.Acknowledged}, nil
		case TeamHarnessTaskCancelled:
			return HostExecutionOutcome{ExecutionID: taskID, Status: HostExecutionCancelled, Summary: snapshot.Summary, Acknowledged: acknowledged || snapshot.Acknowledged}, nil
		case TeamHarnessTaskFailed:
			if recorder, ok := h.evidence.(interface {
				RecordExecutionFailure(context.Context, HostExecutionRequest) error
			}); ok {
				if err := recorder.RecordExecutionFailure(ctx, request); err != nil {
					return HostExecutionOutcome{}, err
				}
			}
			return HostExecutionOutcome{ExecutionID: taskID, Status: HostExecutionFailed, Summary: snapshot.Summary, Acknowledged: acknowledged || snapshot.Acknowledged}, nil
		case TeamHarnessTaskWaiting, TeamHarnessTaskStopped:
			return HostExecutionOutcome{}, &UnsupportedControlFlowError{Flow: string(snapshot.Status)}
		default:
			return HostExecutionOutcome{}, fmt.Errorf("unknown TeamHarness task status %q", snapshot.Status)
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return HostExecutionOutcome{ExecutionID: taskID, Status: HostExecutionCancelled, Summary: ctx.Err().Error(), Acknowledged: acknowledged}, nil
		case <-timer.C:
		}
	}
}

// TaskflowClient is the adapter-private representation of the TeamHarness
// taskflow transport. M1's future implementation can use a supported Python
// API or MCP transport once that boundary is selected; its callers never see
// TeamHarness action names outside this package.
type TaskflowClient interface {
	DelegateTask(ctx context.Context, request TeamHarnessDelegateTaskRequest) error
	CheckTask(ctx context.Context, taskID string) (TeamHarnessTaskSnapshot, error)
}

// TaskflowCanceller is an optional transport capability because only await
// relinquish needs TeamHarness cancel_task. Keeping it separate preserves the
// fresh-execution TaskflowClient contract for existing transports.
type TaskflowCanceller interface {
	CancelTask(ctx context.Context, taskID, reason string) error
}

// TaskflowActivationObserver is the non-blocking M4-D activation seam. It
// delegates through the existing production TaskflowClient, then observes the
// taskflow-owned status until the worker has accepted it. It deliberately does
// not write InvocationTaskMap: epoch-aware carrier history belongs to Runtime's
// PhysicalExecutionStore, while InvocationTaskMap retains legacy current-task
// compatibility for fresh execution.
type TaskflowActivationObserver struct {
	Taskflow     TaskflowClient
	PollInterval time.Duration
}

type TeamHarnessTaskActivation struct {
	TaskID       string
	AssignedTo   string
	Status       TeamHarnessTaskStatus
	Acknowledged bool
	ObservedAt   time.Time
}

func (o TaskflowActivationObserver) DelegateAndObserveAcceptance(ctx context.Context, request TeamHarnessDelegateTaskRequest) (TeamHarnessTaskActivation, error) {
	if o.Taskflow == nil {
		return TeamHarnessTaskActivation{}, contractError("taskflow client is required")
	}
	if request.TaskID == "" || request.Assignee == "" {
		return TeamHarnessTaskActivation{}, contractError("task id and assignee are required")
	}
	if err := o.Taskflow.DelegateTask(ctx, request); err != nil {
		return TeamHarnessTaskActivation{}, err
	}
	interval := o.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		snapshot, err := o.Taskflow.CheckTask(ctx, request.TaskID)
		if err != nil {
			return TeamHarnessTaskActivation{}, err
		}
		// check_task is authoritative for the persisted task identity. A caller
		// supplied ID is not accepted as evidence until TeamHarness returns it.
		if snapshot.TaskID == "" || snapshot.TaskID != request.TaskID {
			return TeamHarnessTaskActivation{}, contractError("taskflow returned a mismatched task id")
		}
		if snapshot.Status == TeamHarnessTaskInProgress || (snapshot.Status == TeamHarnessTaskAssigned && snapshot.Acknowledged) {
			return TeamHarnessTaskActivation{TaskID: snapshot.TaskID, AssignedTo: request.Assignee, Status: snapshot.Status, Acknowledged: snapshot.Acknowledged, ObservedAt: time.Now().UTC()}, nil
		}
		if snapshot.Status == TeamHarnessTaskSubmitted {
			return TeamHarnessTaskActivation{}, contractError("task submitted before activation observation")
		}
		if snapshot.Status == TeamHarnessTaskCancelled || snapshot.Status == TeamHarnessTaskFailed {
			return TeamHarnessTaskActivation{}, fmt.Errorf("taskflow activation ended in %s", snapshot.Status)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return TeamHarnessTaskActivation{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// CancelInvocationTask cancels the transient TeamHarness task associated with
// a logical Threadmill invocation. It is physical-carrier cleanup only: it
// does not cancel or complete the Threadmill Invocation itself.
func (h *TeamHarnessExecutionHost) CancelInvocationTask(ctx context.Context, invocationID, reason string) error {
	taskID, found, err := h.taskIDs.Lookup(ctx, invocationID)
	if err != nil {
		return err
	}
	if !found {
		return contractError("no TeamHarness task is recorded for invocation_id")
	}
	return h.CancelTeamHarnessTask(ctx, taskID, reason)
}

// CancelTeamHarnessTask exposes the narrow carrier-level cancellation port
// used by Runtime await relinquish after it has resolved the transient task.
func (h *TeamHarnessExecutionHost) CancelTeamHarnessTask(ctx context.Context, taskID, reason string) error {
	canceller, ok := h.taskflow.(TaskflowCanceller)
	if !ok {
		return contractError("taskflow client does not support cancellation")
	}
	return canceller.CancelTask(ctx, taskID, reason)
}

// TeamHarnessDelegateTaskRequest is exactly the M1 subset derived from the
// real taskflow delegate inputs: project, task, room, assignee and spec.
type TeamHarnessDelegateTaskRequest struct {
	ProjectID string `json:"project_id"`
	TaskID    string `json:"task_id"`
	RoomID    string `json:"room_id"`
	Assignee  string `json:"assignee"`
	Title     string `json:"title"`
	Spec      string `json:"spec"`
}

// TeamHarnessTaskStatus matches only taskflow states relevant to fresh M1
// observation. submitted is physical worker submission, not acceptance.
type TeamHarnessTaskStatus string

const (
	TeamHarnessTaskPrepared   TeamHarnessTaskStatus = "prepared"
	TeamHarnessTaskAssigned   TeamHarnessTaskStatus = "assigned"
	TeamHarnessTaskInProgress TeamHarnessTaskStatus = "in_progress"
	TeamHarnessTaskSubmitted  TeamHarnessTaskStatus = "submitted"
	TeamHarnessTaskCancelled  TeamHarnessTaskStatus = "cancelled"
	TeamHarnessTaskFailed     TeamHarnessTaskStatus = "failed"
	TeamHarnessTaskWaiting    TeamHarnessTaskStatus = "waiting"
	TeamHarnessTaskStopped    TeamHarnessTaskStatus = "stopped"
)

// TeamHarnessTaskSnapshot is host evidence obtained through check_task or its
// future direct store implementation. ResultStatus/deliverables remain
// evidence and are intentionally not converted to PhaseOutput in M1.
type TeamHarnessTaskSnapshot struct {
	TaskID       string                `json:"task_id"`
	Status       TeamHarnessTaskStatus `json:"status"`
	Acknowledged bool                  `json:"acknowledged"`
	Summary      string                `json:"summary"`
	ResultStatus string                `json:"result_status"`
	Deliverables []string              `json:"deliverables"`
	ResultPath   string                `json:"result_path"`
}

// TaskflowRouteResolver supplies TeamHarness project/room routing owned by
// the Runtime integration. It is not a worker scheduler.
type TaskflowRouteResolver interface {
	ResolveTaskflowRoute(ctx context.Context, request HostExecutionRequest) (TaskflowRoute, error)
}

type TaskflowRoute struct {
	ProjectID string `json:"project_id"`
	RoomID    string `json:"room_id"`
}

// WorkerSelector is intentionally minimal because TeamHarness delegate_task
// requires an assignee; provider/worker availability policy remains Runtime
// integration, not a new Threadmill scheduler here.
type WorkerSelector interface {
	SelectWorker(ctx context.Context, request HostExecutionRequest) (string, error)
}

// InvocationTaskMap records the private InvocationID <-> TeamHarness task-ID
// relation. M1 provides an in-memory implementation; Runtime must replace it
// with durable storage before recovery, cancellation, or Event Log linking.
type InvocationTaskMap interface {
	Record(ctx context.Context, invocationID, taskID string) error
	Lookup(ctx context.Context, invocationID string) (string, bool, error)
}

// InMemoryInvocationTaskMap is suitable only for M1 tests and local fresh
// invocations. It is deliberately not a persistence design.
type InMemoryInvocationTaskMap struct {
	mu    sync.RWMutex
	tasks map[string]string
}

func NewInMemoryInvocationTaskMap() *InMemoryInvocationTaskMap {
	return &InMemoryInvocationTaskMap{tasks: make(map[string]string)}
}

func (m *InMemoryInvocationTaskMap) Record(_ context.Context, invocationID, taskID string) error {
	if invocationID == "" || taskID == "" {
		return contractError("invocation_id and task_id are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.tasks[invocationID]; ok && existing != taskID {
		return contractError("invocation_id is already mapped to a different task_id")
	}
	m.tasks[invocationID] = taskID
	return nil
}

func (m *InMemoryInvocationTaskMap) Lookup(_ context.Context, invocationID string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	taskID, ok := m.tasks[invocationID]
	return taskID, ok, nil
}

// TaskIDGenerator gives AgentTeams its own task identity. The default derives
// a stable, taskflow-safe ID with an explicit "tm-phase-" namespace, so it is
// never confused with the Threadmill InvocationID.
type TaskIDGenerator interface {
	NewTaskID(ctx context.Context, request HostExecutionRequest) (string, error)
}

type DefaultTaskIDGenerator struct{}

func (DefaultTaskIDGenerator) NewTaskID(_ context.Context, request HostExecutionRequest) (string, error) {
	if request.InvocationID == "" {
		return "", contractError("invocation_id is required")
	}
	return "tm-phase-" + taskflowSafeID(request.InvocationID), nil
}

func taskflowSafeID(value string) string {
	var out strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('-')
		}
	}
	return out.String()
}

// BuildDelegateTaskRequest projects only worker-appropriate material into the
// TeamHarness task spec. It intentionally excludes BindingRef, trusted MCP
// fields, permissions, lease identity and the HostExecutionRequest JSON.
func BuildDelegateTaskRequest(request HostExecutionRequest, route TaskflowRoute, assignee, taskID string) TeamHarnessDelegateTaskRequest {
	return TeamHarnessDelegateTaskRequest{
		ProjectID: route.ProjectID,
		TaskID:    taskID,
		RoomID:    route.RoomID,
		Assignee:  assignee,
		Title:     "Threadmill " + string(request.Role.Phase) + " phase",
		Spec:      BuildTaskSpec(request),
	}
}

// BuildTaskSpec makes the explicit TeamHarness spec.md projection. Filesystem
// and tool policy are descriptive in M1 only; M2/M7 will install and enforce
// actual MCP policies, permission binding, lease and AllowedDirs controls.
func BuildTaskSpec(request HostExecutionRequest) string {
	var spec strings.Builder
	fmt.Fprintf(&spec, "# Threadmill %s phase\n\n", request.Role.Phase)
	fmt.Fprintln(&spec, "## Task instructions")
	fmt.Fprintln(&spec, request.Envelope.TaskSpec)
	fmt.Fprintln(&spec, "\n## Workspace")
	fmt.Fprintf(&spec, "- Root: `%s`\n", request.Envelope.Workspace.Root)
	fmt.Fprintf(&spec, "- Allowed directories: `%s`\n", strings.Join(request.Envelope.Workspace.AllowedDirs, ", "))
	fmt.Fprintf(&spec, "- Read-only: `%t`\n", request.Envelope.Workspace.ReadOnly)
	fmt.Fprintln(&spec, "\n## Formal inputs")
	fmt.Fprintf(&spec, "- Revision: `%s`\n", request.Inputs.InputRevision)
	fmt.Fprintf(&spec, "- Required: %d; delivered: %d; pending: %d\n", len(request.Inputs.Required), len(request.Inputs.Delivered), len(request.Inputs.Pending))
	fmt.Fprintln(&spec, "\n## Authorized context")
	fmt.Fprintln(&spec, request.Envelope.Context.Content)
	fmt.Fprintln(&spec, "\n## Task Memory")
	fmt.Fprintf(&spec, "- Visible memory candidates: %d\n", len(request.Envelope.TaskMemory.Candidates))
	fmt.Fprintln(&spec, "\n## Threadmill tool and phase rules")
	fmt.Fprintln(&spec, "- Use only Threadmill tools injected by the execution host; M1 does not yet install those tools.")
	fmt.Fprintln(&spec, "- Do not use project orchestration, Coordination Graph writes, mailbox messaging, or context.search.")
	fmt.Fprintf(&spec, "- Implementation write allowed: `%t`; structured artifact write allowed: `%t`; evidence write allowed: `%t`.\n", request.Policy.AllowImplementationWrite, request.Policy.AllowStructuredArtifactWrite, request.Policy.AllowEvidenceWrite)
	fmt.Fprintln(&spec, "- A TeamHarness submission is execution evidence only. It is not PhaseOutput, endpoint acceptance, verify pass, or Task completion.")
	return spec.String()
}
