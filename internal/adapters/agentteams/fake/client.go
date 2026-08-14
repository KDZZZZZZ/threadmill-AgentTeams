package fake

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	adapter "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/adapters/agentteams"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type taskRecord struct {
	Request      adapter.DelegateTaskRequest
	Check        adapter.TaskCheck
	Result       []byte
	Reason       string
	SlotReleased bool
}

type Client struct {
	mu sync.Mutex

	hosts         map[string]adapter.HostStatus
	tasks         map[string]taskRecord
	observations  []adapter.RawObservation
	preparations  []adapter.HostPreparation
	calls         []string
	delegateCalls int

	FailPrepare           error
	FailDelegate          error
	DelegateResponseError error
	FailRevoke            error
	FailForceStop         error
	FailRelease           error
	FailCancel            error
	FailCheck             error
	FailObserve           error
	FailPull              error
	FailReadResult        error
}

func NewClient() *Client {
	return &Client{
		hosts: make(map[string]adapter.HostStatus),
		tasks: make(map[string]taskRecord),
	}
}

func (c *Client) AddHost(host adapter.HostStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	host.Capabilities = append([]string(nil), host.Capabilities...)
	c.hosts[host.Ref] = host
}

func (c *Client) SetDelegateResponseError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.DelegateResponseError = err
}

func (c *Client) ListHosts(ctx context.Context) ([]adapter.HostStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	hosts := make([]adapter.HostStatus, 0, len(c.hosts))
	for _, host := range c.hosts {
		host.Capabilities = append([]string(nil), host.Capabilities...)
		hosts = append(hosts, host)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Ref < hosts[j].Ref })
	return hosts, nil
}

func (c *Client) PrepareHost(ctx context.Context, preparation adapter.HostPreparation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "prepare:"+preparation.HostRef)
	if c.FailPrepare != nil {
		return c.FailPrepare
	}
	if _, ok := c.hosts[preparation.HostRef]; !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "fake host not found"}
	}
	c.preparations = append(c.preparations, preparation)
	return nil
}

func (c *Client) DelegateTask(ctx context.Context, request adapter.DelegateTaskRequest) (adapter.TaskSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return adapter.TaskSnapshot{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delegateCalls++
	c.calls = append(c.calls, "delegate:"+request.TaskID)
	if c.FailDelegate != nil {
		return adapter.TaskSnapshot{}, c.FailDelegate
	}
	if existing, ok := c.tasks[request.TaskID]; ok {
		if !reflect.DeepEqual(existing.Request, request) {
			return adapter.TaskSnapshot{}, kernel.IdempotencyConflict()
		}
		return existing.Check.Task, nil
	}
	host, ok := c.hosts[request.HostRef]
	if !ok {
		return adapter.TaskSnapshot{}, kernel.Error{Code: kernel.CodeNotFound, Message: "fake host not found"}
	}
	if host.ActiveExecutions >= host.Capacity {
		return adapter.TaskSnapshot{}, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "fake host capacity exhausted", Recoverable: true}
	}
	host.ActiveExecutions++
	c.hosts[request.HostRef] = host
	task := adapter.TaskSnapshot{
		TaskID:    request.TaskID,
		ProjectID: request.ProjectID,
		HostRef:   request.HostRef,
		Status:    "assigned",
		EventID:   "delegate:" + request.TaskID,
	}
	c.tasks[request.TaskID] = taskRecord{
		Request: request,
		Check:   adapter.TaskCheck{Task: task},
	}
	c.appendObservationLocked(adapter.RawObservation{
		ProviderEventID: "delegate:" + request.TaskID,
		TaskID:          request.TaskID,
		HostRef:         request.HostRef,
		Kind:            "execution_dispatched",
		ObservedAt:      time.Now().UTC(),
	})
	if c.DelegateResponseError != nil {
		return adapter.TaskSnapshot{}, c.DelegateResponseError
	}
	return task, nil
}

func (c *Client) ReleaseHostSlot(ctx context.Context, taskID, hostRef string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "release:"+taskID)
	if c.FailRelease != nil {
		return c.FailRelease
	}
	record, ok := c.tasks[taskID]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "fake task not found"}
	}
	if record.Request.HostRef != hostRef {
		return kernel.Error{Code: kernel.CodeInternalError, Message: "fake release host mismatch"}
	}
	if record.SlotReleased {
		return nil
	}
	host, ok := c.hosts[hostRef]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "fake host not found"}
	}
	if host.ActiveExecutions > 0 {
		host.ActiveExecutions--
	}
	c.hosts[hostRef] = host
	record.SlotReleased = true
	c.tasks[taskID] = record
	return nil
}

func (c *Client) RevokeInvocation(ctx context.Context, hostRef string, invocationID kernel.InvocationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "revoke:"+string(invocationID))
	return c.FailRevoke
}

func (c *Client) FenceInvocation(ctx context.Context, _ string, invocationID kernel.InvocationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "fence:"+string(invocationID))
	return c.FailRevoke
}

func (c *Client) ForceStopHost(ctx context.Context, hostRef string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "force-stop:"+hostRef)
	return c.FailForceStop
}

func (c *Client) CancelTask(ctx context.Context, taskID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "cancel:"+taskID)
	if c.FailCancel != nil {
		return c.FailCancel
	}
	record, ok := c.tasks[taskID]
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "fake task not found"}
	}
	if record.Reason != "" && record.Reason != reason {
		return kernel.IdempotencyConflict()
	}
	record.Reason = reason
	record.Check.Task.Status = "cancelled"
	c.tasks[taskID] = record
	c.appendObservationLocked(adapter.RawObservation{
		ProviderEventID: "cancel:" + taskID,
		TaskID:          taskID,
		HostRef:         record.Request.HostRef,
		Kind:            "execution_cancelled",
		Payload:         map[string]string{"reason": reason},
		ObservedAt:      time.Now().UTC(),
	})
	return nil
}

func (c *Client) CheckTask(ctx context.Context, taskID string) (adapter.TaskCheck, error) {
	if err := ctx.Err(); err != nil {
		return adapter.TaskCheck{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailCheck != nil {
		return adapter.TaskCheck{}, c.FailCheck
	}
	record, ok := c.tasks[taskID]
	if !ok {
		return adapter.TaskCheck{}, kernel.Error{Code: kernel.CodeNotFound, Message: "fake task not found"}
	}
	return cloneCheck(record.Check), nil
}

func (c *Client) ReadObservations(ctx context.Context, cursor string) ([]adapter.RawObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailObserve != nil {
		return nil, c.FailObserve
	}
	position := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 || parsed > len(c.observations) {
			return nil, kernel.InvalidArgument("invalid fake observation cursor")
		}
		position = parsed
	}
	result := make([]adapter.RawObservation, 0, len(c.observations)-position)
	for _, observation := range c.observations[position:] {
		observation.Payload = clonePayload(observation.Payload)
		result = append(result, observation)
	}
	return result, nil
}

func (c *Client) PullExecution(ctx context.Context, execution adapter.AgentTeamsExecutionRef) (adapter.ExecutionWorkspaceCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return adapter.ExecutionWorkspaceCheckpoint{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "pull:"+execution.AgentTeamsTaskID)
	return adapter.ExecutionWorkspaceCheckpoint{}, c.FailPull
}

func (c *Client) PrepareExecution(ctx context.Context, execution adapter.AgentTeamsExecutionRef, _ adapter.PreparedInvocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "prepare-files:"+execution.AgentTeamsTaskID)
	return c.FailPull
}

func (c *Client) ReadResult(ctx context.Context, taskID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailReadResult != nil {
		return nil, c.FailReadResult
	}
	record, ok := c.tasks[taskID]
	if !ok {
		return nil, kernel.Error{Code: kernel.CodeNotFound, Message: "fake task not found"}
	}
	return append([]byte(nil), record.Result...), nil
}

func (c *Client) Ack(taskID string, observedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.tasks[taskID]
	if !ok {
		return errors.New("fake task not found")
	}
	record.Check.Task.Status = "in_progress"
	c.tasks[taskID] = record
	c.appendObservationLocked(adapter.RawObservation{
		ProviderEventID: "ack:" + taskID,
		TaskID:          taskID,
		HostRef:         record.Request.HostRef,
		Kind:            "execution_acked",
		ObservedAt:      observedAt,
	})
	return nil
}

func (c *Client) SetResult(taskID string, check adapter.TaskCheck, document []byte, observedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.tasks[taskID]
	if !ok {
		return errors.New("fake task not found")
	}
	check.Task.TaskID = taskID
	check.Task.ProjectID = record.Request.ProjectID
	check.Task.HostRef = record.Request.HostRef
	check.Task.Status = "submitted"
	record.Check = cloneCheck(check)
	record.Result = append([]byte(nil), document...)
	c.tasks[taskID] = record
	c.appendObservationLocked(adapter.RawObservation{
		ProviderEventID: "submit:" + taskID,
		TaskID:          taskID,
		HostRef:         record.Request.HostRef,
		Kind:            "execution_submitted",
		Payload:         map[string]string{"status": check.ResultStatus},
		ObservedAt:      observedAt,
	})
	return nil
}

func (c *Client) Emit(observation adapter.RawObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appendObservationLocked(observation)
}

func (c *Client) SetHostPhase(ref, phase string, observedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	host, ok := c.hosts[ref]
	if !ok {
		return errors.New("fake host not found")
	}
	host.Phase = phase
	c.hosts[ref] = host
	c.appendObservationLocked(adapter.RawObservation{
		ProviderEventID: "host:" + ref + ":" + phase,
		HostRef:         ref,
		Kind:            "host_" + phase,
		ObservedAt:      observedAt,
	})
	return nil
}

func (c *Client) TaskCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tasks)
}

func (c *Client) ActiveExecutions(hostRef string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hosts[hostRef].ActiveExecutions
}

func (c *Client) DelegateCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.delegateCalls
}

func (c *Client) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *Client) Preparations() []adapter.HostPreparation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]adapter.HostPreparation(nil), c.preparations...)
}

func (c *Client) appendObservationLocked(observation adapter.RawObservation) {
	observation.Cursor = strconv.Itoa(len(c.observations) + 1)
	observation.Payload = clonePayload(observation.Payload)
	c.observations = append(c.observations, observation)
}

func cloneCheck(check adapter.TaskCheck) adapter.TaskCheck {
	check.Deliverables = append([]string(nil), check.Deliverables...)
	check.ValidationErrors = append([]string(nil), check.ValidationErrors...)
	return check
}

func clonePayload(payload map[string]string) map[string]string {
	result := make(map[string]string, len(payload))
	for key, value := range payload {
		result[key] = value
	}
	return result
}
