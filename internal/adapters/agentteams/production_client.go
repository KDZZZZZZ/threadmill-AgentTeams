package agentteams

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

type InvocationMCPMaterial struct {
	URL             string
	BearerToken     string
	TokenIdentifier string
	ExpectedTools   []string
}

type InvocationMCPResolver interface {
	ResolveInvocationMCP(context.Context, HostPreparation) (InvocationMCPMaterial, error)
	RevokeInvocationMCP(context.Context, kernel.InvocationID) error
}

type QwenPawProvider interface {
	ForHost(context.Context, string) (*QwenPawAPI, error)
}

type TaskflowCaller interface {
	Call(context.Context, string, TaskflowCall) (TaskflowCallResult, error)
}

type ContainerResolver interface {
	ContainerForHost(context.Context, string) (string, error)
}

type RawObservationReader interface {
	ReadRawObservations(context.Context, string) ([]RawObservation, error)
}

type hostSlotStore interface {
	ActiveCounts(context.Context) (map[string]int, error)
	Claim(context.Context, string, kernel.InvocationID, string, []byte, string) error
	Release(context.Context, string, string) error
	MarkRevoked(context.Context, string, string) error
	MarkHostFenced(context.Context, string) error
	BeginHostFence(context.Context, string) ([]HostSlotClaim, error)
	CompleteHostFence(context.Context, string, []HostSlotClaim) error
	ClearHostFenceIfReusable(context.Context, string) (bool, error)
	ByInvocation(context.Context, string, kernel.InvocationID) (HostSlotClaim, bool, error)
	ByTaskID(context.Context, string) (HostSlotClaim, bool, error)
	ActiveByHost(context.Context, string) ([]HostSlotClaim, error)
}

const productionCleanupTimeout = 5 * time.Second

type ProductionClientOptions struct {
	Controller      *AgentTeamsControllerClient
	Slots           hostSlotStore
	MCPResolver     InvocationMCPResolver
	QwenPaw         QwenPawProvider
	Taskflow        TaskflowCaller
	Containers      ContainerResolver
	ManagerWorkers  map[string]string
	TaskflowHostRef string
	Observations    RawObservationReader
}

type ProductionClient struct {
	controller      *AgentTeamsControllerClient
	slots           hostSlotStore
	mcpResolver     InvocationMCPResolver
	qwenPaw         QwenPawProvider
	taskflow        TaskflowCaller
	containers      ContainerResolver
	managerWorkers  map[string]string
	taskflowHostRef string
	observations    RawObservationReader
}

func NewProductionClient(options ProductionClientOptions) (*ProductionClient, error) {
	if options.Controller == nil || options.Slots == nil || options.MCPResolver == nil ||
		options.QwenPaw == nil || options.Taskflow == nil || options.Containers == nil {
		return nil, kernel.InvalidArgument("AgentTeams production client dependencies are required")
	}
	managerWorkers := make(map[string]string, len(options.ManagerWorkers))
	for logicalHost, worker := range options.ManagerWorkers {
		logicalHost = strings.TrimSpace(logicalHost)
		worker = strings.TrimSpace(worker)
		if !safeProviderID(logicalHost) || !safeProviderID(worker) || logicalHost == worker {
			return nil, kernel.InvalidArgument("AgentTeams manager worker alias is invalid")
		}
		managerWorkers[logicalHost] = worker
	}
	taskflowHostRef := strings.TrimSpace(options.TaskflowHostRef)
	if taskflowHostRef != "" && !safeProviderID(taskflowHostRef) {
		return nil, kernel.InvalidArgument("AgentTeams taskflow host is invalid")
	}
	return &ProductionClient{
		controller:      options.Controller,
		slots:           options.Slots,
		mcpResolver:     options.MCPResolver,
		qwenPaw:         options.QwenPaw,
		taskflow:        options.Taskflow,
		containers:      options.Containers,
		managerWorkers:  managerWorkers,
		taskflowHostRef: taskflowHostRef,
		observations:    options.Observations,
	}, nil
}

func (c *ProductionClient) Check(ctx context.Context) error {
	return c.controller.Check(ctx)
}

func (c *ProductionClient) ListHosts(ctx context.Context) ([]HostStatus, error) {
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return nil, err
	}
	logicalByWorker := make(map[string]string, len(c.managerWorkers))
	for logicalHost, worker := range c.managerWorkers {
		logicalByWorker[worker] = logicalHost
	}
	projected := make([]HostStatus, 0, len(hosts))
	for _, host := range hosts {
		if host.Ref == c.taskflowHostRef {
			continue
		}
		if host.Kind == HostManager {
			if _, replaced := c.managerWorkers[host.Ref]; replaced {
				continue
			}
		}
		if logicalHost, aliased := logicalByWorker[host.Ref]; aliased && host.Kind == HostWorker {
			host.Ref = logicalHost
			host.Kind = HostManager
			host.Capabilities = normalizeToolNames(append(host.Capabilities, "manager"))
		}
		projected = append(projected, host)
	}
	hosts = projected
	counts, err := c.slots.ActiveCounts(ctx)
	if err != nil {
		return nil, err
	}
	for index := range hosts {
		hosts[index].ActiveExecutions = counts[hosts[index].Ref]
		if hosts[index].Capacity <= 0 {
			hosts[index].Capacity = 1
		}
	}
	return hosts, nil
}

func (c *ProductionClient) PrepareHost(ctx context.Context, prep HostPreparation) error {
	if err := validateHostPreparation(prep); err != nil {
		return err
	}
	material, err := c.mcpResolver.ResolveInvocationMCP(ctx, prep)
	if err != nil {
		return err
	}
	key, err := InvocationMCPKey(prep.InvocationID)
	if err != nil {
		return err
	}
	desired := InvocationMCP{
		Key:           key,
		URL:           strings.TrimSpace(material.URL),
		BearerToken:   strings.TrimSpace(material.BearerToken),
		ExpectedTools: material.ExpectedTools,
	}
	if err := validateInvocationMCP(desired); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.mcpResolver.RevokeInvocationMCP(cleanupCtx, prep.InvocationID))
	}
	tokenIdentifier := strings.TrimSpace(material.TokenIdentifier)
	if tokenIdentifier == "" {
		tokenIdentifier = key
	}
	if err := c.slots.Claim(ctx, prep.HostRef, prep.InvocationID, key, auth.HashOpaqueSecret(desired.BearerToken), tokenIdentifier); err != nil {
		if kernel.IsCode(err, kernel.CodeExecutorUnavailable) {
			if clearErr := c.clearReusableFenceForClaim(ctx, prep.HostRef); clearErr != nil {
				cleanupCtx, cancel := boundedCleanupContext(ctx)
				defer cancel()
				return errors.Join(err, clearErr, c.mcpResolver.RevokeInvocationMCP(cleanupCtx, prep.InvocationID))
			}
			if retryErr := c.slots.Claim(ctx, prep.HostRef, prep.InvocationID, key, auth.HashOpaqueSecret(desired.BearerToken), tokenIdentifier); retryErr == nil {
				goto claimed
			} else {
				err = errors.Join(err, retryErr)
			}
		}
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.mcpResolver.RevokeInvocationMCP(cleanupCtx, prep.InvocationID))
	}
claimed:
	qwenPaw, err := c.qwenPaw.ForHost(ctx, prep.HostRef)
	if err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, nil))
	}
	if err := qwenPaw.InstallInvocationMCP(ctx, desired); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, qwenPaw))
	}
	providerHost := c.providerHost(prep.HostRef)
	if providerHost != prep.HostRef || prep.Role != auth.RoleTaskManager && prep.Role != auth.RoleContext {
		if err := c.controller.EnsureWorkerReady(ctx, providerHost); err != nil {
			cleanupCtx, cancel := boundedCleanupContext(ctx)
			defer cancel()
			return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, qwenPaw))
		}
	}
	return nil
}

func (c *ProductionClient) DelegateTask(ctx context.Context, req DelegateTaskRequest) (TaskSnapshot, error) {
	if strings.TrimSpace(req.HostRef) == "" || strings.TrimSpace(req.TaskID) == "" {
		return TaskSnapshot{}, kernel.InvalidArgument("delegate_task host_ref and task_id are required")
	}
	container, err := c.taskflowContainer(ctx, req.HostRef)
	if err != nil {
		return TaskSnapshot{}, err
	}
	result, err := c.taskflow.Call(ctx, container, TaskflowCall{
		Action:     "delegate_task",
		ProjectID:  string(req.ProjectID),
		TaskID:     req.TaskID,
		RoomID:     req.RoomID,
		AssignedTo: c.providerHost(req.HostRef),
		Spec:       req.Spec,
	})
	if err != nil {
		return TaskSnapshot{}, err
	}
	providerHost := c.providerHost(req.HostRef)
	providerMatrixID, _ := matrixUserIDForWorker(req.RoomID, providerHost)
	if result.Task.HostRef == providerHost || result.Task.HostRef == providerMatrixID {
		result.Task.HostRef = req.HostRef
	}
	return result.Task, nil
}

func (c *ProductionClient) ReleaseHostSlot(ctx context.Context, taskID string, hostRef string) error {
	return c.slots.Release(ctx, taskID, hostRef)
}

func (c *ProductionClient) RevokeInvocation(ctx context.Context, hostRef string, invocationID kernel.InvocationID) error {
	claim, ok, err := c.slots.ByInvocation(ctx, hostRef, invocationID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams invocation MCP slot not found"}
	}
	if err := c.mcpResolver.RevokeInvocationMCP(ctx, invocationID); err != nil {
		return err
	}
	if !claim.RevokedAt.IsZero() {
		return nil
	}
	qwenPaw, err := c.qwenPaw.ForHost(ctx, hostRef)
	if err != nil {
		return err
	}
	if err := qwenPaw.RevokeInvocationMCP(ctx, claim.MCPClientKey); err != nil {
		return err
	}
	return c.slots.MarkRevoked(ctx, claim.TaskID, hostRef)
}

func (c *ProductionClient) ForceStopHost(ctx context.Context, hostRef string) error {
	claims, err := c.slots.BeginHostFence(ctx, hostRef)
	if err != nil {
		return err
	}
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		if host.Ref != hostRef {
			continue
		}
		if host.Kind == HostManager {
			if providerHost := c.providerHost(hostRef); providerHost != hostRef {
				if err := c.controller.StopWorker(ctx, providerHost); err != nil {
					return err
				}
			} else if err := c.controller.StopManager(ctx, hostRef); err != nil {
				return err
			}
		} else if err := c.controller.StopWorker(ctx, hostRef); err != nil {
			return err
		}
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		var revokeErr error
		for _, claim := range claims {
			revokeErr = errors.Join(revokeErr, c.mcpResolver.RevokeInvocationMCP(cleanupCtx, claim.InvocationID))
		}
		if revokeErr != nil {
			return revokeErr
		}
		return c.slots.CompleteHostFence(cleanupCtx, hostRef, claims)
	}
	return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams host not found"}
}

func (c *ProductionClient) providerHost(logicalHost string) string {
	if worker := c.managerWorkers[logicalHost]; worker != "" {
		return worker
	}
	return logicalHost
}

func (c *ProductionClient) taskflowContainer(ctx context.Context, fallbackHost string) (string, error) {
	hostRef := c.taskflowHostRef
	if hostRef == "" {
		hostRef = fallbackHost
	}
	return c.containers.ContainerForHost(ctx, hostRef)
}

func (c *ProductionClient) CancelTask(ctx context.Context, taskID string, reason string) error {
	claim, ok, err := c.slots.ByTaskID(ctx, taskID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams task slot not found"}
	}
	container, err := c.taskflowContainer(ctx, claim.HostRef)
	if err != nil {
		return err
	}
	_, err = c.taskflow.Call(ctx, container, TaskflowCall{Action: "cancel_task", TaskID: taskID, Reason: reason})
	return err
}

func (c *ProductionClient) CheckTask(ctx context.Context, taskID string) (TaskCheck, error) {
	claim, ok, err := c.slots.ByTaskID(ctx, taskID)
	if err != nil {
		return TaskCheck{}, err
	}
	if !ok {
		return TaskCheck{}, kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams task slot not found"}
	}
	container, err := c.taskflowContainer(ctx, claim.HostRef)
	if err != nil {
		return TaskCheck{}, err
	}
	result, err := c.taskflow.Call(ctx, container, TaskflowCall{Action: "check_task", TaskID: taskID})
	if err != nil {
		return TaskCheck{}, err
	}
	return TaskCheck{
		Task:             result.Task,
		ResultStatus:     result.ResultStatus,
		Summary:          result.Summary,
		Deliverables:     append([]string(nil), result.Deliverables...),
		Effective:        result.Effective,
		ValidationErrors: append([]string(nil), result.ValidationErrors...),
	}, nil
}

func (c *ProductionClient) ReadObservations(ctx context.Context, cursor string) ([]RawObservation, error) {
	if c.observations == nil {
		return nil, nil
	}
	return c.observations.ReadRawObservations(ctx, cursor)
}

func (c *ProductionClient) cleanupPreparationFailure(ctx context.Context, hostRef string, invocationID kernel.InvocationID, qwenPaw *QwenPawAPI) error {
	claim, ok, err := c.slots.ByInvocation(ctx, hostRef, invocationID)
	if err != nil || !ok {
		return err
	}
	tokenErr := c.mcpResolver.RevokeInvocationMCP(ctx, invocationID)
	if qwenPaw != nil && tokenErr == nil {
		if revokeErr := qwenPaw.RevokeInvocationMCP(ctx, claim.MCPClientKey); revokeErr == nil {
			if err := c.slots.MarkRevoked(ctx, claim.TaskID, hostRef); err != nil {
				return err
			}
			return c.slots.Release(ctx, claim.TaskID, hostRef)
		}
	}
	if err := c.ForceStopHost(ctx, hostRef); err != nil {
		return errors.Join(tokenErr, err)
	}
	return c.slots.Release(ctx, claim.TaskID, hostRef)
}

func (c *ProductionClient) clearReusableFenceForClaim(ctx context.Context, hostRef string) error {
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		if host.Ref != hostRef {
			continue
		}
		if !hostObservedReadyOrRunning(host) {
			return nil
		}
		_, err := c.slots.ClearHostFenceIfReusable(ctx, hostRef)
		return err
	}
	return nil
}

func hostObservedReadyOrRunning(host HostStatus) bool {
	phase := strings.ToLower(strings.TrimSpace(host.Phase))
	return phase == "ready" || phase == "running"
}

func boundedCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), productionCleanupTimeout)
}

func validateHostPreparation(prep HostPreparation) error {
	if strings.TrimSpace(prep.HostRef) == "" {
		return kernel.InvalidArgument("AgentTeams host_ref is required")
	}
	if err := kernel.RequireID("invocation_id", prep.InvocationID); err != nil {
		return err
	}
	if _, err := hostKindForRole(prep.Role); err != nil {
		return err
	}
	if strings.TrimSpace(prep.RuntimeConfigRef) == "" || strings.TrimSpace(prep.EnvelopeRef) == "" {
		return kernel.InvalidArgument("Runtime config and signed envelope references are required")
	}
	return nil
}

type StaticContainerResolver map[string]string

func (r StaticContainerResolver) ContainerForHost(_ context.Context, hostRef string) (string, error) {
	container := strings.TrimSpace(r[hostRef])
	if !safeContainerName(container) {
		return "", kernel.InvalidArgument("QwenPaw container name is invalid")
	}
	return container, nil
}

type DockerQwenPawProvider struct {
	Containers   ContainerResolver
	DockerBinary string
	PythonBinary string
}

func (p DockerQwenPawProvider) ForHost(ctx context.Context, hostRef string) (*QwenPawAPI, error) {
	container, err := p.Containers.ContainerForHost(ctx, hostRef)
	if err != nil {
		return nil, err
	}
	transport, err := NewDockerExecRoundTripper(container, p.DockerBinary, p.PythonBinary)
	if err != nil {
		return nil, err
	}
	port := qwenPawManagementPort(container)
	return NewQwenPawAPI("http://127.0.0.1:"+strconv.Itoa(port), &http.Client{Transport: transport})
}

func qwenPawManagementPort(container string) int {
	if container == "agentteams-manager" || strings.HasPrefix(container, "agentteams-manager-") {
		return 18799
	}
	return 8088
}

var _ Client = (*ProductionClient)(nil)
