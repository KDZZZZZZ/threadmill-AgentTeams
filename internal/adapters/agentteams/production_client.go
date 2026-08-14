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

// SharedFilesCaller invokes only TeamHarness's physical filesync primitive.
// It is deliberately separate from taskflow so Runtime workspace checkpointing
// cannot create, complete, or otherwise reinterpret provider tasks.
type SharedFilesCaller interface {
	SnapshotSharedWorkspace(context.Context, string, string) error
	PushSharedPath(context.Context, string, string) error
}

type ContainerResolver interface {
	ContainerForHost(context.Context, string) (string, error)
}

type RawObservationReader interface {
	ReadRawObservations(context.Context, string) ([]RawObservation, error)
}

type hostSlotStore interface {
	ActiveCounts(context.Context) (map[string]int, error)
	StaleMCPClientKeysByHost(context.Context, string) ([]string, error)
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
const productionWorkerReadyHeartbeatMaxAge = 2 * time.Minute
const productionWorkerReadyHeartbeatSkew = 5 * time.Second

type ProductionClientOptions struct {
	Controller      *AgentTeamsControllerClient
	Slots           hostSlotStore
	MCPResolver     InvocationMCPResolver
	QwenPaw         QwenPawProvider
	Taskflow        TaskflowCaller
	SharedFiles     SharedFilesCaller
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
	sharedFiles     SharedFilesCaller
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
		sharedFiles:     options.SharedFiles,
		containers:      options.Containers,
		managerWorkers:  managerWorkers,
		taskflowHostRef: taskflowHostRef,
		observations:    options.Observations,
	}, nil
}

// PushExecutionFiles mirrors one already-claimed execution directory from its
// assigned worker to shared object storage. The adapter validates the durable
// execution identity before reaching this provider-only seam.
func (c *ProductionClient) PushExecutionFiles(ctx context.Context, execution AgentTeamsExecutionRef) error {
	if c.sharedFiles == nil {
		return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams shared filesync is not configured", Recoverable: true}
	}
	claim, ok, err := c.slots.ByTaskID(ctx, execution.AgentTeamsTaskID)
	if err != nil {
		return err
	}
	if !ok || claim.InvocationID != execution.InvocationID || claim.HostRef != execution.HostRef || !claim.ReleasedAt.IsZero() {
		return kernel.StaleBinding("AgentTeams execution claim does not match workspace synchronization")
	}
	container, err := c.containers.ContainerForHost(ctx, execution.HostRef)
	if err != nil {
		return err
	}
	if err := c.sharedFiles.SnapshotSharedWorkspace(ctx, container, execution.AgentTeamsTaskID); err != nil {
		return err
	}
	return c.sharedFiles.PushSharedPath(ctx, container, "shared/tasks/"+execution.AgentTeamsTaskID+"/")
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
	_, hasDedicatedContextHost := c.managerWorkers["context"]
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
			// Dedicated manager workers are internal, controller-managed carriers.
			// After a Docker/controller restart they may legitimately be stopped or
			// not yet materialized. Project that recoverable state as Sleeping so
			// host selection can reach PrepareHost, which performs the authoritative
			// EnsureWorkerReady and QwenPaw readiness checks. Explicitly stopped
			// Phase workers keep their stop semantics.
			if strings.EqualFold(strings.TrimSpace(host.Phase), "stopped") {
				host.Phase = "Sleeping"
			}
			host.Capabilities = append(host.Capabilities, "manager")
			switch logicalHost {
			case "context":
				host.Capabilities = append(host.Capabilities, CapabilityContextAgent)
			case "default":
				host.Capabilities = append(host.Capabilities, CapabilityTaskManager)
				if !hasDedicatedContextHost {
					host.Capabilities = append(host.Capabilities, CapabilityContextAgent)
				}
			}
			host.Capabilities = normalizeToolNames(host.Capabilities)
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
	key, err := InvocationMCPKey(prep.AgentTeamsTaskID)
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
	providerHost := c.providerHost(prep.HostRef)
	readyAfter, previousHeartbeat := c.providerWorkerReadinessBaseline(ctx, providerHost)
	if providerHost != prep.HostRef || prep.Role != auth.RoleTaskManager && prep.Role != auth.RoleContext {
		if err := c.controller.EnsureWorkerReady(ctx, providerHost); err != nil {
			cleanupCtx, cancel := boundedCleanupContext(ctx)
			defer cancel()
			return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, nil))
		}
	}
	qwenPaw, err := c.waitForQwenPawReady(ctx, prep.HostRef, providerHost, readyAfter, previousHeartbeat)
	if err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, nil))
	}
	if err := c.pruneStaleInvocationMCP(ctx, qwenPaw, prep.HostRef, desired.Key); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, nil))
	}
	if err := qwenPaw.PruneInvocationMCPExcept(ctx, desired.Key); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, nil))
	}
	if err := qwenPaw.EnsureNativeProjectTools(ctx); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, qwenPaw))
	}
	if err := qwenPaw.InstallInvocationMCP(ctx, desired); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, qwenPaw))
	}
	if err := qwenPaw.WaitInvocationReady(ctx, desired.Key, desired.ExpectedTools); err != nil {
		cleanupCtx, cancel := boundedCleanupContext(ctx)
		defer cancel()
		return errors.Join(err, c.cleanupPreparationFailure(cleanupCtx, prep.HostRef, prep.InvocationID, qwenPaw))
	}
	return nil
}

// pruneStaleInvocationMCP removes only provider clients that the durable host
// slot history proves are no longer active. It runs before QwenPaw lists MCP
// clients because one expired historical bearer can otherwise block clean
// upstream QwenPaw during cold-start restoration. The current key is excluded
// defensively even though the store query already excludes active claims.
func (c *ProductionClient) pruneStaleInvocationMCP(ctx context.Context, qwenPaw *QwenPawAPI, hostRef, currentKey string) error {
	keys, err := c.slots.StaleMCPClientKeysByHost(ctx, hostRef)
	if err != nil {
		return err
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == currentKey || !isInvocationMCPKey(key) {
			continue
		}
		if err := qwenPaw.DeleteInvocationMCPIfPresent(ctx, key); err != nil {
			return err
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
		Spec:       taskflowSpecification(req.TaskID, req.Spec),
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

func taskflowSpecification(taskID, spec string) string {
	return "Read the complete authoritative task specification before acting: shared/tasks/" + taskID + "/spec.md\n" +
		"The Matrix assignment body is only a preview. Do not infer or omit truncated input.\n" +
		"Immediately call TeamHarness taskflow ack_task for task " + taskID + "; ack_task performs the authoritative filesync pull. Then read the returned specification or shared/tasks/" + taskID + "/spec.md. Do not inspect unrelated tasks or files.\n" +
		"For a Threadmill phase invocation, the complete native project workspace is shared/tasks/" + taskID + "/workspace/. Use your native file search, read, edit/write, and shell tools in that directory to inspect, implement, and test the task. Those native tools are expected; do not use them to access Coordination Graph or Context Graph storage. Graph reads and mutations remain available only through the Threadmill MCP tools named in the authoritative specification.\n" +
		"If the required Threadmill mutation succeeds, return your final response immediately; Runtime owns TeamHarness SUCCESS finalization with an empty deliverables list. Never put Threadmill artifact refs into TeamHarness deliverables. If no authoritative Threadmill terminal mutation can be submitted, call TeamHarness submit_task with BLOCKED or FAILED, the real error, and deliverables [].\n" +
		"A successful invocation-terminal Threadmill mutation may fence its bearer immediately. Do not make any Threadmill MCP confirmation call after that success.\n\n" + spec
}

func (c *ProductionClient) ReleaseHostSlot(ctx context.Context, taskID string, hostRef string) error {
	// Releasing one logical execution must never sleep its physical carrier.
	// A new claim can be reserved between an "idle" read and the controller
	// sleep call, which would terminate unrelated work on the same reusable
	// host. Capacity lifecycle is owned by explicit operator/controller policy,
	// not by an individual invocation cleanup.
	return c.slots.Release(ctx, taskID, hostRef)
}

func (c *ProductionClient) RevokeInvocation(ctx context.Context, hostRef string, invocationID kernel.InvocationID) error {
	claim, ok, err := c.slots.ByInvocation(ctx, hostRef, invocationID)
	if err != nil {
		return err
	}
	if !ok || claim.ClaimedAt.IsZero() {
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
		if invocationCarrierCleanupUnavailable(err) {
			return c.slots.MarkRevoked(ctx, claim.TaskID, hostRef)
		}
		return err
	}
	if err := qwenPaw.RevokeInvocationMCP(ctx, claim.MCPClientKey); err != nil {
		if invocationCarrierCleanupUnavailable(err) {
			return c.slots.MarkRevoked(ctx, claim.TaskID, hostRef)
		}
		return err
	}
	return c.slots.MarkRevoked(ctx, claim.TaskID, hostRef)
}

func invocationCarrierCleanupUnavailable(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "host not found") ||
		strings.Contains(message, "host missing") ||
		strings.Contains(message, "container not found") ||
		strings.Contains(message, "no such container") ||
		strings.Contains(message, "is not running") ||
		strings.Contains(message, "worker not found") ||
		strings.Contains(message, "worker is unavailable") ||
		strings.Contains(message, "carrier is unavailable") {
		return true
	}
	return kernel.IsCode(err, kernel.CodeNotFound) || kernel.IsCode(err, kernel.CodeExecutorUnavailable)
}

func (c *ProductionClient) FenceInvocation(ctx context.Context, hostRef string, invocationID kernel.InvocationID) error {
	claim, ok, err := c.slots.ByInvocation(ctx, hostRef, invocationID)
	if err != nil {
		return err
	}
	if !ok || claim.ClaimedAt.IsZero() || !claim.ReleasedAt.IsZero() {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams invocation MCP slot not found"}
	}
	// Runtime invocation status is the logical authority fence enforced by the
	// MCP tools/call guard. Keep the bearer and QwenPaw driver alive so the same
	// model run can finish native file work and TeamHarness submit_task. The
	// destructive token/driver revocation happens only in RevokeInvocation when
	// provider cleanup terminates the execution.
	return nil
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
	} else {
		// The dedicated dispatcher is an internal AgentTeams worker and can be
		// sleeping or stopped independently of the assigned execution host.
		// Wake it at the dispatch boundary and wait for its QwenPaw management
		// API before invoking TeamHarness; controller phase=Running alone is not
		// a process-readiness guarantee.
		readyAfter, previousHeartbeat := c.providerWorkerReadinessBaseline(ctx, hostRef)
		if err := c.controller.EnsureWorkerReady(ctx, hostRef); err != nil {
			return "", err
		}
		if _, err := c.waitForQwenPawReady(ctx, hostRef, hostRef, readyAfter, previousHeartbeat); err != nil {
			return "", err
		}
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
	if !claim.ReleasedAt.IsZero() {
		return nil
	}
	// Task state is authoritative in the assigned worker workspace after
	// delegation. The dispatcher only owns task creation and can lag behind the
	// worker's ack/submit files, so lifecycle actions must target the claimed
	// execution host.
	container, err := c.containers.ContainerForHost(ctx, claim.HostRef)
	if err != nil {
		return err
	}
	_, err = c.taskflow.Call(ctx, container, TaskflowCall{Action: "cancel_task", TaskID: taskID, Reason: reason})
	if kernel.IsCode(err, kernel.CodeNotFound) || kernel.IsCode(err, kernel.CodeStaleCommand) {
		return nil
	}
	return err
}

// CompleteTask closes only AgentTeams/TeamHarness provider bookkeeping after
// Runtime has already accepted the authoritative invocation-terminal result.
// Threadmill artifact refs intentionally never enter TeamHarness deliverables.
func (c *ProductionClient) CompleteTask(ctx context.Context, taskID string, summary string) error {
	claim, ok, err := c.slots.ByTaskID(ctx, taskID)
	if err != nil {
		return err
	}
	if !ok {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "AgentTeams task slot not found"}
	}
	if !claim.ReleasedAt.IsZero() {
		return nil
	}
	if err := c.ensureExecutionCarrierReady(ctx, claim.HostRef); err != nil {
		return err
	}
	container, err := c.containers.ContainerForHost(ctx, claim.HostRef)
	if err != nil {
		return err
	}
	_, err = c.taskflow.Call(ctx, container, TaskflowCall{
		Action: "submit_task", Role: "worker", TaskID: taskID,
		Status: "SUCCESS", Summary: strings.TrimSpace(summary), Deliverables: []string{},
	})
	if kernel.IsCode(err, kernel.CodeNotFound) || kernel.IsCode(err, kernel.CodeStaleCommand) {
		return nil
	}
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
	if !claim.ReleasedAt.IsZero() {
		// Release is permitted only after invocation authority was revoked (or a
		// host fence made it unreachable). It is therefore durable proof that the
		// provider execution is no longer live. Report that terminal lifecycle
		// state so a post-crash cleanup pass can finish marking the adapter record
		// terminated instead of retrying a task check against a slot that cannot
		// be reactivated.
		return TaskCheck{Task: TaskSnapshot{
			TaskID:  claim.TaskID,
			HostRef: claim.HostRef,
			Status:  "released",
		}}, nil
	}
	if c.taskflowHostRef == "" {
		// Compatibility path for deployments without a dedicated TeamHarness
		// carrier: a controller-managed execution worker may have been put to
		// sleep while Threadmill still holds a durable execution claim. Checking
		// provider finalization only needs TeamHarness; wake the claimed carrier
		// without reinstalling the already-fenced invocation MCP or reactivating
		// its one-lifetime bearer.
		if err := c.ensureExecutionCarrierReady(ctx, claim.HostRef); err != nil {
			return TaskCheck{}, err
		}
	}
	// When configured, check_task must run through the dedicated taskflow
	// carrier. TeamHarness check_task pulls the shared task directory before
	// reading status; running that pull in the claimed execution worker can
	// overwrite an in-progress phase workspace with older object-store state.
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

func (c *ProductionClient) HostActivity(ctx context.Context, hostRef string) (HostActivity, error) {
	active, err := c.slots.ActiveByHost(ctx, strings.TrimSpace(hostRef))
	if err != nil {
		return HostActivity{}, err
	}
	if len(active) > 0 {
		// See CheckTask: activity observation must recover the physical carrier,
		// not the invocation credential. This lets cleanup distinguish a live
		// model turn from an abandoned provider task after worker sleep/restart.
		if err := c.ensureExecutionCarrierReady(ctx, hostRef); err != nil {
			return HostActivity{}, err
		}
	}
	qwenPaw, err := c.qwenPaw.ForHost(ctx, strings.TrimSpace(hostRef))
	if err != nil {
		return HostActivity{}, err
	}
	return qwenPaw.AgentActivity(ctx)
}

func (c *ProductionClient) ensureExecutionCarrierReady(ctx context.Context, hostRef string) error {
	providerHost := c.providerHost(strings.TrimSpace(hostRef))
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		if host.Ref != providerHost {
			continue
		}
		// A durable active slot is also the liveness lease for its physical
		// carrier.  EnsureWorkerReady refreshes the controller's LastActiveAt
		// even when the container already reports Running; skipping it here lets
		// auto-sleep terminate a model turn that is still actively owned by
		// Threadmill.
		return c.controller.EnsureWorkerReady(ctx, providerHost)
	}
	return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams execution carrier is unavailable", Recoverable: true}
}

// RecoverExecutionHost restores the already-claimed physical carrier and its
// invocation-scoped MCP after a Threadmill or Docker restart. It does not
// create a task, replace a claim, or mutate either Threadmill graph; the
// durable execution and host-slot records remain the authority.
func (c *ProductionClient) RecoverExecutionHost(ctx context.Context, prep HostPreparation) error {
	if err := validateHostPreparation(prep); err != nil {
		return err
	}
	claim, ok, err := c.slots.ByInvocation(ctx, prep.HostRef, prep.InvocationID)
	if err != nil {
		return err
	}
	if !ok || claim.TaskID != prep.AgentTeamsTaskID || !claim.ReleasedAt.IsZero() {
		return kernel.Error{Code: kernel.CodeNotFound, Message: "active AgentTeams invocation slot not found"}
	}
	hostRef := strings.TrimSpace(prep.HostRef)
	providerHost := c.providerHost(hostRef)
	if !safeProviderID(providerHost) {
		return kernel.InvalidArgument("AgentTeams execution host is invalid")
	}
	readyAfter, previousHeartbeat := c.providerWorkerReadinessBaseline(ctx, providerHost)
	if err := c.controller.EnsureWorkerReady(ctx, providerHost); err != nil {
		return err
	}
	qwenPaw, err := c.waitForQwenPawReady(ctx, hostRef, providerHost, readyAfter, previousHeartbeat)
	if err != nil {
		return err
	}
	material, err := c.mcpResolver.ResolveInvocationMCP(ctx, prep)
	if err != nil {
		return err
	}
	key, err := InvocationMCPKey(prep.AgentTeamsTaskID)
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
		return err
	}
	if err := c.pruneStaleInvocationMCP(ctx, qwenPaw, hostRef, desired.Key); err != nil {
		return err
	}
	if err := qwenPaw.EnsureNativeProjectTools(ctx); err != nil {
		return err
	}
	if err := qwenPaw.InstallInvocationMCP(ctx, desired); err != nil {
		return err
	}
	return qwenPaw.WaitInvocationReady(ctx, desired.Key, desired.ExpectedTools)
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
	// The store serializes this check with claims and only clears a completed
	// fence when no active slot remains. The physical worker is expected to be
	// stopped or sleeping after a force-stop, so requiring it to be running here
	// creates a deadlock: Claim needs the fence cleared, while wake happens only
	// after Claim. Clear the durable fence first; PrepareHost then performs the
	// authoritative EnsureWorkerReady/readiness sequence.
	_, err := c.slots.ClearHostFenceIfReusable(ctx, hostRef)
	return err
}

func (c *ProductionClient) sleepIdlePhysicalWorker(ctx context.Context, hostRef string) error {
	hostRef = strings.TrimSpace(hostRef)
	if hostRef == "" || c.providerHost(hostRef) != hostRef || hostRef == c.taskflowHostRef {
		return nil
	}
	active, err := c.slots.ActiveByHost(ctx, hostRef)
	if err != nil {
		return err
	}
	if len(active) > 0 {
		return nil
	}
	return c.controller.SleepWorker(ctx, hostRef)
}

func (c *ProductionClient) waitForQwenPawReady(ctx context.Context, hostRef, providerHost string, observedAfter, previousHeartbeat time.Time) (*QwenPawAPI, error) {
	waitCtx := ctx
	cancel := func() {}
	if _, ok := waitCtx.Deadline(); !ok {
		waitCtx, cancel = context.WithTimeout(ctx, 90*time.Second)
	}
	defer cancel()

	var lastErr error
	for {
		if err := c.providerWorkerObservedReady(waitCtx, providerHost, observedAfter, previousHeartbeat); err == nil {
			qwenPaw, err := c.qwenPaw.ForHost(waitCtx, hostRef)
			if err == nil {
				if readyErr := qwenPaw.WaitStartupReady(waitCtx); readyErr == nil {
					return qwenPaw, nil
				} else {
					lastErr = readyErr
				}
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, waitCtx.Err()
		case <-timer.C:
		}
	}
}

func (c *ProductionClient) providerWorkerObservedReady(ctx context.Context, providerHost string, observedAfter, previousHeartbeat time.Time) error {
	if !safeProviderID(providerHost) {
		return kernel.InvalidArgument("AgentTeams worker name is invalid")
	}
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return err
	}
	for _, host := range hosts {
		if host.Ref != providerHost {
			continue
		}
		if !hostObservedReadyOrRunning(host) {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams worker is not running", Recoverable: true}
		}
		if host.Kind != HostWorker {
			return nil
		}
		if host.LastHeartbeat.IsZero() || time.Since(host.LastHeartbeat) > productionWorkerReadyHeartbeatMaxAge {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams worker has not reported a fresh heartbeat", Recoverable: true}
		}
		if !observedAfter.IsZero() && host.LastHeartbeat.Before(observedAfter.Add(-productionWorkerReadyHeartbeatSkew)) {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams worker heartbeat predates the readiness request", Recoverable: true}
		}
		if !previousHeartbeat.IsZero() && !host.LastHeartbeat.After(previousHeartbeat) {
			return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams worker has not reported a heartbeat for this readiness request", Recoverable: true}
		}
		return nil
	}
	return kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams worker is unavailable", Recoverable: true}
}

// providerWorkerReadinessBaseline returns a heartbeat only when the controller
// says the carrier must be woken. A running carrier with a fresh heartbeat may
// serve another invocation immediately; a sleeping/stopped carrier must emit a
// different heartbeat after EnsureWorkerReady before it is trusted.
func (c *ProductionClient) providerWorkerReadinessBaseline(ctx context.Context, providerHost string) (time.Time, time.Time) {
	hosts, err := c.controller.ListHosts(ctx)
	if err != nil {
		return time.Time{}, time.Time{}
	}
	for _, host := range hosts {
		if host.Ref == providerHost && host.Kind == HostWorker && !hostObservedReadyOrRunning(host) {
			return time.Now().UTC(), host.LastHeartbeat
		}
	}
	// A carrier that is already Running/Ready may serve immediately when its
	// existing heartbeat is fresh. Requiring that heartbeat to be newer than
	// this request would couple every dispatch to the worker's periodic
	// heartbeat interval. Sleeping/Stopped carriers still take the branch above
	// and must prove a post-wake heartbeat before their API is trusted.
	return time.Time{}, time.Time{}
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
	if _, err := InvocationMCPKey(prep.AgentTeamsTaskID); err != nil {
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
