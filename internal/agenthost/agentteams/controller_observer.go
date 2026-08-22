package agentteams

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
)

// ControllerPhysicalExecutionObserver composes only public, read-only
// AgentTeams status APIs. Controller client authorization is deployment
// configuration and never appears in the request, result, Runtime database,
// or event outbox.
type ControllerPhysicalExecutionObserver struct {
	Controller *ControllerReprovisioner
	Taskflow   TaskflowClient
	Now        func() time.Time
}

var _ runtime.PhysicalExecutionObserver = (*ControllerPhysicalExecutionObserver)(nil)

func (o *ControllerPhysicalExecutionObserver) Observe(ctx context.Context, request runtime.PhysicalExecutionObservationRequest) (runtime.PhysicalExecutionObservation, error) {
	if err := request.ValidateForObservation(); err != nil {
		return runtime.PhysicalExecutionObservation{}, err
	}
	if o == nil || o.Controller == nil || o.Taskflow == nil {
		return runtime.PhysicalExecutionObservation{}, errors.New("controller and taskflow observers are required")
	}
	now := time.Now().UTC()
	if o.Now != nil {
		now = o.Now().UTC()
	}
	result := runtime.PhysicalExecutionObservation{
		ObservedAt: now,
		Worker:     runtime.ObservedWorkerUnknown, Task: runtime.ObservedTaskUnknown,
		Runtime: runtime.ObservedRuntimeUnknown, MCP: runtime.ObservedMCPUnknown,
		Identity: runtime.ObservedCarrierIdentityUnknown,
	}

	status, err := o.Controller.workerStatus(ctx, request.WorkerName)
	switch {
	case err == nil:
		result = observeControllerWorker(result, request, status)
	case isNotFoundObservation(err):
		result.Worker = runtime.ObservedWorkerNotFound
		result.Runtime = runtime.ObservedRuntimeUnknown
		result.MCP = runtime.ObservedMCPUnknown
	default:
		result.SourceErrors = append(result.SourceErrors, runtime.ObservationSourceError{Source: "controller", Kind: observationErrorKind(err)})
	}

	task, taskErr := o.Taskflow.CheckTask(ctx, request.TeamHarnessTaskID)
	switch {
	case taskErr == nil:
		result.TaskID = task.TaskID
		result.Task = observedTaskState(task.Status)
		if task.TaskID != request.TeamHarnessTaskID || task.TaskID != expectedTaskID(request) {
			result.Identity = runtime.ObservedCarrierIdentityMismatch
		} else if result.Identity != runtime.ObservedCarrierIdentityMismatch && result.Worker != runtime.ObservedWorkerNotFound && result.Worker != runtime.ObservedWorkerUnknown {
			result.Identity = runtime.ObservedCarrierIdentityVerified
		}
	case isNotFoundObservation(taskErr):
		result.Task = runtime.ObservedTaskNotFound
	default:
		result.SourceErrors = append(result.SourceErrors, runtime.ObservationSourceError{Source: "taskflow", Kind: observationErrorKind(taskErr)})
	}
	return result, nil
}

func observeControllerWorker(result runtime.PhysicalExecutionObservation, request runtime.PhysicalExecutionObservationRequest, status controllerWorkerStatus) runtime.PhysicalExecutionObservation {
	result.WorkerName = status.Name
	result.Worker = observedWorkerState(status.Phase, status.ContainerState, status.State)
	if status.Name != request.WorkerName || (request.WorkerID != "" && request.WorkerID != status.Name) || status.Name != expectedWorkerName(request) {
		result.Identity = runtime.ObservedCarrierIdentityMismatch
	}
	if status.RoomID != "" {
		result.RoomRef = "matrix:" + status.RoomID
		if request.AgentSessionRef != "" && request.AgentSessionRef != result.RoomRef {
			result.Identity = runtime.ObservedCarrierIdentityMismatch
		}
	}
	desired, desiredOK := parseObservedGeneration(status.RuntimeConfig.DesiredGeneration)
	applied, appliedOK := parseObservedGeneration(status.RuntimeConfig.AppliedGeneration)
	result.DesiredGeneration, result.AppliedGeneration = desired, applied
	switch {
	case !desiredOK:
		result.Runtime = runtime.ObservedRuntimeUnknown
	case desired != request.DesiredGeneration:
		result.Runtime = runtime.ObservedRuntimeGenerationMismatch
		result.Identity = runtime.ObservedCarrierIdentityMismatch
	case !appliedOK || applied != desired || (request.AppliedGeneration > 0 && applied != request.AppliedGeneration):
		result.Runtime = runtime.ObservedRuntimeGenerationPending
	case desired == request.DesiredGeneration && applied == request.AppliedGeneration:
		result.Runtime = runtime.ObservedRuntimeApplied
	default:
		result.Runtime = runtime.ObservedRuntimeGenerationMismatch
		result.Identity = runtime.ObservedCarrierIdentityMismatch
	}
	for _, value := range status.RuntimeConfig.MCPServers {
		if value.Name != request.MCPClientID {
			continue
		}
		if value.Applied && !value.Removed && value.Error == "" {
			result.MCP = runtime.ObservedMCPApplied
		} else {
			result.MCP = runtime.ObservedMCPNotApplied
		}
		break
	}
	return result
}

func expectedWorkerName(request runtime.PhysicalExecutionObservationRequest) string {
	return fmt.Sprintf("tm-%s-g%d-e%d", request.InvocationID, request.Generation, request.ExecutionEpoch)
}

func expectedTaskID(request runtime.PhysicalExecutionObservationRequest) string {
	return fmt.Sprintf("tm-phase-%s-g%d-e%d", taskflowSafeID(request.InvocationID), request.Generation, request.ExecutionEpoch)
}

func parseObservedGeneration(value string) (int64, bool) {
	generation, err := parseGeneration(value)
	return generation, err == nil
}

func observedWorkerState(phase, container, desired string) runtime.ObservedWorkerState {
	value := strings.ToLower(strings.TrimSpace(phase + " " + container + " " + desired))
	switch {
	case strings.Contains(value, "fail"):
		return runtime.ObservedWorkerFailed
	case strings.Contains(value, "terminat"), strings.Contains(value, "stopping"), strings.Contains(value, "stopped"):
		return runtime.ObservedWorkerTerminating
	case strings.EqualFold(strings.TrimSpace(phase), "ready") && strings.EqualFold(strings.TrimSpace(container), "running"):
		return runtime.ObservedWorkerReady
	case strings.TrimSpace(phase) != "" || strings.TrimSpace(container) != "":
		return runtime.ObservedWorkerProvisioning
	default:
		return runtime.ObservedWorkerUnknown
	}
}

func observedTaskState(status TeamHarnessTaskStatus) runtime.ObservedTaskState {
	switch status {
	case TeamHarnessTaskAssigned, TeamHarnessTaskPrepared, TeamHarnessTaskWaiting:
		return runtime.ObservedTaskAssigned
	case TeamHarnessTaskInProgress:
		return runtime.ObservedTaskInProgress
	case TeamHarnessTaskSubmitted:
		return runtime.ObservedTaskCompleted
	case TeamHarnessTaskFailed, TeamHarnessTaskStopped:
		return runtime.ObservedTaskFailed
	case TeamHarnessTaskCancelled:
		return runtime.ObservedTaskCancelled
	default:
		return runtime.ObservedTaskUnknown
	}
}

func isNotFoundObservation(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "http 404") || strings.Contains(value, "not found")
}

func observationErrorKind(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		if networkErr.Timeout() {
			return "timeout"
		}
		return "network"
	}
	if strings.Contains(strings.ToLower(err.Error()), "http 5") {
		return "server_error"
	}
	return "unavailable"
}
