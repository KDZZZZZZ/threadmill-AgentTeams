package agentteams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/artifacts"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/phaseagent"
)

// ControllerReprovisioner uses the public AgentTeams controller APIs for the
// Worker and MCP credential portions of M4-D. The controller remains the
// owner of the Worker CR, runtime.yaml projection, and private header
// resolution; Threadmill never writes runtime.yaml directly.
//
// BearerToken is controller-client configuration, never a worker credential.
// It is deliberately excluded from JSON and from returned Runtime records.
type ControllerReprovisioner struct {
	BaseURL       string
	BearerToken   string `json:"-"`
	HTTPClient    *http.Client
	Model         string
	ModelProvider string
	Runtime       string
	Image         string
	PollInterval  time.Duration
}

// RehydratedTeamHarnessTaskActivator bridges Runtime's M4-D task port to the
// existing TeamHarness taskflow client. TeamHarness remains authoritative for
// the observed assignment/acknowledgement state; this adapter neither submits
// the task nor derives a PhaseOutput from it.
type RehydratedTeamHarnessTaskActivator struct {
	Controller   *ControllerReprovisioner
	Taskflow     TaskflowClient
	ProjectID    string
	PollInterval time.Duration
}

// RehydratedHostPackageMaterializer reuses the established HostEnvelope
// authority, then projects only agent-visible logical material. It does not
// expose the envelope's trusted MCP binding or physical workspace root.
type RehydratedHostPackageMaterializer struct {
	Envelopes HostEnvelopeResolver
}

func (m RehydratedHostPackageMaterializer) MaterializeRehydratedExecution(ctx context.Context, plan runtime.RehydrationPlan) (runtime.RehydratedExecutionPackage, error) {
	if m.Envelopes == nil {
		return runtime.RehydratedExecutionPackage{}, errors.New("host envelope resolver is required")
	}
	envelope, err := m.Envelopes.ResolveHostEnvelope(ctx, plan.Execution)
	if err != nil {
		return runtime.RehydratedExecutionPackage{}, err
	}
	if envelope.BindingRef != plan.NewBindingRef {
		return runtime.RehydratedExecutionPackage{}, errors.New("host envelope binding does not match rehydration plan")
	}
	return runtime.RehydratedExecutionPackage{
		TaskID: plan.TaskID, InvocationID: plan.InvocationID, Generation: plan.Generation,
		ExecutionEpoch: plan.NextExecutionEpoch, Endpoint: plan.Endpoint, BindingRef: plan.NewBindingRef,
		InputRevision: plan.NewInputRevision, Inputs: plan.Inputs,
		NewlyDelivered: append([]phaseagent.InputDelivery(nil), plan.NewlyDelivered...),
		TaskContract:   envelope.TaskContract, PhaseInstruction: envelope.PhaseInstruction,
		Context:      runtime.AgentVisibleContext{SliceRef: plan.Context.SliceRef, BaselineRef: plan.Context.BaselineRef, Content: envelope.Context.Content},
		TaskMemory:   envelope.TaskMemory,
		Workspace:    runtime.AgentVisibleWorkspace{Ref: plan.Workspace.Ref, Revision: plan.Workspace.Revision, AllowedDirs: append([]string(nil), envelope.Workspace.AllowedDirs...)},
		ArtifactRefs: append([]artifacts.ArtifactRef(nil), plan.ArtifactRefs...),
		EventRefs:    append([]string(nil), plan.EventRefs...), EvidenceRefs: append([]string(nil), plan.EvidenceRefs...),
	}, nil
}

var _ runtime.TeamHarnessTaskProvisioner = (*RehydratedTeamHarnessTaskActivator)(nil)

func (a *RehydratedTeamHarnessTaskActivator) CreateTeamHarnessTask(ctx context.Context, request runtime.TeamHarnessTaskRequest) (runtime.TeamHarnessTask, error) {
	if a == nil || a.Controller == nil || a.Taskflow == nil {
		return runtime.TeamHarnessTask{}, errors.New("controller and taskflow are required for rehydrated task activation")
	}
	if request.Worker.Name == "" || request.Plan.InvocationID == "" || request.Plan.Generation <= 0 || request.Plan.NextExecutionEpoch <= 0 {
		return runtime.TeamHarnessTask{}, errors.New("rehydration worker and epoch identity are required")
	}
	roomID, err := a.Controller.WorkerRoomID(ctx, request.Worker.Name)
	if err != nil {
		return runtime.TeamHarnessTask{}, err
	}
	assignee, err := a.Controller.WorkerMatrixUserID(ctx, request.Worker.Name)
	if err != nil {
		return runtime.TeamHarnessTask{}, err
	}
	projectID := a.ProjectID
	if projectID == "" {
		projectID = "threadmill-" + taskflowSafeID(request.Plan.TaskID)
	}
	// TeamHarness's official delegate_task API intentionally receives a caller
	// taskId. This stable Threadmill-generated physical ID includes the epoch;
	// check_task then returns the persisted authoritative ID as activation
	// evidence. It is not a fixture-generated placeholder.
	taskID := fmt.Sprintf("tm-phase-%s-g%d-e%d", taskflowSafeID(request.Plan.InvocationID), request.Plan.Generation, request.Plan.NextExecutionEpoch)
	activation, err := (TaskflowActivationObserver{Taskflow: a.Taskflow, PollInterval: a.PollInterval}).DelegateAndObserveAcceptance(ctx, TeamHarnessDelegateTaskRequest{
		ProjectID: projectID,
		TaskID:    taskID,
		RoomID:    roomID,
		Assignee:  assignee,
		Title:     "Threadmill rehydrated " + string(request.Plan.Endpoint.EndpointID),
		Spec:      rehydratedTaskSpec(request),
	})
	if err != nil {
		a.cancelUnacceptedTask(ctx, taskID)
		return runtime.TeamHarnessTask{}, err
	}
	if !activation.Acknowledged {
		a.cancelUnacceptedTask(ctx, taskID)
		return runtime.TeamHarnessTask{}, errors.New("worker did not acknowledge the rehydrated agent-start package")
	}
	digest, err := runtime.RehydratedExecutionPackageDigest(request.Package)
	if err != nil {
		return runtime.TeamHarnessTask{}, fmt.Errorf("digest rehydrated execution package: %w", err)
	}
	// QwenPaw's production Matrix channel keys a fresh chat as
	// "matrix:<room-id>". The room ID comes from Controller status; it is not a
	// Threadmill-synthesized success URI.
	sessionRef := "matrix:" + roomID
	return runtime.TeamHarnessTask{
		ID: activation.TaskID, AssignedTo: request.Worker.Name, Status: string(activation.Status), Acknowledged: true,
		AgentSessionRef: sessionRef, AgentPackageDigest: digest, ObservedAt: activation.ObservedAt,
	}, nil
}

func (a *RehydratedTeamHarnessTaskActivator) cancelUnacceptedTask(ctx context.Context, taskID string) {
	if a == nil || a.Taskflow == nil || taskID == "" {
		return
	}
	if canceller, ok := a.Taskflow.(TaskflowCanceller); ok {
		_ = canceller.CancelTask(ctx, taskID, "rehydrated agent-start was not acknowledged")
	}
}

func (a *RehydratedTeamHarnessTaskActivator) CancelTeamHarnessTask(ctx context.Context, task runtime.TeamHarnessTask) error {
	if a == nil || a.Taskflow == nil || task.ID == "" {
		return nil
	}
	canceller, ok := a.Taskflow.(TaskflowCanceller)
	if !ok {
		return errors.New("taskflow client does not support cancellation")
	}
	return canceller.CancelTask(ctx, task.ID, "rehydration activation rollback")
}

// CompleteTeamHarnessTask reclaims the transient AgentTeams task after the
// formal Threadmill PhaseOutput has been accepted. This is normal completion,
// not rehydration rollback, even though taskflow uses the same cancel action.
func (a *RehydratedTeamHarnessTaskActivator) CompleteTeamHarnessTask(ctx context.Context, task runtime.TeamHarnessTask) error {
	if a == nil || a.Taskflow == nil || task.ID == "" {
		return nil
	}
	canceller, ok := a.Taskflow.(TaskflowCanceller)
	if !ok {
		return errors.New("taskflow client does not support completion cleanup")
	}
	return canceller.CancelTask(ctx, task.ID, "Threadmill PhaseOutput accepted")
}

func (c *ControllerReprovisioner) WorkerRoomID(ctx context.Context, workerName string) (string, error) {
	status, err := c.workerStatus(ctx, workerName)
	if err != nil {
		return "", err
	}
	if status.RoomID == "" {
		return "", errors.New("controller worker status omitted roomID")
	}
	return status.RoomID, nil
}

func (c *ControllerReprovisioner) WorkerMatrixUserID(ctx context.Context, workerName string) (string, error) {
	status, err := c.workerStatus(ctx, workerName)
	if err != nil {
		return "", err
	}
	if status.MatrixUserID == "" {
		return "", errors.New("controller worker status omitted matrixUserID")
	}
	return status.MatrixUserID, nil
}

func rehydratedTaskSpec(request runtime.TeamHarnessTaskRequest) string {
	encoded, err := json.MarshalIndent(request.Package, "", "  ")
	if err != nil {
		return ""
	}
	digest, err := runtime.RehydratedExecutionPackageDigest(request.Package)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("# Threadmill rehydrated phase\n\nThe following package is a Runtime-authorized, agent-visible projection. Treat formal inputs and references as read-only authority.\n\n- Package digest: `%s`\n- This is a fresh physical session. Do not load or infer any previous provider conversation or hidden reasoning.\n\n```json\n%s\n```\n\n## Required continuation handshake\n\n1. Call TeamHarness `ack_task` and read the complete task specification returned by that tool.\n2. Parse the JSON package above and verify its digest is `%s`.\n3. Call Threadmill `runtime.confirmPackageConsumption` with `package_digest=%s` and `consumed=true`. Runtime binds the authoritative Matrix session identity server-side.\n4. Begin continuation work only after that call succeeds.\n\n- TeamHarness acknowledgement is activation evidence only; it is not package-consumption evidence.\n- TeamHarness submission is execution evidence only; it is not PhaseOutput.\n- Submit formal output only through `agent.submitPhaseOutput`.\n", digest, encoded, digest, digest)
}

var (
	_ runtime.MCPCredentialProvisioner = (*ControllerReprovisioner)(nil)
	_ runtime.WorkerProvisioner        = (*ControllerReprovisioner)(nil)
	_ runtime.MCPClientCleaner         = (*ControllerReprovisioner)(nil)
	_ runtime.WorkerRuntimeGate        = (*ControllerReprovisioner)(nil)
)

type controllerCredentialView struct {
	ID         string `json:"id"`
	WorkerName string `json:"workerName"`
	HeaderName string `json:"headerName"`
	State      string `json:"state"`
}

func (c *ControllerReprovisioner) CreateMCPCredential(ctx context.Context, request runtime.MCPCredentialRequest) (runtime.MCPCredentialBinding, error) {
	if request.WorkerName == "" || request.HeaderName == "" || request.Token == "" {
		return runtime.MCPCredentialBinding{}, errors.New("worker name, header name, and execution token are required")
	}
	var view controllerCredentialView
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/mcp-credentials", map[string]string{
		"workerName": request.WorkerName, "headerName": request.HeaderName, "secretValue": request.Token,
	}, &view); err != nil {
		return runtime.MCPCredentialBinding{}, err
	}
	if view.ID == "" || view.WorkerName != request.WorkerName || view.HeaderName != request.HeaderName || view.State != "active" {
		return runtime.MCPCredentialBinding{}, errors.New("controller returned an invalid MCP credential binding")
	}
	return runtime.MCPCredentialBinding{Ref: view.ID, WorkerName: view.WorkerName}, nil
}

func (c *ControllerReprovisioner) RevokeMCPCredential(ctx context.Context, credential runtime.MCPCredentialBinding) error {
	if credential.Ref == "" {
		return nil
	}
	return c.doJSON(ctx, http.MethodPost, "/api/v1/mcp-credentials/"+credential.Ref+"/revoke", nil, nil)
}

func (c *ControllerReprovisioner) ProvisionWorker(ctx context.Context, request runtime.WorkerProvisionRequest) (runtime.ProvisionedWorker, error) {
	if request.WorkerName == "" || request.CredentialRef == "" || request.MCPName == "" || request.MCPURL == "" || request.Transport == "" {
		return runtime.ProvisionedWorker{}, errors.New("worker, credential, and MCP desired state are required")
	}
	if c.Model == "" || c.Runtime == "" {
		return runtime.ProvisionedWorker{}, errors.New("controller worker model and runtime are required")
	}
	body := map[string]any{
		"name": request.WorkerName, "workerName": request.WorkerName,
		"model": c.Model, "runtime": c.Runtime,
		"mcpServers": []map[string]string{{"name": request.MCPName, "url": request.MCPURL, "transport": request.Transport, "credentialBindingRef": request.CredentialRef}},
	}
	if c.ModelProvider != "" {
		body["modelProvider"] = c.ModelProvider
	}
	if c.Image != "" {
		body["image"] = c.Image
	}
	var created controllerWorkerStatus
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/workers", body, &created); err != nil {
		return runtime.ProvisionedWorker{}, err
	}
	if created.Name != request.WorkerName {
		return runtime.ProvisionedWorker{}, errors.New("controller returned a mismatched worker")
	}
	if err := c.ensureWorkerReady(ctx, request.WorkerName); err != nil {
		return runtime.ProvisionedWorker{}, err
	}
	status, err := c.waitForDesiredRuntimeConfig(ctx, request.WorkerName)
	if err != nil {
		return runtime.ProvisionedWorker{}, err
	}
	generation, err := parseGeneration(status.RuntimeConfig.DesiredGeneration)
	if err != nil {
		return runtime.ProvisionedWorker{}, fmt.Errorf("controller desired generation: %w", err)
	}
	return runtime.ProvisionedWorker{ID: status.Name, Name: status.Name, RuntimeGeneration: generation, MCPClientID: request.MCPName}, nil
}

func (c *ControllerReprovisioner) ensureWorkerReady(ctx context.Context, workerName string) error {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	path := "/api/v1/workers/" + workerName + "/ensure-ready"
	for {
		err := c.doJSON(ctx, http.MethodPost, path, nil, nil)
		if err == nil {
			return nil
		}
		// The embedded API persists Worker desired state asynchronously after
		// create. Only this transient not-found window is retryable; auth,
		// validation, and server errors remain fail-fast.
		if !strings.Contains(err.Error(), "failed with HTTP 404") {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// waitForDesiredRuntimeConfig accounts for the Controller's asynchronous
// Worker CR -> runtime.yaml projection. A successful create/ensure-ready call
// does not make desiredGeneration synchronously observable, and treating an
// empty value as a malformed response races normal reconciliation.
func (c *ControllerReprovisioner) waitForDesiredRuntimeConfig(ctx context.Context, workerName string) (controllerWorkerStatus, error) {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	for {
		status, err := c.workerStatus(ctx, workerName)
		if err != nil {
			return controllerWorkerStatus{}, err
		}
		if status.RuntimeConfig.DesiredGeneration != "" {
			return status, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return controllerWorkerStatus{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *ControllerReprovisioner) DeleteWorker(ctx context.Context, worker runtime.ProvisionedWorker) error {
	if worker.Name == "" {
		return nil
	}
	return c.doJSON(ctx, http.MethodDelete, "/api/v1/workers/"+worker.Name, nil, nil)
}

// CleanupWorkerMCP has no separate controller endpoint: deleting the Worker
// removes the controller desired state that projects the runtime MCP client.
// It remains a distinct runtime port because backends with asynchronous worker
// deletion can later provide an explicit client-removal acknowledgement.
func (*ControllerReprovisioner) CleanupWorkerMCP(context.Context, runtime.ProvisionedWorker) error {
	return nil
}

func (c *ControllerReprovisioner) WaitForRuntimeReady(ctx context.Context, worker runtime.ProvisionedWorker) (runtime.RuntimeReadback, error) {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	for {
		status, err := c.workerStatus(ctx, worker.Name)
		if err != nil {
			return runtime.RuntimeReadback{}, err
		}
		// Heartbeat/readback is also asynchronous: an empty applied generation
		// means the worker has not yet reported its first runtime reconciliation,
		// not that the Controller returned malformed state.
		if status.RuntimeConfig.DesiredGeneration == "" || status.RuntimeConfig.AppliedGeneration == "" {
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return runtime.RuntimeReadback{}, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		readback, err := controllerReadback(status, worker)
		if err != nil {
			return runtime.RuntimeReadback{}, err
		}
		if readback.BackendRunning && readback.APIVersionReady && readback.MCPApplied && readback.DesiredGeneration == worker.RuntimeGeneration && readback.AppliedGeneration == worker.RuntimeGeneration {
			return readback, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return runtime.RuntimeReadback{}, ctx.Err()
		case <-timer.C:
		}
	}
}

type controllerWorkerStatus struct {
	Name           string `json:"name"`
	MatrixUserID   string `json:"matrixUserID"`
	RoomID         string `json:"roomID"`
	Phase          string `json:"phase"`
	ContainerState string `json:"containerState"`
	RuntimeConfig  struct {
		DesiredGeneration string `json:"desiredGeneration"`
		AppliedGeneration string `json:"appliedGeneration"`
		MCPServers        []struct {
			Name        string   `json:"name"`
			Applied     bool     `json:"applied"`
			HeaderNames []string `json:"headerNames"`
			Removed     bool     `json:"removed"`
			Error       string   `json:"error"`
		} `json:"mcpServers"`
	} `json:"runtimeConfig"`
}

func (c *ControllerReprovisioner) workerStatus(ctx context.Context, workerName string) (controllerWorkerStatus, error) {
	var status controllerWorkerStatus
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/workers/"+workerName+"/status", nil, &status); err != nil {
		return controllerWorkerStatus{}, err
	}
	if status.Name != workerName {
		return controllerWorkerStatus{}, errors.New("controller returned a mismatched worker status")
	}
	return status, nil
}

func controllerReadback(status controllerWorkerStatus, worker runtime.ProvisionedWorker) (runtime.RuntimeReadback, error) {
	desired, err := parseGeneration(status.RuntimeConfig.DesiredGeneration)
	if err != nil {
		return runtime.RuntimeReadback{}, err
	}
	applied, err := parseGeneration(status.RuntimeConfig.AppliedGeneration)
	if err != nil {
		return runtime.RuntimeReadback{}, err
	}
	readback := runtime.RuntimeReadback{
		BackendRunning: strings.EqualFold(status.ContainerState, "running"),
		// The controller only marks a worker Ready after the worker heartbeat
		// and runtime config report have both been accepted.
		APIVersionReady:   strings.EqualFold(status.Phase, "ready"),
		DesiredGeneration: desired, AppliedGeneration: applied, MCPClientID: worker.MCPClientID,
	}
	for _, mcp := range status.RuntimeConfig.MCPServers {
		if mcp.Name == worker.MCPClientID {
			readback.MCPApplied = mcp.Applied && !mcp.Removed && mcp.Error == ""
			readback.HeaderNames = append([]string(nil), mcp.HeaderNames...)
			break
		}
	}
	return readback, nil
}

func parseGeneration(value string) (int64, error) {
	if value == "" {
		return 0, errors.New("missing generation")
	}
	return strconv.ParseInt(value, 10, 64)
}

func (c *ControllerReprovisioner) doJSON(ctx context.Context, method, path string, payload any, output any) error {
	if c == nil || c.BaseURL == "" {
		return errors.New("AgentTeams controller base URL is required")
	}
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("AgentTeams controller %s %s failed with HTTP %d", method, path, response.StatusCode)
	}
	if output != nil && response.StatusCode != http.StatusNoContent {
		return json.NewDecoder(response.Body).Decode(output)
	}
	return nil
}
