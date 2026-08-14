package app

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	runtimepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime"
	phasepkg "github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

func TestProductionIngressCompletesAlreadyAppliedPhaseEvaluationWithoutRedispatchAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-real")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)

	ref := coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointVerify}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := productionEndpoint(snapshot, ref)
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
		Generation: endpoint.Generation, Action: string(coordination.EndpointSubmitted),
	})
	if err != nil {
		t.Fatalf("submit endpoint: %v", err)
	}
	outputRef := "output://task-real/verify/already-applied"
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
		Generation: endpoint.Generation, Action: string(coordination.EndpointSatisfied),
		Result: coordination.PhaseResult{
			ID: "result-task-real-verify-already-applied", Endpoint: ref,
			BindingRef: endpoint.BindingRef, OutputRef: outputRef, Verdict: coordination.VerdictSatisfied,
		},
	})
	if err != nil {
		t.Fatalf("satisfy endpoint: %v", err)
	}

	boundary := productionPhaseEvaluationBoundary{
		SourceInputRef: "manager-input:phase-output-source",
		Endpoint:       ref,
		Generation:     endpoint.Generation,
		BindingRef:     endpoint.BindingRef,
		Output: productionPhaseOutputBoundary{
			OutputRef: outputRef,
			Receipt: phasepkg.OutputReceipt{
				Endpoint: ref, Generation: endpoint.Generation, BindingRef: endpoint.BindingRef,
			},
		},
	}
	payload, err := json.Marshal(boundary)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{}
	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	stored, err := ingress.DispatchTaskManagerFollowup(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "already-applied-evaluation",
		ConversationID: "runtime:already-applied", Body: "evaluate exact terminal output",
		Payload: payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &ref,
		TargetKind: "phase_evaluation", TargetRef: outputRef,
	})
	if err != nil {
		t.Fatalf("DispatchTaskManagerFollowup: %v", err)
	}
	if stored.Status != "completed" || dispatcher.calls != 0 {
		t.Fatalf("stored status=%q dispatcher calls=%d, want completed/0", stored.Status, dispatcher.calls)
	}
	var inputStatus, invocationStatus, disposition string
	if err := db.QueryRowContext(ctx, `SELECT status FROM production_manager_inputs WHERE invocation_id=$1`, stored.InvocationID).Scan(&inputStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runtime_invocations WHERE invocation_id=$1`, stored.InvocationID).Scan(&invocationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT disposition FROM production_conversation_entries
WHERE project_id=$1 AND manager_input_ref=$2 AND entry_kind='runtime'`, projectID, stored.InputRef).Scan(&disposition); err != nil {
		t.Fatal(err)
	}
	if inputStatus != "completed" || invocationStatus != string(runtimepkg.InvocationCompleted) || disposition != "already_applied" {
		t.Fatalf("input=%q invocation=%q disposition=%q", inputStatus, invocationStatus, disposition)
	}
}

func TestProductionIngressRetryCompletesFailedAlreadyAppliedPhaseEvaluationWithoutRotationAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	defer db.Close()
	projectID := kernel.ProjectID("project-real")
	graph := coordination.NewPostgresStore(db)
	seedProductionPhaseTask(t, ctx, db, projectID, graph)
	ref := coordination.PhaseEndpointRef{TaskID: "task-real", EndpointID: coordination.EndpointVerify}
	snapshot, err := graph.Latest(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := productionEndpoint(snapshot, ref)
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
		Generation: endpoint.Generation, Action: string(coordination.EndpointSubmitted),
	})
	if err != nil {
		t.Fatal(err)
	}
	outputRef := "output://task-real/verify/retry-already-applied"
	snapshot, err = graph.Transition(ctx, projectID, snapshot.Revision, coordination.GraphTransition{
		TargetKind: coordination.TargetPhaseEndpoint, Endpoint: ref,
		Generation: endpoint.Generation, Action: string(coordination.EndpointRejected),
		Result: coordination.PhaseResult{
			ID: "result-task-real-verify-retry-applied", Endpoint: ref,
			BindingRef: endpoint.BindingRef, OutputRef: outputRef, Verdict: coordination.VerdictRejected,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	boundary := productionPhaseEvaluationBoundary{
		SourceInputRef: "manager-input:phase-output-source-retry", Endpoint: ref,
		Generation: endpoint.Generation, BindingRef: endpoint.BindingRef,
		Output: productionPhaseOutputBoundary{OutputRef: outputRef, Receipt: phasepkg.OutputReceipt{
			Endpoint: ref, Generation: endpoint.Generation, BindingRef: endpoint.BindingRef,
		}},
	}
	payload, err := json.Marshal(boundary)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingProductionDispatcher{nextErr: errProductionDispatch}
	ingress, err := newProductionIngress(db, projectID, "room-real", productionPhaseTestAssembler(t), graph, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.setDispatcher(dispatcher); err != nil {
		t.Fatal(err)
	}
	stored, err := ingress.persistAndDispatch(ctx, productionInput{
		Kind: "phase_orchestration", RequestID: "retry-already-applied-evaluation",
		ConversationID: "runtime:retry-already-applied", Body: "retry exact terminal output",
		Payload: payload, SeenRevision: snapshot.Revision, SelectedEndpoint: &ref,
		TargetKind: "phase_evaluation", TargetRef: outputRef,
	})
	if err != nil {
		t.Fatalf("persistAndDispatch should complete before provider dispatch: %v", err)
	}
	if stored.Status != "completed" || dispatcher.calls != 0 {
		t.Fatalf("initial stored status=%q dispatcher calls=%d, want completed/0", stored.Status, dispatcher.calls)
	}
	if _, err := db.ExecContext(ctx, `UPDATE production_manager_inputs SET status='failed' WHERE project_id=$1 AND input_ref=$2`, projectID, stored.InputRef); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_invocations SET status='failed' WHERE invocation_id=$1`, stored.InvocationID); err != nil {
		t.Fatal(err)
	}
	if err := ingress.RetryFailedTaskManagerInputs(ctx); err != nil {
		t.Fatal(err)
	}
	var currentInvocation kernel.InvocationID
	var status string
	var attempt int
	if err := db.QueryRowContext(ctx, `SELECT invocation_id,status,dispatch_attempt FROM production_manager_inputs WHERE project_id=$1 AND input_ref=$2`, projectID, stored.InputRef).Scan(&currentInvocation, &status, &attempt); err != nil {
		t.Fatal(err)
	}
	if currentInvocation != stored.InvocationID || status != "completed" || attempt != 1 || dispatcher.calls != 0 {
		t.Fatalf("invocation=%q status=%q attempt=%d dispatches=%d, want original/completed/1/0", currentInvocation, status, attempt, dispatcher.calls)
	}
}
