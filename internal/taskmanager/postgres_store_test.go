package taskmanager

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/postgres"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresStoreRealDecisionContractReplyRestartAndAuthorization(t *testing.T) {
	db := openTaskManagerPostgres(t)
	defer db.Close()
	ctx := context.Background()

	coordStore := coordination.NewPostgresStore(db)
	store := NewPostgresStore(db, projectID, coordStore)
	graph := coordination.NewTaskManagerGraph(taskManagerPrincipal(), coordStore, coordStore, kernel.NewMemoryIdempotencyStore())
	manager := NewManager(Options{
		ProjectID: projectID,
		Graph:     graph,
		Decisions: store,
		Contracts: store,
		Replies:   store,
	})

	requirement := RequirementInput{
		InputRef:    "pg-requirement-1",
		TaskID:      "pg-task-a",
		ContractRef: "contract://pg-task-a",
		Requirement: Requirement{Text: "ship pg task", Goal: "persist task manager state"},
	}
	contractInput := TaskContract{
		TaskID:         "pg-task-a",
		ContractRef:    "contract://pg-task-a",
		DeliveryPolicy: DeliveryPolicyExternalDelivery,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "artifact://pg-task-a/plan-spec",
			coordination.EndpointExecute: "artifact://pg-task-a/execute-spec",
			coordination.EndpointVerify:  "artifact://pg-task-a/verify-spec",
		},
	}
	if err := store.PersistRequirementContract(ctx, requirement, contractInput); err != nil {
		t.Fatalf("PersistRequirementContract: %v", err)
	}
	secondRequirement := RequirementInput{
		InputRef:    requirement.InputRef,
		TaskID:      "pg-task-b",
		ContractRef: "contract://pg-task-b",
		Requirement: Requirement{Text: "ship pg task b", Goal: "persist a second task from the same manager input"},
	}
	secondContract := TaskContract{
		TaskID:         secondRequirement.TaskID,
		ContractRef:    secondRequirement.ContractRef,
		DeliveryPolicy: DeliveryPolicyExternalDelivery,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "artifact://pg-task-b/plan-spec",
			coordination.EndpointExecute: "artifact://pg-task-b/execute-spec",
			coordination.EndpointVerify:  "artifact://pg-task-b/verify-spec",
		},
	}
	if err := store.PersistRequirementContract(ctx, secondRequirement, secondContract); err != nil {
		t.Fatalf("PersistRequirementContract second task from same input: %v", err)
	}
	if stored, err := store.TaskContract(ctx, secondRequirement.TaskID); err != nil || stored.ContractRef != secondRequirement.ContractRef {
		t.Fatalf("second task contract from same input = %#v, %v", stored, err)
	}
	result, err := manager.HandleRequirement(ctx, requirement)
	if err != nil {
		t.Fatalf("HandleRequirement: %v", err)
	}
	if result.Status != ReplyAccepted || result.DecisionRef == "" || result.GraphRevision != 2 {
		t.Fatalf("result = %#v, want accepted revision 2 with decision ref", result)
	}
	if _, err := graph.ReplacePending(ctx, coordination.PendingSubgraph{RequestID: "not-authorized", BaseRevision: result.GraphRevision}); !kernel.IsCode(err, kernel.CodeForbidden) {
		t.Fatalf("unauthorized replacePending err = %v, want forbidden", err)
	}

	restartedStore := NewPostgresStore(db, projectID, coordination.NewPostgresStore(db))
	contract, err := restartedStore.TaskContract(ctx, "pg-task-a")
	if err != nil {
		t.Fatalf("restart TaskContract: %v", err)
	}
	if contract.DeliveryPolicy != DeliveryPolicyExternalDelivery || contract.PhaseSpecs[coordination.EndpointVerify] == "" {
		t.Fatalf("contract after restart = %#v, want persisted policy and phase specs", contract)
	}
	reply, ok, err := restartedStore.ManagerReply(ctx, "pg-requirement-1")
	if err != nil || !ok {
		t.Fatalf("restart ManagerReply ok=%v err=%v", ok, err)
	}
	if reply.DecisionRef != result.DecisionRef || reply.GraphRevision != result.GraphRevision {
		t.Fatalf("reply after restart = %#v, want original decision/revision", reply)
	}
	rejected, rejectErr := manager.HandlePhaseStopped(ctx, PhaseStoppedInput{
		InputRef:      "pg-stopped-rejected-1",
		Endpoint:      coordination.PhaseEndpointRef{TaskID: "pg-missing-task", EndpointID: coordination.EndpointExecute},
		CommandID:     "pg-stop-command-1",
		LeaseRef:      "pg-stop-lease-1",
		Generation:    1,
		EvidenceRefs:  []string{"artifact://pg-stop-evidence-1"},
		CheckpointRef: "checkpoint://pg-stop-1",
		NewBindingRef: "binding://pg-stop-1",
	})
	if !kernel.IsCode(rejectErr, kernel.CodeNotFound) || rejected.Status != ReplyRejected {
		t.Fatalf("rejected stopped event result=%#v err=%v, want persisted not-found rejection", rejected, rejectErr)
	}
	rejectedReply, ok, err := restartedStore.ManagerReply(ctx, "pg-stopped-rejected-1")
	if err != nil || !ok || rejectedReply.Status != ReplyRejected || rejectedReply.Reason != "stopped endpoint not found" {
		t.Fatalf("rejected stopped reply after restart=%#v ok=%v err=%v", rejectedReply, ok, err)
	}

	hold, err := manager.HandleManagerDecision(ctx, ManagerDecisionInput{
		InputRef:     "pg-manager-hold-1",
		Endpoint:     coordination.PhaseEndpointRef{TaskID: "pg-task-a", EndpointID: coordination.EndpointExecute},
		SeenRevision: result.GraphRevision,
	}, TaskManagerDecision{
		Action:    "held",
		TargetRef: "pg-task-a/execute",
		Reason:    "hold for postgres authorization test",
	})
	if err != nil {
		t.Fatalf("HandleManagerDecision hold: %v", err)
	}
	snapshot, err := graph.Snapshot(ctx, kernel.LatestRevision)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := findTestEndpoint(t, snapshot, coordination.PhaseEndpointRef{TaskID: "pg-task-a", EndpointID: coordination.EndpointExecute})
	if hold.DecisionRef == "" || endpoint.RunPolicy != coordination.RunHeld {
		t.Fatalf("hold result=%#v endpoint=%#v, want authorized transition to held", hold, endpoint)
	}
}

func TestPostgresStoreRealIdempotencyAndConflict(t *testing.T) {
	db := openTaskManagerPostgres(t)
	defer db.Close()
	ctx := context.Background()

	coordStore := coordination.NewPostgresStore(db)
	store := NewPostgresStore(db, projectID, coordStore)
	submission := DecisionSubmission{
		ProjectID:        projectID,
		InputRef:         "pg-decision-idempotent",
		ExpectedRevision: 1,
		Kind:             DecisionKindReplacePending,
		Decision: TaskManagerDecision{
			Action:    "replace_pending",
			TargetRef: "pg-task-idempotent",
			Reason:    "same semantic decision returns same ref",
		},
	}
	ref1, err := store.SubmitDecision(ctx, submission)
	if err != nil {
		t.Fatalf("SubmitDecision first: %v", err)
	}
	ref2, err := NewPostgresStore(db, projectID, coordination.NewPostgresStore(db)).SubmitDecision(ctx, submission)
	if err != nil {
		t.Fatalf("SubmitDecision restart: %v", err)
	}
	if ref1 != ref2 {
		t.Fatalf("decision refs = %q then %q, want stable across restart", ref1, ref2)
	}

	input := RequirementInput{
		InputRef:    "pg-input-conflict",
		TaskID:      "pg-task-conflict",
		ContractRef: "contract://pg-task-conflict",
		Requirement: Requirement{Text: "original"},
	}
	inputContract := TaskContract{
		TaskID:         "pg-task-conflict",
		ContractRef:    "contract://pg-task-conflict",
		DeliveryPolicy: DeliveryPolicyNonCodeArtifact,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "artifact://pg-task-conflict/plan",
			coordination.EndpointExecute: "artifact://pg-task-conflict/execute",
			coordination.EndpointVerify:  "artifact://pg-task-conflict/verify",
		},
	}
	if err := store.PersistRequirementContract(ctx, input, inputContract); err != nil {
		t.Fatalf("PersistRequirementContract first: %v", err)
	}
	if _, err := store.ResolveRequirementContract(ctx, input); err != nil {
		t.Fatalf("ResolveRequirementContract first: %v", err)
	}
	if _, err := store.ResolveRequirementContract(ctx, input); err != nil {
		t.Fatalf("ResolveRequirementContract repeat: %v", err)
	}
	if got, err := NewPostgresStore(db, projectID, coordination.NewPostgresStore(db)).ResolveRequirementContract(ctx, input); err != nil || got.ContractRef != input.ContractRef {
		t.Fatalf("ResolveRequirementContract replay without resubmitting contract got=%#v err=%v", got, err)
	}
	input.Requirement.Text = "changed"
	if _, err := store.ResolveRequirementContract(ctx, input); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("changed input err = %v, want idempotency conflict", err)
	}

	other := RequirementInput{
		InputRef:    "pg-input-contract-conflict",
		TaskID:      "pg-task-conflict",
		ContractRef: "contract://pg-task-conflict",
		Requirement: Requirement{Text: "other input"},
	}
	otherContract := TaskContract{
		TaskID:         "pg-task-conflict",
		ContractRef:    "contract://pg-task-conflict",
		DeliveryPolicy: DeliveryPolicyCodeMerge,
		PhaseSpecs: map[coordination.EndpointID]string{
			coordination.EndpointPlan:    "artifact://other/plan",
			coordination.EndpointExecute: "artifact://other/execute",
			coordination.EndpointVerify:  "artifact://other/verify",
		},
	}
	if err := store.PersistRequirementContract(ctx, other, otherContract); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("changed contract err = %v, want idempotency conflict", err)
	}

	missing := RequirementInput{
		InputRef:    "pg-input-missing-contract",
		TaskID:      "pg-task-missing-contract",
		ContractRef: "contract://pg-task-missing-contract",
		Requirement: Requirement{Text: "must not guess a contract"},
	}
	if _, err := store.ResolveRequirementContract(ctx, missing); !kernel.IsCode(err, kernel.CodeNotFound) {
		t.Fatalf("missing runtime-attested contract err = %v, want not found", err)
	}

	reply := ManagerReplyEvent{InputRef: "pg-reply-1", Status: ReplyAccepted, DecisionRef: ref1, GraphRevision: 1, Reason: "accepted once"}
	if err := store.AppendManagerReply(ctx, reply); err != nil {
		t.Fatalf("AppendManagerReply first: %v", err)
	}
	if err := store.AppendManagerReply(ctx, reply); err != nil {
		t.Fatalf("AppendManagerReply repeat: %v", err)
	}
	reply.Status = ReplyRejected
	if err := store.AppendManagerReply(ctx, reply); !kernel.IsCode(err, kernel.CodeIdempotencyConflict) {
		t.Fatalf("changed reply err = %v, want idempotency conflict", err)
	}
}

func openTaskManagerPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THREADMILL_PG_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://threadmill_test@127.0.0.1:5432/threadmill_test?sslmode=disable"
	}
	ctx := context.Background()
	baseDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := baseDB.PingContext(ctx); err != nil {
		baseDB.Close()
		t.Fatalf("postgres test database is required at %s: %v", dsn, err)
	}
	schema := fmt.Sprintf("taskmanager_it_%d", time.Now().UnixNano())
	if _, err := baseDB.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		baseDB.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = baseDB.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		baseDB.Close()
	})
	db, err := sql.Open("pgx", dsnWithTaskManagerSearchPath(t, dsn, schema))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := postgres.LoadMigrations(migrations.FS, migrations.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.NewMigrator(db).Apply(ctx, loaded); err != nil {
		db.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}

func dsnWithTaskManagerSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("options", "-c search_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func findTestEndpoint(t *testing.T, snapshot coordination.GraphSnapshot, ref coordination.PhaseEndpointRef) coordination.PhaseEndpoint {
	t.Helper()
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.Ref == ref {
			return endpoint
		}
	}
	t.Fatalf("missing endpoint %#v in %#v", ref, snapshot.Endpoints)
	return coordination.PhaseEndpoint{}
}
