package coordination

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresWriteGraphTablesUsesAuthoritativeSnapshotShape(t *testing.T) {
	rec := &recordingDBTX{}
	snapshot := GraphSnapshot{
		Revision: 2,
		Tasks: []Task{{
			ID:          "task-a",
			ContractRef: "contract://task-a",
			Outcome:     TaskActive,
		}},
		Endpoints: []PhaseEndpoint{
			endpoint("task-a", EndpointPlan),
			endpoint("task-a", EndpointExecute),
			endpoint("task-a", EndpointVerify),
		},
		Edges: []Edge{{
			From:          ref("task-a", EndpointPlan),
			To:            ref("task-a", EndpointExecute),
			Signal:        SignalPhaseSatisfied,
			RequiredBy:    RequiredByStart,
			ArtifactKinds: []string{`quote"kind`, `path\kind`},
			OnFalse:       OnFalseBlock,
		}},
		Blockers: []Blocker{{
			ID:         "blocker-a",
			Target:     ref("task-a", EndpointExecute),
			RequiredBy: RequiredByStart,
			OnFalse:    OnFalseBlock,
			State:      BlockerActive,
		}},
		Results: []PhaseResult{{
			ID:         "result-a",
			Endpoint:   ref("task-a", EndpointPlan),
			BindingRef: "binding://task-a/plan/1",
			OutputRef:  "artifact://task-a/plan",
			Verdict:    VerdictSubmitted,
		}},
	}

	if err := writeGraphTables(context.Background(), rec, "project-a", snapshot); err != nil {
		t.Fatal(err)
	}
	if len(rec.execs) != 5+len(snapshot.Tasks)+len(snapshot.Endpoints)+len(snapshot.Edges)+len(snapshot.Blockers)+len(snapshot.Results) {
		t.Fatalf("exec count = %d, want deletes plus all snapshot rows; execs=%#v", len(rec.execs), rec.execs)
	}
	if !strings.Contains(rec.execs[0].query, "DELETE FROM coordination_edges") ||
		!strings.Contains(rec.execs[4].query, "DELETE FROM coordination_tasks") {
		t.Fatalf("delete order = %#v, want relation tables cleared before tasks", rec.execs[:5])
	}
	edgeExec := rec.execs[5+len(snapshot.Tasks)+len(snapshot.Endpoints)]
	if got := edgeExec.args[7]; got != `{"quote\"kind","path\\kind"}` {
		t.Fatalf("artifact array literal = %v, want escaped text[] literal", got)
	}
}

func TestPostgresDecodeSnapshotForcesStoredRevision(t *testing.T) {
	raw, err := json.Marshal(GraphSnapshot{
		Revision: 99,
		Tasks: []Task{{
			ID:          "task-a",
			ContractRef: "contract://task-a",
			Outcome:     TaskActive,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeSnapshot(7, raw)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 7 {
		t.Fatalf("revision = %d, want database revision 7", snapshot.Revision)
	}
}

func TestPostgresErrorMapping(t *testing.T) {
	if err := mapPostgresError(&pgconn.PgError{Code: "23505", ConstraintName: "coordination_one_active_phase_lease"}); !kernel.IsCode(err, kernel.CodeLeaseConflict) {
		t.Fatalf("lease unique err = %v, want lease_conflict", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "23505", ConstraintName: "coordination_graph_revisions_pkey"}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("revision unique err = %v, want revision_conflict", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "23503"}); !kernel.IsCode(err, kernel.CodeInvalidGraph) {
		t.Fatalf("foreign key err = %v, want invalid_graph", err)
	}
	if err := mapPostgresError(&pgconn.PgError{Code: "40001"}); !kernel.IsCode(err, kernel.CodeRevisionConflict) {
		t.Fatalf("serialization err = %v, want revision_conflict", err)
	}
}

type recordingDBTX struct {
	execs []recordedExec
}

type recordedExec struct {
	query string
	args  []any
}

func (r *recordingDBTX) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.execs = append(r.execs, recordedExec{query: query, args: append([]any(nil), args...)})
	return recordingResult(1), nil
}

func (r *recordingDBTX) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, nil
}

func (r *recordingDBTX) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

type recordingResult int64

func (r recordingResult) LastInsertId() (int64, error) { return 0, nil }
func (r recordingResult) RowsAffected() (int64, error) { return int64(r), nil }
