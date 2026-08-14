package app

import (
	"context"
	"os"
	"testing"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

func TestProductionUIPermissionsRequirePersistedTaskBodyGrantAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("THREADMILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("THREADMILL_TEST_DATABASE_URL is required for the real PostgreSQL integration test")
	}
	ctx := context.Background()
	db := openProductionPhasePostgres(t, ctx, databaseURL)
	projectID := kernel.ProjectID("project-ui-acl")
	permissions := productionUIPermissions{db: db, projectID: projectID}
	operator := auth.Principal{
		ActorPrincipalID: "operator://alice", Kind: auth.PrincipalOperator,
		ProjectID: projectID, Role: auth.RoleOperator,
	}

	grant, err := permissions.TaskGrant(ctx, operator, projectID, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if grant.Visible || grant.ContextBodies || grant.CandidateBodies {
		t.Fatalf("missing persisted grant yielded access: %#v", grant)
	}
	if err := grantProductionOperatorUI(ctx, db, operator.ActorPrincipalID, projectID); err != nil {
		t.Fatal(err)
	}
	grant, err = permissions.TaskGrant(ctx, operator, projectID, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Visible || !grant.ContextBodies || !grant.CandidateBodies {
		t.Fatalf("explicit project-wide UI grant = %#v", grant)
	}

	if _, err := db.ExecContext(ctx, `
INSERT INTO operator_ui_task_grants (
  actor_principal_id, project_id, task_id, visible, context_bodies, candidate_bodies
) VALUES ($1, $2, $3, TRUE, FALSE, FALSE)`, operator.ActorPrincipalID, projectID, "task-a"); err != nil {
		t.Fatal(err)
	}
	redacted, err := permissions.TaskGrant(ctx, operator, projectID, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if !redacted.Visible || redacted.ContextBodies || redacted.CandidateBodies {
		t.Fatalf("task-specific body redaction did not override wildcard: %#v", redacted)
	}
	otherTask, err := permissions.TaskGrant(ctx, operator, projectID, "task-b")
	if err != nil {
		t.Fatal(err)
	}
	if !otherTask.Visible || !otherTask.ContextBodies || !otherTask.CandidateBodies {
		t.Fatalf("wildcard grant for another task = %#v", otherTask)
	}

	otherOperator := operator
	otherOperator.ActorPrincipalID = "operator://bob"
	otherGrant, err := permissions.TaskGrant(ctx, otherOperator, projectID, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if otherGrant.Visible || otherGrant.ContextBodies || otherGrant.CandidateBodies {
		t.Fatalf("grant leaked across operators: %#v", otherGrant)
	}
}
