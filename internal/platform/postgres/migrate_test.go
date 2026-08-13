package postgres

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsPairsUpAndDownFilesInOrder(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"migrations/0002_second.up.sql":   {Data: []byte("create table second();")},
		"migrations/0001_first.up.sql":    {Data: []byte("create table first();")},
		"migrations/0001_first.down.sql":  {Data: []byte("drop table first;")},
		"migrations/0002_second.down.sql": {Data: []byte("drop table second;")},
	}

	migrations, err := LoadMigrations(fsys, "migrations")
	if err != nil {
		t.Fatalf("LoadMigrations returned error: %v", err)
	}

	var got []string
	for _, migration := range migrations {
		got = append(got, migration.Version+":"+migration.Name)
	}
	want := []string{"0001:first", "0002:second"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("migration order = %#v, want %#v", got, want)
	}
	if migrations[0].UpSQL == "" || migrations[0].DownSQL == "" {
		t.Fatal("migration SQL was not loaded")
	}
}

func TestPlatformDownMigrationKeepsSchemaMigrationsTable(t *testing.T) {
	t.Parallel()

	sqlBytes, err := os.ReadFile("../../../migrations/0001_platform.down.sql")
	if err != nil {
		t.Fatalf("read down migration: %v", err)
	}
	downSQL := strings.ToLower(string(sqlBytes))
	if strings.Contains(downSQL, "drop table if exists schema_migrations") ||
		strings.Contains(downSQL, "drop table schema_migrations") {
		t.Fatal("0001 down migration must not drop schema_migrations; Migrator.Rollback deletes its row after running down SQL")
	}
}
