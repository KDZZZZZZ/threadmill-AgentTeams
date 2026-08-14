package integration

import (
	"database/sql"
	"os"
	"strings"
	"testing"
)

func TestPlatformIntegrationRequiresExplicitGate(t *testing.T) {
	if os.Getenv("THREADMILL_INTEGRATION") != "1" {
		t.Skip("set THREADMILL_INTEGRATION=1 and provide a database driver in the integration harness")
	}
	if len(sql.Drivers()) == 0 {
		t.Skip("THREADMILL_INTEGRATION=1 requires the app wiring package to register a database/sql driver")
	}
}

func TestThreadmillDepsComposePinsImages(t *testing.T) {
	content, err := os.ReadFile("../../deploy/compose/threadmill-deps.yml")
	if err != nil {
		t.Fatalf("read compose file: %v", err)
	}
	if strings.Contains(string(content), ":latest") {
		t.Fatal("compose file must not use latest tags")
	}
	if !strings.Contains(string(content), "postgres:16.4-alpine") {
		t.Fatal("compose file must pin PostgreSQL image")
	}
	if !strings.Contains(string(content), "minio/minio:RELEASE.") {
		t.Fatal("compose file must pin MinIO image")
	}
}
