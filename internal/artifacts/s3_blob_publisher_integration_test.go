//go:build integration

package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// TestS3BlobPublisherMinIO exercises the official local fixture's actual
// MinIO S3 API. The fixture owns its ephemeral server, bucket, credentials,
// and cleanup; this test neither prints nor persists those credentials.
func TestS3BlobPublisherMinIO(t *testing.T) {
	endpoint := os.Getenv("THREADMILL_IT_MINIO_ENDPOINT")
	accessKey := os.Getenv("THREADMILL_IT_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("THREADMILL_IT_MINIO_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("requires the existing local MinIO integration fixture")
	}
	contents := []byte("threadmill durable artifact publisher integration")
	digest := sha256.Sum256(contents)
	hash := hex.EncodeToString(digest[:])
	source := filepath.Join(t.TempDir(), "report.md")
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewS3BlobPublisher(S3BlobPublisherConfig{
		Endpoint: endpoint, Bucket: "threadmill-it", Prefix: "threadmill/durable-artifacts",
		AccessKey: accessKey, SecretKey: secretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := publisher.Publish(context.Background(), source, hash)
	if err != nil {
		t.Fatal(err)
	}
	if ref == "" || filepath.IsAbs(ref) {
		t.Fatalf("non-durable blob ref %q", ref)
	}
	if _, err = publisher.Publish(context.Background(), source, hash); err != nil {
		t.Fatalf("idempotent MinIO publish: %v", err)
	}
}
