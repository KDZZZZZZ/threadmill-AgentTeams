package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestS3BlobPublisherPublishesContentAddressedVerifiedBlob(t *testing.T) {
	contents := []byte("durable artifact bytes")
	digest := sha256.Sum256(contents)
	hash := hex.EncodeToString(digest[:])
	var mu sync.Mutex
	objects := map[string][]byte{}
	var putCount, headCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Authorization"), "test-secret") {
			t.Error("secret key leaked in authorization header")
		}
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=test-access/") {
			t.Errorf("missing signed access key: %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("x-amz-content-sha256") == "" || r.Header.Get("x-amz-date") == "" {
			t.Errorf("missing signed content headers: %#v", r.Header)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			putCount++
			if _, exists := objects[r.URL.Path]; exists {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			objects[r.URL.Path] = body
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			headCount++
			body, exists := objects[r.URL.Path]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			actual := sha256.Sum256(body)
			w.Header().Set("x-amz-meta-content-sha256", hex.EncodeToString(actual[:]))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	source := filepath.Join(t.TempDir(), "report.md")
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewS3BlobPublisher(S3BlobPublisherConfig{Endpoint: server.URL, Bucket: "threadmill-artifacts", Prefix: "runtime/artifacts", AccessKey: "test-access", SecretKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	publisher.now = func() time.Time { return time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC) }
	ref, err := publisher.Publish(context.Background(), source, hash)
	if err != nil {
		t.Fatal(err)
	}
	want := "s3://threadmill-artifacts/runtime/artifacts/sha256/" + hash
	if ref != want || filepath.IsAbs(ref) || strings.Contains(ref, source) {
		t.Fatalf("durable blob ref=%q want=%q", ref, want)
	}
	// A fresh publisher instance models a restarted Runtime: its ref remains
	// stable and the existing content-addressed object is verified again.
	reopened, err := NewS3BlobPublisher(S3BlobPublisherConfig{Endpoint: server.URL, Bucket: "threadmill-artifacts", Prefix: "runtime/artifacts", AccessKey: "test-access", SecretKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = publisher.now
	if _, err = reopened.Publish(context.Background(), source, hash); err != nil {
		t.Fatalf("idempotent publish: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if putCount != 2 || headCount != 2 || string(objects["/threadmill-artifacts/runtime/artifacts/sha256/"+hash]) != string(contents) {
		t.Fatalf("puts=%d heads=%d objects=%q", putCount, headCount, objects)
	}
}

func TestS3BlobPublisherConcurrentIdenticalPublishIsSafe(t *testing.T) {
	contents := []byte("same bytes")
	digest := sha256.Sum256(contents)
	hash := hex.EncodeToString(digest[:])
	var mu sync.Mutex
	objects := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if objects[r.URL.Path] {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			objects[r.URL.Path] = true
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			if !objects[r.URL.Path] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("x-amz-meta-content-sha256", hash)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	source := filepath.Join(t.TempDir(), "report.md")
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewS3BlobPublisher(S3BlobPublisherConfig{Endpoint: server.URL, Bucket: "bucket", AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := publisher.Publish(context.Background(), source, hash)
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(objects) != 1 {
		t.Fatalf("objects=%v", objects)
	}
}

func TestS3BlobPublisherRejectsChangedOrUnverifiedSource(t *testing.T) {
	source := filepath.Join(t.TempDir(), "report.md")
	if err := os.WriteFile(source, []byte("actual"), 0o600); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewS3BlobPublisher(S3BlobPublisherConfig{Endpoint: "http://127.0.0.1:9000", Bucket: "bucket", AccessKey: "key", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	wrong := sha256.Sum256([]byte("different"))
	if _, err = publisher.Publish(context.Background(), source, hex.EncodeToString(wrong[:])); err == nil {
		t.Fatal("hash mismatch published source")
	}
}
