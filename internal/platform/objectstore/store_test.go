package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestMemoryStorePutGetDeduplicatesBySHA256(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryStore()
	body := []byte("threadmill artifact")

	first, err := store.Put(ctx, PutObject{
		Bucket:      "artifacts",
		Key:         "ignored/first",
		ContentType: "text/plain",
		Body:        bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("put first object: %v", err)
	}

	second, err := store.Put(ctx, PutObject{
		Bucket: "artifacts",
		Key:    "ignored/second",
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("put duplicate object: %v", err)
	}

	if first.Key != second.Key {
		t.Fatalf("duplicate content stored under different keys: %q != %q", first.Key, second.Key)
	}

	got, err := store.Get(ctx, ObjectRef{Bucket: "artifacts", Key: first.Key})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer got.Body.Close()

	gotBody, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("read object body: %v", err)
	}
	if string(gotBody) != string(body) {
		t.Fatalf("body mismatch: got %q want %q", gotBody, body)
	}
	if got.SHA256 != first.SHA256 {
		t.Fatalf("hash mismatch: got %q want %q", got.SHA256, first.SHA256)
	}
}

func TestMemoryStoreMissingObject(t *testing.T) {
	t.Parallel()

	_, err := NewMemoryStore().Get(context.Background(), ObjectRef{Bucket: "artifacts", Key: "missing"})
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("missing object error = %v, want %v", err, ErrObjectNotFound)
	}
}
