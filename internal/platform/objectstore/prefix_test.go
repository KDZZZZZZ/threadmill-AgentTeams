package objectstore

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestPrefixedStoreUsesPhysicalNamespaceAndReturnsLogicalRefs(t *testing.T) {
	base := NewMemoryStore()
	store, err := WithKeyPrefix(base, "shared/threadmill/evidence")
	if err != nil {
		t.Fatal(err)
	}
	put, err := store.Put(context.Background(), PutObject{Bucket: "artifacts", Key: "tool_output/hash", Body: bytes.NewBufferString("proof")})
	if err != nil {
		t.Fatal(err)
	}
	if put.Key != "tool_output/hash" {
		t.Fatalf("logical put key = %q", put.Key)
	}
	if _, err := base.Get(context.Background(), ObjectRef{Bucket: "artifacts", Key: "tool_output/hash"}); err != ErrObjectNotFound {
		t.Fatalf("unprefixed physical object error = %v, want not found", err)
	}
	physical, err := base.Get(context.Background(), ObjectRef{Bucket: "artifacts", Key: "shared/threadmill/evidence/tool_output/hash"})
	if err != nil {
		t.Fatal(err)
	}
	_ = physical.Body.Close()
	got, err := store.Get(context.Background(), put.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if string(body) != "proof" || got.Key != put.Key {
		t.Fatalf("logical get = key %q body %q", got.Key, body)
	}
	if err := store.Delete(context.Background(), put.ObjectRef); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Get(context.Background(), physical.ObjectRef); err != ErrObjectNotFound {
		t.Fatalf("physical object after delete error = %v, want not found", err)
	}
}
