package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
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

func TestMinIOStorePutGetUsesHashMetadataAndContentType(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	store, err := newMinIOStore("artifacts", client)
	if err != nil {
		t.Fatalf("newMinIOStore() error = %v", err)
	}

	put, err := store.Put(context.Background(), PutObject{
		Key:         "runs/one.txt",
		ContentType: "text/plain",
		Body:        bytes.NewReader([]byte("artifact")),
	})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if put.Bucket != "artifacts" || put.Key != "runs/one.txt" {
		t.Fatalf("Put() ref = %#v", put.ObjectRef)
	}
	if client.lastPut.opts.ContentType != "text/plain" || client.lastPut.opts.SHA256 != put.SHA256 {
		t.Fatalf("Put() options = %#v, want content type and sha", client.lastPut.opts)
	}

	got, err := store.Get(context.Background(), ObjectRef{Bucket: "artifacts", Key: "runs/one.txt"})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "artifact" || got.ContentType != "text/plain" || got.SHA256 != put.SHA256 {
		t.Fatalf("Get() = body %q content-type %q sha %q, want stored object", body, got.ContentType, got.SHA256)
	}
}

func TestMinIOStoreRejectsUnsafeBucketAndKey(t *testing.T) {
	t.Parallel()

	if _, err := newMinIOStore("bad/bucket", newFakeObjectClient()); err == nil {
		t.Fatal("newMinIOStore() error = nil, want unsafe bucket error")
	}

	store, err := newMinIOStore("artifacts", newFakeObjectClient())
	if err != nil {
		t.Fatalf("newMinIOStore() error = %v", err)
	}
	if _, err := store.Put(context.Background(), PutObject{Key: "../escape", Body: bytes.NewReader([]byte("x"))}); err == nil {
		t.Fatal("Put() error = nil, want unsafe key error")
	}
}

func TestMinIOStoreMapsNotFoundAndDetectsHashMismatch(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	store, err := newMinIOStore("artifacts", client)
	if err != nil {
		t.Fatalf("newMinIOStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), ObjectRef{Key: "missing"})
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("missing object error = %v, want %v", err, ErrObjectNotFound)
	}

	client.objects["artifacts/bad"] = fakeStoredObject{
		contentType: "text/plain",
		sha256:      "wrong",
		body:        []byte("artifact"),
	}
	if _, err := store.Get(context.Background(), ObjectRef{Key: "bad"}); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("hash mismatch error = %v, want hash mismatch", err)
	}
}

type fakeObjectClient struct {
	objects map[string]fakeStoredObject
	lastPut struct {
		bucket string
		key    string
		opts   putOptions
	}
}

type fakeStoredObject struct {
	contentType string
	sha256      string
	body        []byte
}

var errFakeNotFound = errors.New("fake object not found")

func newFakeObjectClient() *fakeObjectClient {
	return &fakeObjectClient{objects: make(map[string]fakeStoredObject)}
}

func (f *fakeObjectClient) PutObject(_ context.Context, bucket, key string, body io.Reader, _ int64, opts putOptions) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	f.lastPut.bucket = bucket
	f.lastPut.key = key
	f.lastPut.opts = opts
	f.objects[bucket+"/"+key] = fakeStoredObject{contentType: opts.ContentType, sha256: opts.SHA256, body: data}
	return nil
}

func (f *fakeObjectClient) GetObject(_ context.Context, bucket, key string) (remoteObject, error) {
	stored, ok := f.objects[bucket+"/"+key]
	if !ok {
		return remoteObject{}, errFakeNotFound
	}
	return remoteObject{
		ContentType: stored.contentType,
		SHA256:      stored.sha256,
		Size:        int64(len(stored.body)),
		Body:        io.NopCloser(bytes.NewReader(stored.body)),
	}, nil
}

func (f *fakeObjectClient) DeleteObject(_ context.Context, bucket, key string) error {
	delete(f.objects, bucket+"/"+key)
	return nil
}

func (f *fakeObjectClient) IsNotFound(err error) bool {
	return errors.Is(err, errFakeNotFound)
}
