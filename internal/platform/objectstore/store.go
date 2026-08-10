package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
)

var ErrObjectNotFound = errors.New("object not found")

type ObjectRef struct {
	Bucket string
	Key    string
}

type PutObject struct {
	Bucket      string
	Key         string
	ContentType string
	Body        io.Reader
}

type PutResult struct {
	ObjectRef
	SHA256 string
	Size   int64
}

type GetResult struct {
	ObjectRef
	ContentType string
	SHA256      string
	Size        int64
	Body        io.ReadCloser
}

type Store interface {
	Put(context.Context, PutObject) (PutResult, error)
	Get(context.Context, ObjectRef) (GetResult, error)
	Delete(context.Context, ObjectRef) error
}

type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]memoryObject
	byHash  map[string]ObjectRef
}

type memoryObject struct {
	ref         ObjectRef
	contentType string
	sha256      string
	body        []byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[string]memoryObject),
		byHash:  make(map[string]ObjectRef),
	}
}

func (s *MemoryStore) Put(ctx context.Context, obj PutObject) (PutResult, error) {
	if err := ctx.Err(); err != nil {
		return PutResult{}, err
	}
	if obj.Bucket == "" {
		return PutResult{}, errors.New("bucket is required")
	}
	if obj.Body == nil {
		return PutResult{}, errors.New("body is required")
	}

	body, err := io.ReadAll(obj.Body)
	if err != nil {
		return PutResult{}, fmt.Errorf("read object body: %w", err)
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.byHash[obj.Bucket+"/"+hash]; ok {
		return PutResult{ObjectRef: existing, SHA256: hash, Size: int64(len(body))}, nil
	}

	key := obj.Key
	if key == "" {
		key = hash
	}
	key = strings.TrimPrefix(path.Clean("/"+key), "/")
	ref := ObjectRef{Bucket: obj.Bucket, Key: key}
	stored := memoryObject{
		ref:         ref,
		contentType: obj.ContentType,
		sha256:      hash,
		body:        append([]byte(nil), body...),
	}
	s.objects[objectKey(ref)] = stored
	s.byHash[obj.Bucket+"/"+hash] = ref

	return PutResult{ObjectRef: ref, SHA256: hash, Size: int64(len(body))}, nil
}

func (s *MemoryStore) Get(ctx context.Context, ref ObjectRef) (GetResult, error) {
	if err := ctx.Err(); err != nil {
		return GetResult{}, err
	}

	s.mu.RLock()
	stored, ok := s.objects[objectKey(ref)]
	s.mu.RUnlock()
	if !ok {
		return GetResult{}, ErrObjectNotFound
	}

	body := append([]byte(nil), stored.body...)
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != stored.sha256 {
		return GetResult{}, fmt.Errorf("object hash mismatch: got %s want %s", got, stored.sha256)
	}

	return GetResult{
		ObjectRef:   ref,
		ContentType: stored.contentType,
		SHA256:      stored.sha256,
		Size:        int64(len(body)),
		Body:        io.NopCloser(bytes.NewReader(body)),
	}, nil
}

func (s *MemoryStore) Delete(ctx context.Context, ref ObjectRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, objectKey(ref))
	return nil
}

func objectKey(ref ObjectRef) string {
	return ref.Bucket + "/" + ref.Key
}
