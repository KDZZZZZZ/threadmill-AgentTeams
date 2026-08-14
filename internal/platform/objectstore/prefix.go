package objectstore

import (
	"context"
	"errors"
	"path"
	"strings"
)

// WithKeyPrefix scopes all object keys to a configured namespace while
// preserving the caller-visible logical key. This is useful when production
// credentials are intentionally restricted to a shared MinIO prefix.
func WithKeyPrefix(store Store, prefix string) (Store, error) {
	if store == nil {
		return nil, errors.New("object store is required")
	}
	cleaned, err := cleanKey(prefix)
	if err != nil {
		return nil, err
	}
	return prefixedStore{store: store, prefix: cleaned}, nil
}

type prefixedStore struct {
	store  Store
	prefix string
}

func (s prefixedStore) Put(ctx context.Context, obj PutObject) (PutResult, error) {
	logicalKey := strings.TrimSpace(obj.Key)
	if logicalKey != "" {
		cleaned, err := cleanKey(logicalKey)
		if err != nil {
			return PutResult{}, err
		}
		obj.Key = path.Join(s.prefix, cleaned)
	}
	put, err := s.store.Put(ctx, obj)
	if err != nil {
		return PutResult{}, err
	}
	if logicalKey != "" {
		put.Key = logicalKey
	} else {
		put.Key = strings.TrimPrefix(strings.TrimPrefix(put.Key, s.prefix), "/")
	}
	return put, nil
}

func (s prefixedStore) Get(ctx context.Context, ref ObjectRef) (GetResult, error) {
	logicalKey, err := cleanKey(ref.Key)
	if err != nil {
		return GetResult{}, err
	}
	ref.Key = path.Join(s.prefix, logicalKey)
	got, err := s.store.Get(ctx, ref)
	if err != nil {
		return GetResult{}, err
	}
	got.Key = logicalKey
	return got, nil
}

func (s prefixedStore) Delete(ctx context.Context, ref ObjectRef) error {
	logicalKey, err := cleanKey(ref.Key)
	if err != nil {
		return err
	}
	ref.Key = path.Join(s.prefix, logicalKey)
	return s.store.Delete(ctx, ref)
}
