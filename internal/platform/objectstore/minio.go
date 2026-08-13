package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const defaultContentType = "application/octet-stream"

type MinIOConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Secure    bool
}

type MinIOStore struct {
	bucket string
	client objectClient
}

type putOptions struct {
	ContentType string
	SHA256      string
}

type remoteObject struct {
	ContentType string
	SHA256      string
	Size        int64
	Body        io.ReadCloser
}

type objectClient interface {
	PutObject(context.Context, string, string, io.Reader, int64, putOptions) error
	GetObject(context.Context, string, string) (remoteObject, error)
	DeleteObject(context.Context, string, string) error
	IsNotFound(error) bool
}

func NewMinIOStore(cfg MinIOConfig) (*MinIOStore, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("object store endpoint is required")
	}
	if strings.TrimSpace(cfg.AccessKey) == "" {
		return nil, errors.New("object store access key is required")
	}
	if strings.TrimSpace(cfg.SecretKey) == "" {
		return nil, errors.New("object store secret key is required")
	}
	if err := validateBucket(cfg.Bucket); err != nil {
		return nil, err
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}
	return newMinIOStore(cfg.Bucket, minioClient{client: client})
}

func newMinIOStore(bucket string, client objectClient) (*MinIOStore, error) {
	if err := validateBucket(bucket); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("object store client is required")
	}
	return &MinIOStore{bucket: bucket, client: client}, nil
}

func (s *MinIOStore) Put(ctx context.Context, obj PutObject) (PutResult, error) {
	if err := ctx.Err(); err != nil {
		return PutResult{}, err
	}
	bucket := obj.Bucket
	if bucket == "" {
		bucket = s.bucket
	}
	if bucket != s.bucket {
		return PutResult{}, errors.New("object bucket does not match configured bucket")
	}
	if err := validateBucket(bucket); err != nil {
		return PutResult{}, err
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

	key := obj.Key
	if key == "" {
		key = hash
	}
	key, err = cleanKey(key)
	if err != nil {
		return PutResult{}, err
	}

	contentType := obj.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	if err := s.client.PutObject(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), putOptions{ContentType: contentType, SHA256: hash}); err != nil {
		return PutResult{}, fmt.Errorf("put object: %w", err)
	}
	return PutResult{ObjectRef: ObjectRef{Bucket: bucket, Key: key}, SHA256: hash, Size: int64(len(body))}, nil
}

func (s *MinIOStore) Get(ctx context.Context, ref ObjectRef) (GetResult, error) {
	if err := ctx.Err(); err != nil {
		return GetResult{}, err
	}
	if ref.Bucket == "" {
		ref.Bucket = s.bucket
	}
	if ref.Bucket != s.bucket {
		return GetResult{}, errors.New("object bucket does not match configured bucket")
	}
	key, err := cleanKey(ref.Key)
	if err != nil {
		return GetResult{}, err
	}

	obj, err := s.client.GetObject(ctx, ref.Bucket, key)
	if err != nil {
		if s.client.IsNotFound(err) {
			return GetResult{}, ErrObjectNotFound
		}
		return GetResult{}, fmt.Errorf("get object: %w", err)
	}
	body, err := io.ReadAll(obj.Body)
	closeErr := obj.Body.Close()
	if err != nil {
		return GetResult{}, fmt.Errorf("read object body: %w", err)
	}
	if closeErr != nil {
		return GetResult{}, fmt.Errorf("close object body: %w", closeErr)
	}

	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	if obj.SHA256 != "" && obj.SHA256 != hash {
		return GetResult{}, fmt.Errorf("object hash mismatch: got %s want %s", hash, obj.SHA256)
	}
	if obj.Size >= 0 && obj.Size != int64(len(body)) {
		return GetResult{}, fmt.Errorf("object size mismatch: got %d want %d", len(body), obj.Size)
	}
	contentType := obj.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	return GetResult{
		ObjectRef:   ObjectRef{Bucket: ref.Bucket, Key: key},
		ContentType: contentType,
		SHA256:      hash,
		Size:        int64(len(body)),
		Body:        io.NopCloser(bytes.NewReader(body)),
	}, nil
}

func (s *MinIOStore) Delete(ctx context.Context, ref ObjectRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ref.Bucket == "" {
		ref.Bucket = s.bucket
	}
	if ref.Bucket != s.bucket {
		return errors.New("object bucket does not match configured bucket")
	}
	key, err := cleanKey(ref.Key)
	if err != nil {
		return err
	}
	if err := s.client.DeleteObject(ctx, ref.Bucket, key); err != nil {
		if s.client.IsNotFound(err) {
			return ErrObjectNotFound
		}
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

type minioClient struct {
	client *minio.Client
}

func (c minioClient) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, opts putOptions) error {
	_, err := c.client.PutObject(ctx, bucket, key, body, size, minio.PutObjectOptions{
		ContentType:  opts.ContentType,
		UserMetadata: map[string]string{"sha256": opts.SHA256},
	})
	return err
}

func (c minioClient) GetObject(ctx context.Context, bucket, key string) (remoteObject, error) {
	obj, err := c.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return remoteObject{}, err
	}
	info, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return remoteObject{}, err
	}
	return remoteObject{
		ContentType: info.ContentType,
		SHA256:      metadataValue(info, "sha256"),
		Size:        info.Size,
		Body:        obj,
	}, nil
}

func (c minioClient) DeleteObject(ctx context.Context, bucket, key string) error {
	return c.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

func (c minioClient) IsNotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" || resp.Code == "NoSuchBucket"
}

func metadataValue(info minio.ObjectInfo, key string) string {
	if value, ok := info.UserMetadata[key]; ok {
		return value
	}
	if value := info.Metadata.Get("x-amz-meta-" + key); value != "" {
		return value
	}
	return info.Metadata.Get(key)
}

func validateBucket(bucket string) error {
	if bucket == "" {
		return errors.New("bucket is required")
	}
	if bucket != strings.TrimSpace(bucket) || strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") {
		return errors.New("bucket is invalid")
	}
	return nil
}

func cleanKey(key string) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", errors.New("object key is required")
	}
	if strings.Contains(key, "\\") {
		return "", errors.New("object key is invalid")
	}
	cleaned := strings.TrimPrefix(path.Clean("/"+key), "/")
	if cleaned == "." || cleaned == "" || strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", errors.New("object key is invalid")
	}
	if cleaned != key {
		return "", errors.New("object key must already be clean")
	}
	return cleaned, nil
}
