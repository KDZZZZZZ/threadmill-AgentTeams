package agentteams

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/objectstore"
)

type SharedObjectFileTransport struct {
	store  objectstore.Store
	bucket string
	prefix string
	limit  int64
}

func NewSharedObjectFileTransport(store objectstore.Store, bucket, prefix string) (*SharedObjectFileTransport, error) {
	bucket = strings.TrimSpace(bucket)
	if store == nil || bucket == "" {
		return nil, kernel.InvalidArgument("AgentTeams shared object file transport requires store and bucket")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	return &SharedObjectFileTransport{
		store:  store,
		bucket: bucket,
		prefix: prefix,
		limit:  1 << 20,
	}, nil
}

func (t *SharedObjectFileTransport) PullExecution(ctx context.Context, taskID string) error {
	if !safeProviderID(taskID) {
		return kernel.InvalidArgument("AgentTeams task_id is invalid")
	}
	return ctx.Err()
}

func (t *SharedObjectFileTransport) ReadResult(ctx context.Context, taskID string) ([]byte, error) {
	if !safeProviderID(taskID) {
		return nil, kernel.InvalidArgument("AgentTeams task_id is invalid")
	}
	key := path.Join(t.prefix, "shared", "tasks", taskID, "result.md")
	got, err := t.store.Get(ctx, objectstore.ObjectRef{Bucket: t.bucket, Key: key})
	if err != nil {
		return nil, err
	}
	defer got.Body.Close()
	body, err := io.ReadAll(io.LimitReader(got.Body, t.limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > t.limit {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "AgentTeams result document exceeded the limit", Recoverable: true}
	}
	return body, nil
}

var _ FileTransport = (*SharedObjectFileTransport)(nil)
