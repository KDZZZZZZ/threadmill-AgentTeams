package kernel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

type IdempotencyResponse struct {
	StatusCode int
	Body       []byte
}

type IdempotencyExecutor func(context.Context) (IdempotencyResponse, error)

// IdempotencyStore is intentionally narrow so a future PostgreSQL
// implementation can provide the same atomic semantics with row locks.
type IdempotencyStore interface {
	Execute(context.Context, IDScope, IdempotencyKey, []byte, IdempotencyExecutor) (IdempotencyResponse, error)
}

type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[idempotencyRecordKey]*idempotencyRecord
}

func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{records: make(map[idempotencyRecordKey]*idempotencyRecord)}
}

func (s *MemoryIdempotencyStore) Execute(
	ctx context.Context,
	scope IDScope,
	key IdempotencyKey,
	payload []byte,
	execute IdempotencyExecutor,
) (IdempotencyResponse, error) {
	if stringsBlank(string(scope)) {
		return IdempotencyResponse{}, InvalidArgument("idempotency scope is required")
	}
	if IsZeroID(key) {
		return IdempotencyResponse{}, InvalidArgument("idempotency key is required")
	}
	if execute == nil {
		return IdempotencyResponse{}, InvalidArgument("idempotency executor is required")
	}

	recordKey := idempotencyRecordKey{scope: scope, key: key}
	payloadHash := hashPayload(payload)

	record, owned, err := s.reserve(recordKey, payloadHash)
	if err != nil {
		return IdempotencyResponse{}, err
	}
	if !owned {
		return record.await(ctx)
	}

	response, execErr := execute(ctx)
	if execErr != nil {
		s.releaseFailed(recordKey, record, execErr)
		return IdempotencyResponse{}, execErr
	}

	response.Body = append([]byte(nil), response.Body...)
	s.commit(recordKey, record, response)
	return response, nil
}

type idempotencyRecordKey struct {
	scope IDScope
	key   IdempotencyKey
}

type idempotencyRecord struct {
	payloadHash string
	done        chan struct{}
	response    IdempotencyResponse
	err         error
}

func (s *MemoryIdempotencyStore) reserve(recordKey idempotencyRecordKey, payloadHash string) (*idempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if record := s.records[recordKey]; record != nil {
		if record.payloadHash != payloadHash {
			return nil, false, IdempotencyConflict()
		}
		return record, false, nil
	}

	record := &idempotencyRecord{
		payloadHash: payloadHash,
		done:        make(chan struct{}),
	}
	s.records[recordKey] = record
	return record, true, nil
}

func (s *MemoryIdempotencyStore) commit(recordKey idempotencyRecordKey, record *idempotencyRecord, response IdempotencyResponse) {
	s.mu.Lock()
	current := s.records[recordKey]
	if current == record {
		record.response = cloneResponse(response)
	}
	s.mu.Unlock()
	close(record.done)
}

func (s *MemoryIdempotencyStore) releaseFailed(recordKey idempotencyRecordKey, record *idempotencyRecord, execErr error) {
	s.mu.Lock()
	if s.records[recordKey] == record {
		delete(s.records, recordKey)
	}
	record.err = execErr
	s.mu.Unlock()
	close(record.done)
}

func (r *idempotencyRecord) await(ctx context.Context) (IdempotencyResponse, error) {
	select {
	case <-ctx.Done():
		return IdempotencyResponse{}, ctx.Err()
	case <-r.done:
		if r.err != nil {
			return IdempotencyResponse{}, r.err
		}
		return cloneResponse(r.response), nil
	}
}

func cloneResponse(response IdempotencyResponse) IdempotencyResponse {
	return IdempotencyResponse{
		StatusCode: response.StatusCode,
		Body:       append([]byte(nil), response.Body...),
	}
}

func hashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func stringsBlank(value string) bool {
	for _, char := range value {
		if char != ' ' && char != '\t' && char != '\n' && char != '\r' {
			return false
		}
	}
	return true
}
