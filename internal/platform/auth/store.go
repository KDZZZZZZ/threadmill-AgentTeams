package auth

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type SessionRecord struct {
	SessionHash      []byte
	ActorPrincipalID kernel.ActorPrincipalID
	ProjectIDs       map[kernel.ProjectID]struct{}
	CSRFHash         []byte
	ExpiresAt        time.Time
	RevokedAt        *time.Time
}

type TokenRecord struct {
	TokenHash        []byte
	ActorPrincipalID kernel.ActorPrincipalID
	Capability       Capability
	ExpiresAt        time.Time
	RevokedAt        *time.Time
}

type Store interface {
	PutSession(ctx context.Context, record SessionRecord) error
	SessionByHash(ctx context.Context, hash []byte) (SessionRecord, bool, error)
	RevokeSession(ctx context.Context, hash []byte, at time.Time) error
	PutToken(ctx context.Context, record TokenRecord) error
	TokenByHash(ctx context.Context, hash []byte) (TokenRecord, bool, error)
	RevokeToken(ctx context.Context, hash []byte, at time.Time) error
}

type MemoryStore struct {
	mu       sync.RWMutex
	sessions []SessionRecord
	tokens   []TokenRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) PutSession(ctx context.Context, record SessionRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.SessionHash = bytes.Clone(record.SessionHash)
	record.CSRFHash = bytes.Clone(record.CSRFHash)
	record.ProjectIDs = cloneStringSet(record.ProjectIDs)
	s.sessions = upsertSession(s.sessions, record)
	return nil
}

func (s *MemoryStore) SessionByHash(ctx context.Context, hash []byte) (SessionRecord, bool, error) {
	select {
	case <-ctx.Done():
		return SessionRecord{}, false, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.sessions {
		if bytes.Equal(record.SessionHash, hash) {
			return cloneSessionRecord(record), true, nil
		}
	}
	return SessionRecord{}, false, nil
}

func (s *MemoryStore) RevokeSession(ctx context.Context, hash []byte, at time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.sessions {
		if bytes.Equal(s.sessions[i].SessionHash, hash) {
			s.sessions[i].RevokedAt = &at
			return nil
		}
	}
	return kernel.Error{Code: kernel.CodeUnauthorized, Message: "session not found"}
}

func (s *MemoryStore) PutToken(ctx context.Context, record TokenRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.TokenHash = bytes.Clone(record.TokenHash)
	record.Capability.Tools = cloneTools(record.Capability.Tools)
	s.tokens = upsertToken(s.tokens, record)
	return nil
}

func (s *MemoryStore) TokenByHash(ctx context.Context, hash []byte) (TokenRecord, bool, error) {
	select {
	case <-ctx.Done():
		return TokenRecord{}, false, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.tokens {
		if bytes.Equal(record.TokenHash, hash) {
			return cloneTokenRecord(record), true, nil
		}
	}
	return TokenRecord{}, false, nil
}

func (s *MemoryStore) RevokeToken(ctx context.Context, hash []byte, at time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.tokens {
		if bytes.Equal(s.tokens[i].TokenHash, hash) {
			s.tokens[i].RevokedAt = &at
			return nil
		}
	}
	return kernel.Error{Code: kernel.CodeUnauthorized, Message: "token not found"}
}

func upsertSession(records []SessionRecord, record SessionRecord) []SessionRecord {
	for i := range records {
		if bytes.Equal(records[i].SessionHash, record.SessionHash) {
			records[i] = record
			return records
		}
	}
	return append(records, record)
}

func upsertToken(records []TokenRecord, record TokenRecord) []TokenRecord {
	for i := range records {
		if bytes.Equal(records[i].TokenHash, record.TokenHash) {
			records[i] = record
			return records
		}
	}
	return append(records, record)
}

func cloneSessionRecord(record SessionRecord) SessionRecord {
	record.SessionHash = bytes.Clone(record.SessionHash)
	record.CSRFHash = bytes.Clone(record.CSRFHash)
	record.ProjectIDs = cloneStringSet(record.ProjectIDs)
	return record
}

func cloneTokenRecord(record TokenRecord) TokenRecord {
	record.TokenHash = bytes.Clone(record.TokenHash)
	record.Capability.Tools = cloneTools(record.Capability.Tools)
	return record
}

func cloneStringSet(set map[kernel.ProjectID]struct{}) map[kernel.ProjectID]struct{} {
	copied := make(map[kernel.ProjectID]struct{}, len(set))
	for item := range set {
		copied[item] = struct{}{}
	}
	return copied
}
