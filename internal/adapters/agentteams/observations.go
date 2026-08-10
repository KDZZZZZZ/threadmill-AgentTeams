package agentteams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type ObservationProjector struct{}

type ProjectedObservations struct {
	Observations       []ExecutionObservation
	NextCursor         string
	IgnoredTaskEvents  int
	LastIgnoredHostRef string
	LastIgnoredAt      time.Time
}

func (ObservationProjector) Project(
	ctx context.Context,
	raw []RawObservation,
	store ExecutionStore,
) (ProjectedObservations, error) {
	result := ProjectedObservations{}
	result.Observations = make([]ExecutionObservation, 0, len(raw))
	indexByID := make(map[string]int, len(raw))
	fingerprints := make(map[string]string, len(raw))
	for _, item := range raw {
		if strings.TrimSpace(item.Cursor) == "" || strings.TrimSpace(item.Kind) == "" || strings.TrimSpace(item.HostRef) == "" || item.ObservedAt.IsZero() {
			return ProjectedObservations{}, kernel.InvalidArgument("AgentTeams observation requires cursor, host_ref, kind, and observed_at")
		}
		result.NextCursor = item.Cursor
		var invocationID kernel.InvocationID
		if item.TaskID != "" {
			record, ok, err := store.GetByTaskID(ctx, item.TaskID)
			if err != nil {
				return ProjectedObservations{}, err
			}
			if !ok {
				result.IgnoredTaskEvents++
				result.LastIgnoredHostRef = item.HostRef
				result.LastIgnoredAt = item.ObservedAt
				continue
			}
			invocationID = record.Execution.InvocationID
		}
		id, fingerprint, err := observationIdentity(item)
		if err != nil {
			return ProjectedObservations{}, err
		}
		if position, ok := indexByID[id]; ok {
			if fingerprints[id] != fingerprint {
				return ProjectedObservations{}, kernel.IdempotencyConflict()
			}
			result.Observations[position].Cursor = item.Cursor
			continue
		}
		observation := ExecutionObservation{
			ID:               id,
			Cursor:           item.Cursor,
			AgentTeamsTaskID: item.TaskID,
			HostRef:          item.HostRef,
			Kind:             item.Kind,
			Payload:          clonePayload(item.Payload),
			ObservedAt:       item.ObservedAt,
			InvocationID:     invocationID,
		}
		indexByID[id] = len(result.Observations)
		fingerprints[id] = fingerprint
		result.Observations = append(result.Observations, observation)
	}
	return result, nil
}

func (p ObservationProjector) ProjectObservations(
	ctx context.Context,
	raw []RawObservation,
	store ExecutionStore,
) ([]ExecutionObservation, error) {
	result, err := p.Project(ctx, raw, store)
	if err != nil {
		return nil, err
	}
	return result.Observations, nil
}

func cursorAdvanceObservation(projected ProjectedObservations) ExecutionObservation {
	identity := sha256.Sum256([]byte(projected.NextCursor + "\x00" + projected.LastIgnoredHostRef))
	return ExecutionObservation{
		ID:         "agentteams:cursor:" + hex.EncodeToString(identity[:16]),
		Cursor:     projected.NextCursor,
		HostRef:    projected.LastIgnoredHostRef,
		Kind:       "cursor_advance",
		Payload:    map[string]string{"ignored_task_events": strconv.Itoa(projected.IgnoredTaskEvents)},
		ObservedAt: projected.LastIgnoredAt,
	}
}

func observationIdentity(item RawObservation) (string, string, error) {
	canonicalPayload := clonePayload(item.Payload)
	keys := make([]string, 0, len(canonicalPayload))
	for key := range canonicalPayload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([][2]string, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, [2]string{key, canonicalPayload[key]})
	}
	payload, err := json.Marshal(struct {
		ProviderEventID string
		TaskID          string
		HostRef         string
		Kind            string
		Payload         [][2]string
		ObservedAt      string
	}{
		ProviderEventID: item.ProviderEventID,
		TaskID:          item.TaskID,
		HostRef:         item.HostRef,
		Kind:            item.Kind,
		Payload:         ordered,
		ObservedAt:      item.ObservedAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
	})
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(sum[:])
	if item.ProviderEventID != "" {
		providerSum := sha256.Sum256([]byte(item.HostRef + "\x00" + item.ProviderEventID))
		return "agentteams:" + hex.EncodeToString(providerSum[:16]), fingerprint, nil
	}
	return "agentteams:" + fingerprint[:32], fingerprint, nil
}

func clonePayload(payload map[string]string) map[string]string {
	cloned := make(map[string]string, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}
