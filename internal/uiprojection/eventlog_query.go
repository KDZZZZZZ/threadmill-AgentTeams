package uiprojection

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	defaultEventLogReplayBatch = 100
	defaultEventLogStreamBuf   = 32
	defaultEventLogPollEvery   = 25 * time.Millisecond
)

// EventLogQuery adapts the authoritative evidence.EventLog into the GUI event
// query surface. It does not keep a second event store; every page is rebuilt
// by replaying EventLog rows through EventMapper.
type EventLogQuery struct {
	log       evidence.EventStore
	mapper    EventMapper
	retained  evidence.Cursor
	batchSize int
	streamBuf int
	pollEvery time.Duration

	mu          sync.Mutex
	nextSubID   int64
	subscribers map[int64]*eventLogSubscriber
}

type EventLogQueryOption func(*EventLogQuery)

func WithEventLogRetentionFloor(cursor evidence.Cursor) EventLogQueryOption {
	return func(q *EventLogQuery) {
		if cursor > 0 {
			q.retained = cursor
		}
	}
}

func WithEventLogReplayBatchSize(size int) EventLogQueryOption {
	return func(q *EventLogQuery) {
		if size > 0 {
			q.batchSize = size
		}
	}
}

func WithEventLogStreamBuffer(size int) EventLogQueryOption {
	return func(q *EventLogQuery) {
		if size > 0 {
			q.streamBuf = size
		}
	}
}

func WithEventLogPollInterval(interval time.Duration) EventLogQueryOption {
	return func(q *EventLogQuery) {
		if interval > 0 {
			q.pollEvery = interval
		}
	}
}

func NewEventLogQuery(log evidence.EventStore, permissions PermissionReader, options ...EventLogQueryOption) *EventLogQuery {
	q := &EventLogQuery{
		log:         log,
		mapper:      NewEventMapper(permissions),
		batchSize:   defaultEventLogReplayBatch,
		streamBuf:   defaultEventLogStreamBuf,
		pollEvery:   defaultEventLogPollEvery,
		subscribers: make(map[int64]*eventLogSubscriber),
	}
	for _, option := range options {
		option(q)
	}
	return q
}

func (q *EventLogQuery) ListEvents(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, after string, limit int) (EventPage, error) {
	if err := q.configured(); err != nil {
		return EventPage{}, err
	}
	if projectID == "" {
		return EventPage{}, kernel.InvalidArgument("project_id is required")
	}
	if err := q.requireProject(ctx, principal, projectID); err != nil {
		return EventPage{}, err
	}
	if limit <= 0 {
		limit = defaultEventLogReplayBatch
	}
	afterCursor, err := q.parseCursor(after)
	if err != nil {
		return EventPage{}, err
	}

	events, next, err := q.visibleEventsAfter(ctx, principal, projectID, afterCursor, limit)
	if err != nil {
		return EventPage{}, err
	}
	return EventPage{
		NextCursor: formatEvidenceCursor(next),
		Events:     events,
	}, nil
}

func (q *EventLogQuery) SubscribeEvents(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, after string) (<-chan UIEvent, error) {
	if err := q.configured(); err != nil {
		return nil, err
	}
	if projectID == "" {
		return nil, kernel.InvalidArgument("project_id is required")
	}
	if err := q.requireProject(ctx, principal, projectID); err != nil {
		return nil, err
	}
	afterCursor, err := q.parseCursor(after)
	if err != nil {
		return nil, err
	}

	backlog, cursor, err := q.visibleBacklog(ctx, principal, projectID, afterCursor)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.nextSubID++
	id := q.nextSubID
	sub := &eventLogSubscriber{
		id:         id,
		principal:  principal,
		projectID:  projectID,
		lastCursor: cursor,
		wake:       make(chan struct{}, 1),
		out:        make(chan UIEvent, q.streamBuf),
	}
	q.subscribers[id] = sub
	q.mu.Unlock()

	go q.runSubscriber(ctx, sub, backlog)
	return sub.out, nil
}

// CurrentCursor implements CursorReader for snapshots. The cursor is the latest
// EventLog sequence, independent of ACL filtering.
func (q *EventLogQuery) CurrentCursor(ctx context.Context, _ kernel.ProjectID) (string, error) {
	if err := q.configured(); err != nil {
		return "", err
	}
	cursor := evidence.Cursor(0)
	for {
		events, next, err := q.log.Replay(ctx, cursor, q.batchSize)
		if err != nil {
			return "", err
		}
		if len(events) == 0 {
			return formatEvidenceCursor(cursor), nil
		}
		cursor = next
	}
}

// Append writes through the authoritative EventLog and wakes active subscribers.
// Subscribers poll the EventStore cursor, so writers that bypass this adapter
// are still streamed without relying on this fast wake-up path.
func (q *EventLogQuery) Append(ctx context.Context, event evidence.AppendEvent) (evidence.Event, error) {
	if err := q.configured(); err != nil {
		return evidence.Event{}, err
	}
	appended, err := q.log.Append(ctx, event)
	if err != nil {
		return evidence.Event{}, err
	}
	q.wakeSubscribers(appended)
	return appended, nil
}

func (q *EventLogQuery) configured() error {
	if q == nil || q.log == nil {
		return kernel.InvalidArgument("event log is required")
	}
	return nil
}

func (q *EventLogQuery) requireProject(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID) error {
	if q.mapper.permissions == nil {
		return kernel.Forbidden("UI event permission reader is not configured")
	}
	return requireEventProject(ctx, q.mapper.permissions, principal, projectID)
}

func (q *EventLogQuery) parseCursor(raw string) (evidence.Cursor, error) {
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, kernel.InvalidArgument("event cursor must be a non-negative integer")
	}
	cursor := evidence.Cursor(value)
	if q.retained > 0 && cursor < q.retained {
		return 0, CursorExpired(raw)
	}
	return cursor, nil
}

func (q *EventLogQuery) visibleBacklog(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, after evidence.Cursor) ([]UIEvent, evidence.Cursor, error) {
	var backlog []UIEvent
	cursor := after
	for {
		events, next, err := q.log.Replay(ctx, cursor, q.batchSize)
		if err != nil {
			return nil, cursor, err
		}
		if len(events) == 0 {
			return backlog, cursor, nil
		}
		for _, event := range events {
			if event.Sequence <= cursor {
				continue
			}
			mapped, ok, err := q.mapEvidenceEvent(ctx, principal, projectID, event)
			if err != nil {
				return nil, cursor, err
			}
			cursor = event.Sequence
			if ok {
				backlog = append(backlog, mapped)
			}
		}
		if next <= cursor && len(events) == 0 {
			return backlog, cursor, nil
		}
	}
}

func (q *EventLogQuery) visibleEventsAfter(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, after evidence.Cursor, limit int) ([]UIEvent, evidence.Cursor, error) {
	cursor := after
	out := make([]UIEvent, 0, limit)
	for {
		events, _, err := q.log.Replay(ctx, cursor, q.batchSize)
		if err != nil {
			return nil, cursor, err
		}
		if len(events) == 0 {
			return out, cursor, nil
		}
		for _, event := range events {
			if event.Sequence <= cursor {
				continue
			}
			mapped, ok, err := q.mapEvidenceEvent(ctx, principal, projectID, event)
			if err != nil {
				return nil, cursor, err
			}
			if ok && len(out) == limit {
				return out, cursor, nil
			}
			cursor = event.Sequence
			if ok {
				out = append(out, mapped)
			}
		}
	}
}

func (q *EventLogQuery) mapEvidenceEvent(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, event evidence.Event) (UIEvent, bool, error) {
	if event.ProjectID != projectID {
		return UIEvent{}, false, nil
	}
	return q.mapper.Map(ctx, principal, RawEvent{
		EventID:       string(event.ID),
		Cursor:        formatEvidenceCursor(event.Sequence),
		Type:          event.Type,
		ProjectID:     event.ProjectID,
		TaskID:        event.TaskID,
		EndpointID:    event.PhaseEndpoint,
		InvocationID:  event.AgentInvocationID,
		GraphRevision: kernel.Revision(event.GraphRevision),
		OccurredAt:    event.CreatedAt,
		Payload:       event.Payload,
	})
}

func (q *EventLogQuery) wakeSubscribers(event evidence.Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, sub := range q.subscribers {
		if event.Sequence <= sub.lastCursor {
			continue
		}
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

func (q *EventLogQuery) runSubscriber(ctx context.Context, sub *eventLogSubscriber, backlog []UIEvent) {
	defer close(sub.out)
	defer q.unsubscribe(sub.id)

	for _, event := range backlog {
		select {
		case <-ctx.Done():
			return
		case sub.out <- event:
		}
	}
	ticker := time.NewTicker(q.pollEvery)
	defer ticker.Stop()
	for {
		if !q.pollSubscriber(ctx, sub) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-sub.wake:
		case <-ticker.C:
		}
	}
}

func (q *EventLogQuery) pollSubscriber(ctx context.Context, sub *eventLogSubscriber) bool {
	for {
		events, cursor, err := q.visibleEventsAfter(ctx, sub.principal, sub.projectID, sub.lastCursor, q.batchSize)
		if err != nil {
			return false
		}
		if len(events) == 0 {
			sub.lastCursor = cursor
			return true
		}
		for _, event := range events {
			select {
			case <-ctx.Done():
				return false
			case sub.out <- event:
			}
		}
		sub.lastCursor = cursor
		if len(events) < q.batchSize {
			return true
		}
	}
}

func (q *EventLogQuery) unsubscribe(id int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if sub, ok := q.subscribers[id]; ok {
		close(sub.wake)
		delete(q.subscribers, id)
	}
}

type eventLogSubscriber struct {
	id         int64
	principal  auth.Principal
	projectID  kernel.ProjectID
	lastCursor evidence.Cursor
	wake       chan struct{}
	out        chan UIEvent
}

func formatEvidenceCursor(cursor evidence.Cursor) string {
	return strconv.FormatInt(int64(cursor), 10)
}
