package uiprojection

import (
	"context"
	"strconv"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/evidence"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
)

const (
	defaultEventLogReplayBatch = 100
	defaultEventLogStreamBuf   = 32
)

// EventLogQuery adapts the authoritative evidence.EventLog into the GUI event
// query surface. It does not keep a second event store; every page is rebuilt
// by replaying EventLog rows through EventMapper.
type EventLogQuery struct {
	log       *evidence.EventLog
	mapper    EventMapper
	retained  evidence.Cursor
	batchSize int
	streamBuf int

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

func NewEventLogQuery(log *evidence.EventLog, permissions PermissionReader, options ...EventLogQueryOption) *EventLogQuery {
	q := &EventLogQuery{
		log:         log,
		mapper:      NewEventMapper(permissions),
		batchSize:   defaultEventLogReplayBatch,
		streamBuf:   defaultEventLogStreamBuf,
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

	q.mu.Lock()
	backlog, cursor, err := q.visibleBacklogLocked(ctx, principal, projectID, afterCursor)
	if err != nil {
		q.mu.Unlock()
		return nil, err
	}
	q.nextSubID++
	id := q.nextSubID
	sub := &eventLogSubscriber{
		id:         id,
		principal:  principal,
		projectID:  projectID,
		lastCursor: cursor,
		live:       make(chan UIEvent, q.streamBuf),
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

// Append writes through the authoritative EventLog and broadcasts the mapped
// event to active subscribers. Writers that bypass this adapter remain
// replayable by ListEvents, but they cannot be pushed live until wiring uses
// this method or EventLog grows a native subscribe hook.
func (q *EventLogQuery) Append(ctx context.Context, event evidence.AppendEvent) (evidence.Event, error) {
	if err := q.configured(); err != nil {
		return evidence.Event{}, err
	}
	appended, err := q.log.Append(ctx, event)
	if err != nil {
		return evidence.Event{}, err
	}
	q.broadcast(ctx, appended)
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

func (q *EventLogQuery) visibleBacklogLocked(ctx context.Context, principal auth.Principal, projectID kernel.ProjectID, after evidence.Cursor) ([]UIEvent, evidence.Cursor, error) {
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

func (q *EventLogQuery) broadcast(ctx context.Context, event evidence.Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for id, sub := range q.subscribers {
		if event.Sequence <= sub.lastCursor {
			continue
		}
		mapped, ok, err := q.mapEvidenceEvent(ctx, sub.principal, sub.projectID, event)
		if err != nil || !ok {
			sub.lastCursor = event.Sequence
			continue
		}
		select {
		case sub.live <- mapped:
			sub.lastCursor = event.Sequence
		default:
			close(sub.live)
			delete(q.subscribers, id)
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
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-sub.live:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case sub.out <- event:
			}
		}
	}
}

func (q *EventLogQuery) unsubscribe(id int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if sub, ok := q.subscribers[id]; ok {
		close(sub.live)
		delete(q.subscribers, id)
	}
}

type eventLogSubscriber struct {
	id         int64
	principal  auth.Principal
	projectID  kernel.ProjectID
	lastCursor evidence.Cursor
	live       chan UIEvent
	out        chan UIEvent
}

func formatEvidenceCursor(cursor evidence.Cursor) string {
	return strconv.FormatInt(int64(cursor), 10)
}
