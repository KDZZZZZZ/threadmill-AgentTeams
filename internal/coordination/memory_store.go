package coordination

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

type MemoryStore struct {
	mu       sync.Mutex
	projects map[kernel.ProjectID]*projectState
}

type projectState struct {
	latest  kernel.Revision
	history map[kernel.Revision]graphState
	current graphState
}

type graphState struct {
	tasks     map[kernel.TaskID]Task
	endpoints map[PhaseEndpointRef]PhaseEndpoint
	edges     []Edge
	blockers  map[string]Blocker
	results   map[string]PhaseResult
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{projects: make(map[kernel.ProjectID]*projectState)}
}

func (s *MemoryStore) Latest(_ context.Context, projectID kernel.ProjectID) (GraphSnapshot, error) {
	if kernel.IsZeroID(projectID) {
		return GraphSnapshot{}, kernel.InvalidArgument("project_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	return project.current.snapshot(project.latest), nil
}

func (s *MemoryStore) Snapshot(_ context.Context, projectID kernel.ProjectID, revision kernel.Revision) (GraphSnapshot, error) {
	if kernel.IsZeroID(projectID) {
		return GraphSnapshot{}, kernel.InvalidArgument("project_id is required")
	}
	if revision.IsLatestRead() {
		return GraphSnapshot{}, kernel.InvalidArgument("concrete revision is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	state, ok := project.history[revision]
	if !ok {
		return GraphSnapshot{}, kernel.Error{Code: kernel.CodeNotFound, Message: fmt.Sprintf("graph revision %d not found", revision), Recoverable: false}
	}
	return state.snapshot(revision), nil
}

func (s *MemoryStore) ReplacePending(_ context.Context, projectID kernel.ProjectID, next PendingSubgraph) (GraphSnapshot, error) {
	if kernel.IsZeroID(projectID) {
		return GraphSnapshot{}, kernel.InvalidArgument("project_id is required")
	}
	if kernel.IsZeroID(next.RequestID) {
		return GraphSnapshot{}, kernel.InvalidArgument("request_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	if err := kernel.CheckExpectedRevision(next.BaseRevision, project.latest); err != nil {
		return GraphSnapshot{}, err
	}
	state := project.current.clone()
	if err := validatePendingSubgraph(project.current, next); err != nil {
		return GraphSnapshot{}, err
	}
	applyPendingSubgraph(&state, next)
	if err := validateGraph(state); err != nil {
		return GraphSnapshot{}, err
	}
	return s.commit(project, state), nil
}

func (s *MemoryStore) Transition(_ context.Context, projectID kernel.ProjectID, expectedRevision kernel.Revision, transition GraphTransition) (GraphSnapshot, error) {
	if kernel.IsZeroID(projectID) {
		return GraphSnapshot{}, kernel.InvalidArgument("project_id is required")
	}
	if err := validateTransitionShape(transition); err != nil {
		return GraphSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	project := s.ensureProject(projectID)
	if err := kernel.CheckExpectedRevision(expectedRevision, project.latest); err != nil {
		return GraphSnapshot{}, err
	}
	state := project.current.clone()
	if err := applyTransition(&state, transition); err != nil {
		return GraphSnapshot{}, err
	}
	if err := validateGraph(state); err != nil {
		return GraphSnapshot{}, err
	}
	return s.commit(project, state), nil
}

func (s *MemoryStore) ensureProject(projectID kernel.ProjectID) *projectState {
	project := s.projects[projectID]
	if project == nil {
		project = &projectState{
			latest:  1,
			history: make(map[kernel.Revision]graphState),
			current: newGraphState(),
		}
		project.history[project.latest] = project.current.clone()
		s.projects[projectID] = project
	}
	return project
}

func (s *MemoryStore) commit(project *projectState, state graphState) GraphSnapshot {
	project.latest = project.latest.Next()
	project.current = state.clone()
	project.history[project.latest] = state.clone()
	return project.current.snapshot(project.latest)
}

func newGraphState() graphState {
	return graphState{
		tasks:     make(map[kernel.TaskID]Task),
		endpoints: make(map[PhaseEndpointRef]PhaseEndpoint),
		blockers:  make(map[string]Blocker),
		results:   make(map[string]PhaseResult),
	}
}

func (s graphState) clone() graphState {
	copied := newGraphState()
	for id, task := range s.tasks {
		copied.tasks[id] = task
	}
	for ref, endpoint := range s.endpoints {
		copied.endpoints[ref] = endpoint
	}
	copied.edges = cloneEdges(s.edges)
	for id, blocker := range s.blockers {
		copied.blockers[id] = blocker
	}
	for id, result := range s.results {
		copied.results[id] = result
	}
	return copied
}

func (s graphState) snapshot(revision kernel.Revision) GraphSnapshot {
	tasks := make([]Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })

	endpoints := make([]PhaseEndpoint, 0, len(s.endpoints))
	for _, endpoint := range s.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Ref.TaskID == endpoints[j].Ref.TaskID {
			return phaseOrder(endpoints[i].Ref.EndpointID) < phaseOrder(endpoints[j].Ref.EndpointID)
		}
		return endpoints[i].Ref.TaskID < endpoints[j].Ref.TaskID
	})

	edges := cloneEdges(s.edges)
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].To != edges[j].To {
			if edges[i].To.TaskID == edges[j].To.TaskID {
				return phaseOrder(edges[i].To.EndpointID) < phaseOrder(edges[j].To.EndpointID)
			}
			return edges[i].To.TaskID < edges[j].To.TaskID
		}
		if edges[i].From.TaskID == edges[j].From.TaskID {
			return phaseOrder(edges[i].From.EndpointID) < phaseOrder(edges[j].From.EndpointID)
		}
		return edges[i].From.TaskID < edges[j].From.TaskID
	})

	blockers := make([]Blocker, 0, len(s.blockers))
	for _, blocker := range s.blockers {
		blockers = append(blockers, blocker)
	}
	sort.Slice(blockers, func(i, j int) bool { return blockers[i].ID < blockers[j].ID })

	results := make([]PhaseResult, 0, len(s.results))
	for _, result := range s.results {
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })

	return GraphSnapshot{
		Revision:  revision,
		Tasks:     tasks,
		Endpoints: endpoints,
		Edges:     edges,
		Blockers:  blockers,
		Results:   results,
	}
}

func cloneEdges(edges []Edge) []Edge {
	if len(edges) == 0 {
		return nil
	}
	copied := make([]Edge, len(edges))
	for i, edge := range edges {
		copied[i] = edge
		copied[i].ArtifactKinds = append([]string(nil), edge.ArtifactKinds...)
	}
	return copied
}

func phaseOrder(id EndpointID) int {
	switch id {
	case EndpointPlan:
		return 0
	case EndpointExecute:
		return 1
	case EndpointVerify:
		return 2
	default:
		return 99
	}
}
