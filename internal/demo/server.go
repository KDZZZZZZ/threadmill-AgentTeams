package demo

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed web/*
var webFiles embed.FS

type Server struct {
	mu          sync.Mutex
	graphRev    int64
	capacity    Capacity
	tasks       map[string]*Task
	endpoints   map[string]*Endpoint
	edges       []Edge
	manager     ManagerLog
	invocations map[string][]Invocation
	subscribers map[chan []byte]struct{}
	nextID      int64
}

type Capacity struct {
	Desired  int `json:"desired"`
	Healthy  int `json:"healthy"`
	Active   int `json:"active"`
	Waiting  int `json:"waiting"`
	Revision int `json:"revision"`
}

type Task struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Phase    string `json:"phase"`
	Endpoint string `json:"endpoint"`
}

type Endpoint struct {
	ID              string   `json:"id"`
	TaskID          string   `json:"task_id"`
	Title           string   `json:"title"`
	Phase           string   `json:"phase"`
	Generation      int      `json:"generation"`
	Held            bool     `json:"held"`
	Satisfied       bool     `json:"satisfied"`
	Dependencies    []string `json:"dependencies"`
	LeaseID         string   `json:"lease_id,omitempty"`
	Checkpoint      string   `json:"checkpoint,omitempty"`
	FormalOutput    string   `json:"formal_output,omitempty"`
	LastDecisionRef string   `json:"last_decision_ref,omitempty"`
}

type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"`
}

type ManagerLog struct {
	Messages  []ManagerMessage `json:"messages"`
	Events    []ManagerEvent   `json:"events"`
	Decisions []Decision       `json:"decisions"`
}

type Decision struct {
	ManagerInputRef string    `json:"manager_input_ref"`
	DecisionRef     string    `json:"decision_ref"`
	EndpointID      string    `json:"endpoint_id"`
	Intent          string    `json:"intent"`
	CreatedAt       time.Time `json:"createdAt"`
}

type ManagerMessage struct {
	Text       string    `json:"text"`
	EndpointID string    `json:"endpoint_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	InputRef   string    `json:"input_ref"`
}

type ManagerEvent struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	EndpointID  string    `json:"endpoint_id,omitempty"`
	InputRef    string    `json:"input_ref,omitempty"`
	DecisionRef string    `json:"decision_ref,omitempty"`
	Text        string    `json:"text"`
	CreatedAt   time.Time `json:"created_at"`
}

type State struct {
	GraphRevision int64      `json:"graph_revision"`
	Capacity      Capacity   `json:"capacity"`
	Tasks         []Task     `json:"tasks"`
	Endpoints     []Endpoint `json:"endpoints"`
	Edges         []Edge     `json:"edges"`
	Manager       ManagerLog `json:"manager"`
}

type Invocation struct {
	ID           string    `json:"id"`
	EndpointID   string    `json:"endpoint_id"`
	Generation   int       `json:"generation"`
	LeaseID      string    `json:"lease_id"`
	Phase        string    `json:"phase"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	FormalOutput string    `json:"formal_output,omitempty"`
}

type Subscription struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Active   bool   `json:"active"`
	TargetID string `json:"target_id"`
}

type ContextItem struct {
	SourceEndpointID string `json:"source_endpoint_id"`
	Kind             string `json:"kind"`
	Text             string `json:"text"`
}

type Candidate struct {
	EndpointID               string `json:"endpoint_id"`
	Reason                   string `json:"reason"`
	CreatedByInvocationID    string `json:"created_by_invocation_id"`
	PrerequisitesSatisfied   bool   `json:"prerequisites_satisfied"`
	WouldExpandEffectiveView bool   `json:"would_expand_effective_view"`
}

type Inspector struct {
	EndpointID             string         `json:"endpoint_id"`
	Current                *Invocation    `json:"current"`
	Recent                 []Invocation   `json:"recent"`
	Subscriptions          []Subscription `json:"subscriptions"`
	EffectiveSubgraphUnion []string       `json:"effective_subgraph_union"`
	ContextSlice           []ContextItem  `json:"context_slice"`
	TaskMemoryBuffer       []string       `json:"task_memory_buffer"`
	Candidates             []Candidate    `json:"candidates"`
}

type capacityRequest struct {
	Desired          int `json:"desired"`
	ExpectedRevision int `json:"expected_revision"`
}

type managerMessageRequest struct {
	Text             string `json:"text"`
	EndpointID       string `json:"endpoint_id"`
	ExpectedRevision int64  `json:"expected_revision"`
}

func NewServer() *Server {
	s := &Server{
		graphRev: 0,
		capacity: Capacity{
			Desired:  2,
			Healthy:  4,
			Revision: 0,
		},
		tasks:       map[string]*Task{},
		endpoints:   map[string]*Endpoint{},
		invocations: map[string][]Invocation{},
		subscribers: map[chan []byte]struct{}{},
	}

	s.addEndpoint("task-brief", "ep-brief", "Brief accepted", "satisfied", nil)
	s.addEndpoint("task-retrieval", "ep-retrieval", "Retrieve source pack", "active", []string{"ep-brief"})
	s.addEndpoint("task-execute", "ep-execute", "Execute semantic core", "active", []string{"ep-brief"})
	s.addEndpoint("task-check", "ep-check", "Check runtime projection", "pending", []string{"ep-brief"})
	s.addEndpoint("task-review", "ep-review", "Review acceptance evidence", "pending", []string{"ep-brief"})
	s.addEndpoint("task-publish", "ep-publish", "Publish downstream summary", "pending", []string{"ep-execute", "ep-review"})
	s.rebuildEdges()
	s.startInvocationLocked(s.endpoints["ep-retrieval"], "initial active retrieval")
	s.startInvocationLocked(s.endpoints["ep-execute"], "initial active execution")
	s.refreshCapacityLocked(false)
	s.appendEventLocked("evt-initial", "state", "", "", "", "initial semantic demo graph")
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/endpoints/{id}/inspector", s.handleInspector)
	mux.HandleFunc("POST /api/capacity", s.handleCapacity)
	mux.HandleFunc("POST /api/manager/messages", s.handleManagerMessage)
	mux.HandleFunc("/api/invocations/{id}/hold", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "invocation control is available only through manager decisions")
	})
	mux.HandleFunc("/api/invocations/{id}/resume", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "invocation control is available only through manager decisions")
	})
	mux.HandleFunc("/api/graph/endpoints", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "direct graph mutation endpoint is not available")
	})
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.Handle("/", s.webHandler())
	return mux
}

func (s *Server) addEndpoint(taskID, endpointID, title, phase string, deps []string) {
	s.tasks[taskID] = &Task{ID: taskID, Title: title, Phase: phase, Endpoint: endpointID}
	s.endpoints[endpointID] = &Endpoint{
		ID:           endpointID,
		TaskID:       taskID,
		Title:        title,
		Phase:        phase,
		Satisfied:    phase == "satisfied",
		Dependencies: append([]string(nil), deps...),
	}
}

func (s *Server) webHandler() http.Handler {
	sub, err := fs.Sub(webFiles, "web")
	if err != nil {
		return http.NotFoundHandler()
	}
	return http.FileServer(http.FS(sub))
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.snapshot())
}

func (s *Server) handleInspector(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.endpoints[id]; !ok {
		writeError(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	s.writeJSON(w, http.StatusOK, s.inspectorLocked(id))
}

func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	var req capacityRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Desired < 0 || req.Desired > s.capacity.Healthy {
		writeError(w, http.StatusBadRequest, "desired must be between 0 and healthy capacity")
		return
	}

	s.mu.Lock()
	if req.ExpectedRevision != s.capacity.Revision {
		current := s.stateLocked()
		s.mu.Unlock()
		s.writeJSON(w, http.StatusConflict, map[string]any{"error": "capacity revision mismatch", "state": current})
		return
	}
	s.capacity.Desired = req.Desired
	s.capacity.Revision++
	s.refreshCapacityLocked(true)
	state := s.stateLocked()
	s.mu.Unlock()

	s.broadcastState()
	s.writeJSON(w, http.StatusOK, state)
}

func (s *Server) handleManagerMessage(w http.ResponseWriter, r *http.Request) {
	var req managerMessageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Text = strings.TrimSpace(req.Text)
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "text is required")
		return
	}

	s.mu.Lock()
	if req.ExpectedRevision != s.graphRev {
		current := s.stateLocked()
		s.mu.Unlock()
		s.writeJSON(w, http.StatusConflict, map[string]any{"error": "graph revision mismatch", "state": current})
		return
	}

	endpointID := req.EndpointID
	if endpointID == "" {
		endpointID = s.defaultManagerTargetLocked()
	}
	ep, ok := s.endpoints[endpointID]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "unknown endpoint")
		return
	}

	inputRef := s.nextRefLocked("manager-input")
	decisionRef := s.nextRefLocked("decision")
	msg := ManagerMessage{
		Text:       req.Text,
		EndpointID: endpointID,
		CreatedAt:  time.Now().UTC(),
		InputRef:   inputRef,
	}
	messageCount := len(s.manager.Messages)
	decisionCount := len(s.manager.Decisions)
	s.manager.Messages = append(s.manager.Messages, msg)

	result, err := s.applyManagerIntentLocked(req.Text, ep, inputRef, decisionRef)
	if err != nil {
		s.manager.Messages = s.manager.Messages[:messageCount]
		s.manager.Decisions = s.manager.Decisions[:decisionCount]
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.graphRev++
	s.refreshCapacityLocked(true)
	state := s.stateLocked()
	s.mu.Unlock()

	s.broadcastState()
	s.writeJSON(w, http.StatusOK, map[string]any{
		"input_ref":      inputRef,
		"decision_ref":   decisionRef,
		"endpoint_id":    endpointID,
		"graph_revision": state.GraphRevision,
		"result":         result,
		"events":         state.Manager.Events,
		"state":          state,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 8)
	s.mu.Lock()
	ch <- mustJSON(s.stateLocked())
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		close(ch)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case payload := <-ch:
			if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) applyManagerIntentLocked(text string, ep *Endpoint, inputRef, decisionRef string) (string, error) {
	intent := detectIntent(text)
	if intent == "complete-named" {
		if targetID := parseEndpointAfter(text, "endpoint"); targetID != "" {
			if target := s.endpoints[targetID]; target != nil {
				ep = target
			}
		}
		intent = "satisfy"
	}
	s.manager.Decisions = append(s.manager.Decisions, Decision{
		ManagerInputRef: inputRef,
		DecisionRef:     decisionRef,
		EndpointID:      ep.ID,
		Intent:          intent,
		CreatedAt:       time.Now().UTC(),
	})
	switch intent {
	case "create":
		n := 1
		if strings.Contains(strings.ToLower(text), "three") || strings.Contains(text, "三个") {
			n = 3
		} else if strings.Contains(strings.ToLower(text), "two") || strings.Contains(text, "两个") {
			n = 2
		}
		created := s.createRunnableEndpointsLocked(n, text)
		s.rebuildEdges()
		s.appendEventLocked(s.nextRefLocked("event"), "manager.create", created[0], inputRef, decisionRef, fmt.Sprintf("created %d endpoint(s)", len(created)))
		return fmt.Sprintf("created %d endpoint(s)", len(created)), nil
	case "hold":
		if len(s.invocations[ep.ID]) == 0 {
			s.startInvocationLocked(ep, "manager checkpoint dispatch")
		}
		ep.Held = true
		ep.Phase = "held"
		ep.Checkpoint = s.nextRefLocked("checkpoint")
		ep.LastDecisionRef = decisionRef
		s.completeCurrentLocked(ep.ID, "recoverable checkpoint")
		s.appendEventLocked(s.nextRefLocked("event"), "manager.hold", ep.ID, inputRef, decisionRef, "held with recoverable stop/checkpoint")
		return "held", nil
	case "resume":
		ep.Held = false
		ep.Satisfied = false
		ep.Phase = "pending"
		if ep.Generation == 0 {
			ep.Generation = 1
		}
		ep.Generation++
		ep.LeaseID = ""
		ep.LastDecisionRef = decisionRef
		s.appendEventLocked(s.nextRefLocked("event"), "manager.resume", ep.ID, inputRef, decisionRef, "released with new generation")
		s.startInvocationLocked(ep, "manager resume")
		return "released", nil
	case "satisfy":
		ep.Held = false
		ep.Satisfied = true
		ep.Phase = "satisfied"
		ep.FormalOutput = "simulated formal output accepted by TaskManager"
		ep.LastDecisionRef = decisionRef
		s.completeCurrentLocked(ep.ID, ep.FormalOutput)
		s.appendEventLocked(s.nextRefLocked("event"), "manager.satisfy", ep.ID, inputRef, decisionRef, "formal output satisfied endpoint and unlocked downstream")
		return "satisfied", nil
	case "dependency":
		if ep.Phase != "pending" && ep.Phase != "held" {
			return "", errors.New("dependency can only be added to a pending or held endpoint")
		}
		depID, ok := s.findDependencyCandidateLocked(ep.ID)
		if !ok {
			return "", errors.New("no existing endpoint prerequisite candidate")
		}
		ep.Dependencies = append(ep.Dependencies, depID)
		sort.Strings(ep.Dependencies)
		ep.LastDecisionRef = decisionRef
		s.rebuildEdges()
		s.appendEventLocked(s.nextRefLocked("event"), "manager.dependency", ep.ID, inputRef, decisionRef, "added prerequisite "+depID)
		return "dependency added: " + depID, nil
	default:
		return "", errors.New("unrecognized manager intent")
	}
}

func detectIntent(text string) string {
	t := strings.ToLower(text)
	switch {
	case strings.Contains(t, "创建"), strings.Contains(t, "新增"), strings.Contains(t, "create"):
		return "create"
	case strings.Contains(t, "complete endpoint"):
		return "complete-named"
	case strings.Contains(t, "暂停"), strings.Contains(t, "停止"), strings.Contains(t, "hold"), strings.Contains(t, "stop"):
		return "hold"
	case strings.Contains(t, "恢复"), strings.Contains(t, "继续"), strings.Contains(t, "resume"), strings.Contains(t, "continue"):
		return "resume"
	case strings.Contains(t, "完成"), strings.Contains(t, "通过"), strings.Contains(t, "satisfy"), strings.Contains(t, "complete"):
		return "satisfy"
	case strings.Contains(t, "前置"), strings.Contains(t, "dependency"), strings.Contains(t, "prerequisite"):
		return "dependency"
	default:
		return ""
	}
}

func (s *Server) createRunnableEndpointsLocked(n int, text string) []string {
	created := make([]string, 0, n)
	base := "manager"
	words := strings.Fields(strings.ToLower(text))
	for _, word := range words {
		word = strings.Trim(word, ",.;:")
		if word == "alpha" || word == "beta" || word == "gamma" || strings.Contains(word, "plan") || strings.Contains(word, "execute") {
			base = word
			break
		}
	}
	for i := 0; i < n; i++ {
		s.nextID++
		id := fmt.Sprintf("ep-%s-%04d", base, s.nextID)
		taskID := strings.Replace(id, "ep-", "task-", 1)
		title := fmt.Sprintf("Manager-created %s %d", base, i+1)
		s.addEndpoint(taskID, id, title, "pending", []string{"ep-brief"})
		created = append(created, id)
	}
	return created
}

func parseEndpointAfter(text, marker string) string {
	fields := strings.Fields(text)
	for i, field := range fields {
		if strings.EqualFold(strings.Trim(field, ":,"), marker) && i+1 < len(fields) {
			return strings.Trim(fields[i+1], " ,.;:")
		}
	}
	return ""
}

func (s *Server) refreshCapacityLocked(dispatch bool) {
	if dispatch {
		for s.activeCountLocked() < s.capacity.Desired {
			ep := s.nextRunnableLocked()
			if ep == nil {
				break
			}
			s.startInvocationLocked(ep, "capacity dispatch")
		}
	}
	s.capacity.Active = s.activeCountLocked()
	s.capacity.Waiting = len(s.runnableLocked())
}

func (s *Server) startInvocationLocked(ep *Endpoint, reason string) {
	if ep.Held || ep.Satisfied {
		return
	}
	ep.Phase = "active"
	if ep.Generation == 0 {
		ep.Generation = 1
	}
	leaseID := s.nextRefLocked("lease")
	ep.LeaseID = leaseID
	inv := Invocation{
		ID:         s.nextRefLocked("invocation"),
		EndpointID: ep.ID,
		Generation: ep.Generation,
		LeaseID:    leaseID,
		Phase:      "execute",
		StartedAt:  time.Now().UTC(),
	}
	s.invocations[ep.ID] = append([]Invocation{inv}, s.invocations[ep.ID]...)
	s.appendEventLocked(s.nextRefLocked("event"), "runtime.dispatch", ep.ID, "", "", reason)
}

func (s *Server) completeCurrentLocked(endpointID, output string) {
	invs := s.invocations[endpointID]
	if len(invs) == 0 {
		return
	}
	invs[0].Phase = "completed"
	invs[0].CompletedAt = time.Now().UTC()
	invs[0].FormalOutput = output
	s.invocations[endpointID] = invs
}

func (s *Server) nextRunnableLocked() *Endpoint {
	for _, id := range sortedEndpointIDs(s.endpoints) {
		ep := s.endpoints[id]
		if s.isRunnableLocked(ep) {
			return ep
		}
	}
	return nil
}

func (s *Server) runnableLocked() []*Endpoint {
	var out []*Endpoint
	for _, id := range sortedEndpointIDs(s.endpoints) {
		ep := s.endpoints[id]
		if s.isRunnableLocked(ep) {
			out = append(out, ep)
		}
	}
	return out
}

func (s *Server) isRunnableLocked(ep *Endpoint) bool {
	if ep.Phase != "pending" || ep.Held || ep.Satisfied {
		return false
	}
	for _, depID := range ep.Dependencies {
		dep := s.endpoints[depID]
		if dep == nil || !dep.Satisfied {
			return false
		}
	}
	return true
}

func (s *Server) activeCountLocked() int {
	active := 0
	for _, ep := range s.endpoints {
		if ep.Phase == "active" {
			active++
		}
	}
	return active
}

func (s *Server) defaultManagerTargetLocked() string {
	for _, id := range sortedEndpointIDs(s.endpoints) {
		if s.endpoints[id].Phase == "pending" || s.endpoints[id].Phase == "held" {
			return id
		}
	}
	for _, id := range sortedEndpointIDs(s.endpoints) {
		return id
	}
	return ""
}

func (s *Server) findDependencyCandidateLocked(endpointID string) (string, bool) {
	ep := s.endpoints[endpointID]
	if ep == nil {
		return "", false
	}
	existing := map[string]bool{endpointID: true}
	for _, depID := range ep.Dependencies {
		existing[depID] = true
	}
	for _, id := range sortedEndpointIDs(s.endpoints) {
		if !existing[id] {
			return id, true
		}
	}
	return "", false
}

func (s *Server) rebuildEdges() {
	edges := make([]Edge, 0)
	for _, id := range sortedEndpointIDs(s.endpoints) {
		for _, depID := range s.endpoints[id].Dependencies {
			edges = append(edges, Edge{From: depID, To: id, Type: "prerequisite"})
		}
	}
	s.edges = edges
}

func (s *Server) appendEventLocked(id, typ, endpointID, inputRef, decisionRef, text string) {
	s.manager.Events = append(s.manager.Events, ManagerEvent{
		ID:          id,
		Type:        typ,
		EndpointID:  endpointID,
		InputRef:    inputRef,
		DecisionRef: decisionRef,
		Text:        text,
		CreatedAt:   time.Now().UTC(),
	})
}

func (s *Server) snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateLocked()
}

func (s *Server) stateLocked() State {
	s.refreshCapacityLocked(false)
	state := State{
		GraphRevision: s.graphRev,
		Capacity:      s.capacity,
		Edges:         append([]Edge(nil), s.edges...),
		Manager: ManagerLog{
			Messages:  append([]ManagerMessage(nil), s.manager.Messages...),
			Events:    append([]ManagerEvent(nil), s.manager.Events...),
			Decisions: append([]Decision(nil), s.manager.Decisions...),
		},
	}
	for _, id := range sortedTaskIDs(s.tasks) {
		state.Tasks = append(state.Tasks, *s.tasks[id])
	}
	for _, id := range sortedEndpointIDs(s.endpoints) {
		state.Endpoints = append(state.Endpoints, copyEndpoint(s.endpoints[id]))
	}
	return state
}

func (s *Server) inspectorLocked(id string) Inspector {
	ep := s.endpoints[id]
	recent := append([]Invocation(nil), s.invocations[id]...)
	if recent == nil {
		recent = []Invocation{}
	}
	var current *Invocation
	if len(recent) > 0 && recent[0].Phase == "execute" && ep.Phase == "active" {
		inv := recent[0]
		current = &inv
	}

	// The runtime context projection is derived from the selected agent's current
	// subscription union. Historical generations remain visible but inactive.
	var subscriptions []Subscription
	for index, invocation := range recent {
		active := index == 0 && ep.Phase == "active" && invocation.Phase == "execute"
		subscriptions = append(subscriptions, subscriptionsForInvocation(ep, invocation, active)...)
	}
	if len(recent) == 0 {
		planned := Invocation{EndpointID: id, Generation: ep.Generation, LeaseID: ep.LeaseID}
		subscriptions = subscriptionsForInvocation(ep, planned, false)
	}

	currentGeneration := ep.Generation
	unionSet := map[string]bool{}
	for _, subscription := range subscriptions {
		if subscription.Kind != "lease" && subscription.TargetID != "" && strings.Contains(subscription.ID, fmt.Sprintf(":g%d:", currentGeneration)) {
			unionSet[subscription.TargetID] = true
		}
	}
	union := make([]string, 0, len(unionSet))
	for endpointID := range unionSet {
		union = append(union, endpointID)
	}
	sort.Strings(union)

	contextSlice := make([]ContextItem, 0, len(union))
	for _, endpointID := range union {
		kind := "subscription-output"
		text := s.endpointMemoryLocked(endpointID)
		switch {
		case endpointID == id:
			kind = "endpoint-scope"
			text = ep.Title + " generation " + fmt.Sprint(ep.Generation)
		case containsString(ep.Dependencies, endpointID):
			kind = "dependency-output"
		case endpointID == "ep-retrieval":
			kind = "retrieval-output"
		}
		contextSlice = append(contextSlice, ContextItem{SourceEndpointID: endpointID, Kind: kind, Text: text})
	}

	createdBy := ""
	if len(recent) > 0 {
		createdBy = recent[0].ID
	}
	if createdBy == "" {
		createdBy = "bootstrap"
	}
	candidates := []Candidate{}
	for _, candidate := range sortedEndpointIDs(s.endpoints) {
		if candidate == id {
			continue
		}
		candidates = append(candidates, Candidate{
			EndpointID:               candidate,
			Reason:                   "available endpoint memory can extend execution context",
			CreatedByInvocationID:    createdBy,
			PrerequisitesSatisfied:   s.dependenciesSatisfiedLocked(candidate),
			WouldExpandEffectiveView: !unionSet[candidate],
		})
	}

	return Inspector{
		EndpointID:             id,
		Current:                current,
		Recent:                 recent,
		Subscriptions:          subscriptions,
		EffectiveSubgraphUnion: union,
		ContextSlice:           contextSlice,
		TaskMemoryBuffer: []string{
			"owner_task=" + ep.TaskID,
			"owner_endpoint=" + ep.ID,
			"phase=" + ep.Phase,
			"generation=" + fmt.Sprint(ep.Generation),
			"checkpoint=" + ep.Checkpoint,
		},
		Candidates: candidates,
	}
}

func subscriptionsForInvocation(ep *Endpoint, invocation Invocation, active bool) []Subscription {
	prefix := fmt.Sprintf("%s:g%d", ep.ID, invocation.Generation)
	subscriptions := []Subscription{
		{ID: prefix + ":initial", Kind: "initial", Active: active, TargetID: ep.ID},
		{ID: prefix + ":retrieval", Kind: "retrieval", Active: active, TargetID: "ep-retrieval"},
	}
	for _, dependency := range ep.Dependencies {
		subscriptions = append(subscriptions, Subscription{
			ID:       prefix + ":explicit:" + dependency,
			Kind:     "explicit",
			Active:   active,
			TargetID: dependency,
		})
	}
	if invocation.LeaseID != "" {
		subscriptions = append(subscriptions, Subscription{
			ID:       prefix + ":lease",
			Kind:     "lease",
			Active:   active,
			TargetID: invocation.LeaseID,
		})
	}
	return subscriptions
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (s *Server) endpointMemoryLocked(id string) string {
	ep := s.endpoints[id]
	if ep == nil {
		return "missing endpoint"
	}
	if ep.FormalOutput != "" {
		return ep.FormalOutput
	}
	return ep.Title + " (" + ep.Phase + ")"
}

func (s *Server) dependenciesSatisfiedLocked(id string) bool {
	ep := s.endpoints[id]
	if ep == nil {
		return false
	}
	for _, depID := range ep.Dependencies {
		dep := s.endpoints[depID]
		if dep == nil || !dep.Satisfied {
			return false
		}
	}
	return true
}

func (s *Server) broadcastState() {
	payload := mustJSON(s.snapshot())
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- payload:
		default:
		}
	}
}

func (s *Server) nextRefLocked(prefix string) string {
	s.nextID++
	return fmt.Sprintf("%s-%04d", prefix, s.nextID)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func copyEndpoint(ep *Endpoint) Endpoint {
	cp := *ep
	cp.Dependencies = append([]string(nil), ep.Dependencies...)
	return cp
}

func sortedEndpointIDs(endpoints map[string]*Endpoint) []string {
	ids := make([]string, 0, len(endpoints))
	for id := range endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sortedTaskIDs(tasks map[string]*Task) []string {
	ids := make([]string, 0, len(tasks))
	for id := range tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
