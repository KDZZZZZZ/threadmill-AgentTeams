package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/coordination"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/platform/auth"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/uiprojection"
)

type Server struct {
	auth              Authenticator
	csrf              StateChangeGuard
	reqs              RequirementCommandPort
	capacity          CapacityPort
	human             HumanDecisionPort
	manager           ManagerPort
	query             QueryPort
	readiness         ReadinessPort
	events            uiprojection.EventQuery
	eventStreamBuffer int
}

type Options struct {
	Authenticator     Authenticator
	CSRFGuard         StateChangeGuard
	Requirements      RequirementCommandPort
	Capacity          CapacityPort
	Human             HumanDecisionPort
	Manager           ManagerPort
	Query             QueryPort
	Readiness         ReadinessPort
	Events            uiprojection.EventQuery
	EventStreamBuffer int
}

func New(options Options) *Server {
	return &Server{
		auth:              options.Authenticator,
		csrf:              options.CSRFGuard,
		reqs:              options.Requirements,
		capacity:          options.Capacity,
		human:             options.Human,
		manager:           options.Manager,
		query:             options.Query,
		readiness:         options.Readiness,
		events:            options.Events,
		eventStreamBuffer: options.EventStreamBuffer,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/requirements", s.handleRequirements)
	mux.HandleFunc("/v1/capacity", s.handleCapacity)
	mux.HandleFunc("/v1/capacity-adjustments", s.handleCapacityAdjustments)
	mux.HandleFunc("/v1/human-decisions", s.handleHumanDecisions)
	mux.HandleFunc("/v1/tasks/", s.handleTask)
	mux.HandleFunc("/v1/coordination/snapshot", s.handleCoordinationSnapshot)
	mux.HandleFunc("/v1/context/snapshot", s.handleContextSnapshot)
	mux.HandleFunc("/v1/coordination/endpoints/", s.handleEndpointInspector)
	mux.HandleFunc("/v1/manager/messages", s.handleManagerMessages)
	mux.HandleFunc("/v1/manager/conversations/", s.handleManagerConversation)
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/v1/events/stream", s.handleEventStream)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReadiness)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeResult(w, HealthStatus{Status: "ok"}, nil, http.StatusOK)
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	if s.readiness == nil {
		writeResult(w, ReadinessStatus{Status: "not_ready", Dependencies: []DependencyReadiness{}}, nil, http.StatusServiceUnavailable)
		return
	}
	status := s.readiness.Readiness(r.Context())
	if status.Dependencies == nil {
		status.Dependencies = []DependencyReadiness{}
	}
	httpStatus := http.StatusOK
	if status.Status != "ready" {
		httpStatus = http.StatusServiceUnavailable
	}
	writeResult(w, status, nil, httpStatus)
}

func (s *Server) handleRequirements(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req RequirementCreateRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	principal, ok := s.authenticateWrite(w, r, req.ProjectID)
	if !ok {
		return
	}
	resp, err := s.reqs.SubmitRequirement(r.Context(), principal, req)
	writeResult(w, resp, err, http.StatusAccepted)
}

func (s *Server) handleCapacity(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	resp, err := s.capacity.GetCapacity(r.Context(), principal, projectID)
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) handleCapacityAdjustments(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req CapacityAdjustmentRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	principal, ok := s.authenticateWrite(w, r, req.ProjectID)
	if !ok {
		return
	}
	resp, err := s.capacity.AdjustCapacity(r.Context(), principal, req)
	writeResult(w, resp, err, http.StatusAccepted)
}

func (s *Server) handleHumanDecisions(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req HumanDecisionRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	principal, ok := s.authenticateWrite(w, r, req.ProjectID)
	if !ok {
		return
	}
	resp, err := s.human.SubmitHumanDecision(r.Context(), principal, req)
	writeResult(w, resp, err, http.StatusAccepted)
}

func (s *Server) handleManagerMessages(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req ManagerMessageRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	principal, ok := s.authenticateWrite(w, r, req.ProjectID)
	if !ok {
		return
	}
	resp, err := s.manager.SubmitManagerMessage(r.Context(), principal, req)
	writeResult(w, resp, err, http.StatusAccepted)
}

func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	taskID := kernel.TaskID(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"))
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	resp, err := s.query.Task(r.Context(), principal, taskID)
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) handleCoordinationSnapshot(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	revision, err := parseRevision(r.URL.Query().Get("revision"))
	if err != nil {
		writeError(w, err)
		return
	}
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	resp, err := s.query.ProjectSnapshot(r.Context(), principal, projectID, revision)
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) handleContextSnapshot(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	resp, err := s.query.ContextSnapshot(r.Context(), principal, projectID)
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) handleEndpointInspector(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	ref, ok := parseEndpointInspectorPath(w, r.URL.Path)
	if !ok {
		return
	}
	rawGeneration := r.URL.Query().Get("generation")
	generation, err := parseIntDefault(rawGeneration, 0)
	if err != nil {
		writeError(w, err)
		return
	}
	if rawGeneration != "" && generation < 1 {
		writeError(w, kernel.InvalidArgument("generation must be at least 1"))
		return
	}
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, authed := s.authenticateRead(w, r, projectID)
	if !authed {
		return
	}
	resp, err := s.query.InspectEndpoint(r.Context(), principal, ref, generation)
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) handleManagerConversation(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	conversationID := strings.TrimPrefix(r.URL.Path, "/v1/manager/conversations/")
	projectID := kernel.ProjectID(r.URL.Query().Get("project_id"))
	principal, ok := s.authenticateRead(w, r, projectID)
	if !ok {
		return
	}
	resp, err := s.manager.Conversation(r.Context(), principal, conversationID, r.URL.Query().Get("after"))
	writeResult(w, resp, err, http.StatusOK)
}

func (s *Server) authenticateRead(w http.ResponseWriter, r *http.Request, projectID kernel.ProjectID) (auth.Principal, bool) {
	if projectID == "" {
		writeError(w, kernel.InvalidArgument("project_id is required"))
		return auth.Principal{}, false
	}
	cookie, err := r.Cookie(auth.SessionCookieName)
	if err != nil {
		writeError(w, kernel.Error{Code: kernel.CodeUnauthorized, Message: "operator session cookie is required"})
		return auth.Principal{}, false
	}
	principal, _, err := s.auth.AuthenticateOperatorSession(r.Context(), cookie.Value, projectID)
	if err != nil {
		writeError(w, err)
		return auth.Principal{}, false
	}
	return principal, true
}

func (s *Server) authenticateWrite(w http.ResponseWriter, r *http.Request, projectID kernel.ProjectID) (auth.Principal, bool) {
	principal, session, ok := s.authenticateWithSession(w, r, projectID)
	if !ok {
		return auth.Principal{}, false
	}
	if err := s.csrf.Check(r, session); err != nil {
		writeError(w, err)
		return auth.Principal{}, false
	}
	return principal, true
}

func (s *Server) authenticateWithSession(w http.ResponseWriter, r *http.Request, projectID kernel.ProjectID) (auth.Principal, auth.SessionRecord, bool) {
	if projectID == "" {
		writeError(w, kernel.InvalidArgument("project_id is required"))
		return auth.Principal{}, auth.SessionRecord{}, false
	}
	cookie, err := r.Cookie(auth.SessionCookieName)
	if err != nil {
		writeError(w, kernel.Error{Code: kernel.CodeUnauthorized, Message: "operator session cookie is required"})
		return auth.Principal{}, auth.SessionRecord{}, false
	}
	principal, session, err := s.auth.AuthenticateOperatorSession(r.Context(), cookie.Value, projectID)
	if err != nil {
		writeError(w, err)
		return auth.Principal{}, auth.SessionRecord{}, false
	}
	return principal, session, true
}

func decodeRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := strictDecode(w, r, target); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

func writeResult(w http.ResponseWriter, value any, err error, status int) {
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	code := kernel.ErrorCodeOf(err)
	status := http.StatusInternalServerError
	switch code {
	case kernel.CodeInvalidRequest:
		status = http.StatusBadRequest
	case kernel.CodeUnauthorized:
		status = http.StatusUnauthorized
	case kernel.CodeForbidden, kernel.CodeCSRFInvalid, kernel.CodeOriginInvalid:
		status = http.StatusForbidden
	case kernel.CodeNotFound:
		status = http.StatusNotFound
	case kernel.CodeRevisionConflict, kernel.CodeIdempotencyConflict, kernel.CodeCommandConflict, kernel.CodeStaleBinding, kernel.CodeStaleCommand, kernel.CodeStaleCheckpoint, kernel.CodeLeaseConflict, kernel.CodeScopeNotPending, kernel.CodeEndpointInFlight, kernel.CodeIncompleteStopEvidence, kernel.CodeTransitionRejected:
		status = http.StatusConflict
	case kernel.CodeExecutorUnavailable:
		status = http.StatusServiceUnavailable
	}
	var coded kernel.Error
	if !errors.As(err, &coded) {
		coded = kernel.Error{Code: code, Message: err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(coded)
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeCodedError(w, kernel.Error{Code: kernel.CodeInvalidRequest, Message: "method not allowed", Recoverable: false}, http.StatusMethodNotAllowed)
	return false
}

func parseEndpointInspectorPath(w http.ResponseWriter, path string) (coordination.PhaseEndpointRef, bool) {
	rest := strings.TrimPrefix(path, "/v1/coordination/endpoints/")
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] != "inspector" {
		writeError(w, kernel.Error{Code: kernel.CodeNotFound, Message: "endpoint inspector path not found"})
		return coordination.PhaseEndpointRef{}, false
	}
	if utf8.RuneCountInString(parts[0]) > 128 || !validEndpointID(coordination.EndpointID(parts[1])) {
		writeError(w, kernel.InvalidArgument("endpoint inspector path contains an invalid task_id or endpoint_id"))
		return coordination.PhaseEndpointRef{}, false
	}
	return coordination.PhaseEndpointRef{TaskID: kernel.TaskID(parts[0]), EndpointID: coordination.EndpointID(parts[1])}, true
}

func parseRevision(raw string) (kernel.Revision, error) {
	if raw == "" {
		return kernel.LatestRevision, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, kernel.InvalidArgument("revision must be an unsigned integer")
	}
	return kernel.Revision(value), nil
}

func parseIntDefault(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, kernel.InvalidArgument("integer query parameter is invalid")
	}
	return value, nil
}

func (r RequirementCreateRequest) validate() error {
	if strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(string(r.ProjectID)) == "" || strings.TrimSpace(r.Body) == "" {
		return kernel.InvalidArgument("request_id, project_id, and body are required")
	}
	if err := validateLength("request_id", r.RequestID, 128); err != nil {
		return err
	}
	if err := validateLength("project_id", string(r.ProjectID), 128); err != nil {
		return err
	}
	if err := validateOptionalLength("conversation_id", r.ConversationID, 128); err != nil {
		return err
	}
	if err := validateLength("body", r.Body, 50000); err != nil {
		return err
	}
	if err := validateOptionalLength("motivation", r.Motivation, 20000); err != nil {
		return err
	}
	for _, value := range append(append([]string(nil), r.Constraints...), r.Acceptance...) {
		if utf8.RuneCountInString(value) > 4000 {
			return kernel.InvalidArgument("constraint and acceptance entries must not exceed 4000 characters")
		}
	}
	if r.Source != nil {
		if r.Source.Kind != "" && !oneOf(r.Source.Kind, "browser", "api", "import") {
			return kernel.InvalidArgument("source.kind must be browser, api, or import")
		}
		if err := validateOptionalLength("source.external_ref", r.Source.ExternalRef, 512); err != nil {
			return err
		}
	}
	return nil
}

func (r CapacityAdjustmentRequest) validate() error {
	if strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(string(r.ProjectID)) == "" {
		return kernel.InvalidArgument("request_id and project_id are required")
	}
	if err := validateLength("request_id", r.RequestID, 128); err != nil {
		return err
	}
	if err := validateLength("project_id", string(r.ProjectID), 128); err != nil {
		return err
	}
	if r.ExpectedRevision < 0 {
		return kernel.InvalidArgument("expected_revision must be non-negative")
	}
	if r.DesiredConcurrency < 0 || r.DesiredConcurrency > 10000 {
		return kernel.InvalidArgument("desired_concurrency must be from 0 to 10000")
	}
	return nil
}

func (r ManagerMessageRequest) validate() error {
	if strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(string(r.ProjectID)) == "" || strings.TrimSpace(r.ConversationID) == "" || strings.TrimSpace(r.Body) == "" {
		return kernel.InvalidArgument("request_id, project_id, conversation_id, and body are required")
	}
	if err := validateLength("request_id", r.RequestID, 128); err != nil {
		return err
	}
	if err := validateLength("project_id", string(r.ProjectID), 128); err != nil {
		return err
	}
	if err := validateLength("conversation_id", r.ConversationID, 128); err != nil {
		return err
	}
	if err := validateLength("body", r.Body, 50000); err != nil {
		return err
	}
	intent := r.Intent
	if intent == "" {
		intent = ManagerIntentOrchestrate
	}
	if intent != ManagerIntentOrchestrate && intent != ManagerIntentHold && intent != ManagerIntentResume {
		return kernel.InvalidArgument("intent must be orchestrate, hold, or resume")
	}
	if r.SelectedEndpoint != nil {
		if strings.TrimSpace(string(r.SelectedEndpoint.TaskID)) == "" || !validEndpointID(r.SelectedEndpoint.EndpointID) {
			return kernel.InvalidArgument("selected_endpoint requires a task_id and plan, execute, or verify endpoint_id")
		}
		if err := validateLength("selected_endpoint.task_id", string(r.SelectedEndpoint.TaskID), 128); err != nil {
			return err
		}
	}
	if (intent == ManagerIntentHold || intent == ManagerIntentResume) && r.SelectedEndpoint == nil {
		return kernel.InvalidArgument("hold and resume intents require selected_endpoint")
	}
	return nil
}

func (r HumanDecisionRequest) validate() error {
	if strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(string(r.ProjectID)) == "" || strings.TrimSpace(r.Target.Kind) == "" || strings.TrimSpace(r.Target.Ref) == "" || strings.TrimSpace(r.Decision) == "" || strings.TrimSpace(r.Reason) == "" {
		return kernel.InvalidArgument("request_id, project_id, target, decision, and reason are required")
	}
	if err := validateLength("request_id", r.RequestID, 128); err != nil {
		return err
	}
	if err := validateLength("project_id", string(r.ProjectID), 128); err != nil {
		return err
	}
	if !oneOf(r.Target.Kind, "task", "endpoint", "blocker", "requirement", "manager_input") {
		return kernel.InvalidArgument("target.kind is invalid")
	}
	if err := validateLength("target.ref", r.Target.Ref, 256); err != nil {
		return err
	}
	if !oneOf(r.Decision, "approve", "reject", "defer", "cancel", "answer") {
		return kernel.InvalidArgument("decision must be approve, reject, defer, cancel, or answer")
	}
	if err := validateLength("reason", r.Reason, 20000); err != nil {
		return err
	}
	for _, artifact := range r.EvidenceRefs {
		if err := artifact.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r ArtifactRef) validate() error {
	if r.ArtifactID == "" || r.SHA256 == "" || r.MediaType == "" {
		return kernel.InvalidArgument("evidence artifact_id, sha256, media_type, and size_bytes are required")
	}
	if len(r.SHA256) != 64 {
		return kernel.InvalidArgument("evidence sha256 must contain 64 lowercase hexadecimal characters")
	}
	for _, character := range r.SHA256 {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return kernel.InvalidArgument("evidence sha256 must contain 64 lowercase hexadecimal characters")
			}
		}
	}
	if r.SizeBytes < 0 {
		return kernel.InvalidArgument("evidence size_bytes must be non-negative")
	}
	return nil
}

func validateLength(field, value string, maximum int) error {
	length := utf8.RuneCountInString(value)
	if length < 1 || length > maximum {
		return kernel.InvalidArgument(field + " must contain from 1 to " + strconv.Itoa(maximum) + " characters")
	}
	return nil
}

func validateOptionalLength(field, value string, maximum int) error {
	if value == "" {
		return nil
	}
	return validateLength(field, value, maximum)
}

func validEndpointID(endpointID coordination.EndpointID) bool {
	return endpointID == coordination.EndpointPlan || endpointID == coordination.EndpointExecute || endpointID == coordination.EndpointVerify
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

type validator interface {
	validate() error
}

func validateDecoded(value any) error {
	if v, ok := value.(validator); ok {
		return v.validate()
	}
	return nil
}

func strictDecode(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return kernel.InvalidArgument("invalid JSON request body")
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return kernel.InvalidArgument("request body must contain a single JSON object")
	} else if !errors.Is(err, io.EOF) {
		return kernel.InvalidArgument("invalid JSON request body")
	}
	if err := validateDecoded(target); err != nil {
		return err
	}
	return nil
}
