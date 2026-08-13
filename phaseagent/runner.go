package phaseagent

import "context"

// RoleSpec is the phase-derived execution boundary. It deliberately contains
// no prompt, provider, Workspace, or AgentTeams-specific data.
type RoleSpec struct {
	Phase        Phase             `json:"phase"`
	Capabilities PhaseCapabilities `json:"capabilities"`
}

// RoleForEndpoint derives the sole phase value from the authoritative endpoint
// ID, preventing a second, independently mutable phase source.
func RoleForEndpoint(endpoint PhaseEndpointRef) (RoleSpec, error) {
	if err := endpoint.Validate(); err != nil {
		return RoleSpec{}, err
	}
	phase := Phase(endpoint.EndpointID)
	capabilities, err := CapabilitiesFor(phase)
	if err != nil {
		return RoleSpec{}, err
	}
	return RoleSpec{Phase: phase, Capabilities: capabilities}, nil
}

// ExecutionContext is a transient, single-execution view for a PhaseExecutor.
// Invocation.Start owns the immutable binding reference; Invocation.Inputs is
// the runner's current input view. No hidden reasoning, provider session,
// worker identity, or expanded binding contents are retained here.
type ExecutionContext struct {
	Invocation    InvocationContext  `json:"invocation"`
	Role          RoleSpec           `json:"role"`
	Runtime       Runtime            `json:"-"`
	ContextReader ContextGraphReader `json:"-"`
	ContextAgent  ContextAgent       `json:"-"`
	ResumeState   *ResumeState       `json:"resume_state,omitempty"`
}

// PhaseExecutor performs one phase execution using only the supplied transient
// context and Runtime outbound port. Its return reports execution health, not
// a PhaseOutput: structured output is submitted explicitly through Runtime.
type PhaseExecutor interface {
	Execute(ctx context.Context, execution ExecutionContext) error
}

// Runner coordinates one already-validated-and-assembled execution call. It
// is not a Host, Scheduler, GraphRuntime, Task Manager, Workspace service, or
// provider adapter.
type Runner struct {
	runtime       Runtime
	executor      PhaseExecutor
	contextReader ContextGraphReader
	contextAgent  ContextAgent
}

// NewRunner creates a provider-neutral runner. Runtime, Context reader, and
// semantic Context retriever are required so RunStart and RunResume always
// inject the complete Phase Agent execution surface. Their concrete
// implementations remain outside this package.
func NewRunner(runtime Runtime, executor PhaseExecutor, reader ContextGraphReader, agent ContextAgent) (*Runner, error) {
	if reader == nil {
		return nil, invalidRunnerError("context reader is required")
	}
	if agent == nil {
		return nil, invalidRunnerError("context agent is required")
	}
	if runtime == nil {
		return nil, invalidRunnerError("runtime is required")
	}
	if executor == nil {
		return nil, invalidRunnerError("executor is required")
	}
	return &Runner{runtime: runtime, executor: executor, contextReader: reader, contextAgent: agent}, nil
}

// RunStart runs a fresh binding with no restored state. Executor success ends
// this execution as finished; it does not imply output submission, endpoint
// satisfaction, or Task completion.
func (r *Runner) RunStart(ctx context.Context, input StartPhaseInput) (InvocationContext, error) {
	if err := input.Validate(); err != nil {
		return InvocationContext{}, err
	}
	return r.run(ctx, input, nil)
}

// RunResume runs the new Invocation carried by a checkpoint-bound resume
// callback. The caller supplies already-restored explicit state; Runner never
// resolves CheckpointRef or revives an earlier InvocationContext.
func (r *Runner) RunResume(ctx context.Context, input ResumePhaseInput, state *ResumeState) (InvocationContext, error) {
	if err := input.Validate(); err != nil {
		return InvocationContext{}, err
	}
	return r.run(ctx, input.Start, state)
}

func (r *Runner) run(ctx context.Context, input StartPhaseInput, state *ResumeState) (InvocationContext, error) {
	session, err := NewInvocationContext(input)
	if err != nil {
		return InvocationContext{}, err
	}
	role, err := RoleForEndpoint(input.Endpoint)
	if err != nil {
		return InvocationContext{}, err
	}
	execution := ExecutionContext{
		Invocation:    session,
		Role:          role,
		Runtime:       r.runtime,
		ContextReader: r.contextReader,
		ContextAgent:  r.contextAgent,
		ResumeState:   state,
	}
	if err := r.executor.Execute(ctx, execution); err != nil {
		session.State = InvocationFailed
		return session, err
	}
	session.State = InvocationFinished
	return session, nil
}

type invalidRunnerError string

func (e invalidRunnerError) Error() string { return string(e) }
