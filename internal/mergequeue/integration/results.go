package integration

import (
	"context"
	"os/exec"
	"strings"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/mergequeue"
	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/runtime/phase"
)

// ResultAcceptor reads the trusted artifacts referenced by a runtime receipt
// and applies the verifier report contract. It does not call a model and does
// not mutate the candidate workspace.
type ResultAcceptor interface {
	AcceptTargetedVerify(context.Context, phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error)
}

type ResultAcceptorFunc func(context.Context, phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error)

func (f ResultAcceptorFunc) AcceptTargetedVerify(ctx context.Context, receipt phase.OutputReceipt) (mergequeue.TargetedVerifyResult, error) {
	return f(ctx, receipt)
}

type MainRevisionReader interface {
	CurrentRevision(context.Context, string, string) (string, error)
}

// GitRevisionReader is a mechanical revision reader for repositories managed
// by the Merge Queue. It has no write or model-execution capability.
type GitRevisionReader struct{}

func (GitRevisionReader) CurrentRevision(ctx context.Context, repository, branch string) (string, error) {
	if branch == "" {
		branch = "main"
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "refs/heads/"+branch)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
