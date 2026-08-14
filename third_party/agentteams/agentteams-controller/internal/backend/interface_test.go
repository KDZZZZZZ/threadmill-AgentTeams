package backend

import (
	"context"
	"errors"
	"testing"
)

func TestResolveRuntime(t *testing.T) {
	cases := []struct {
		name       string
		reqRuntime string
		fallback   string
		want       string
	}{
		{"explicit_request_wins_over_fallback", RuntimeCopaw, RuntimeHermes, RuntimeCopaw},
		{"explicit_over_empty_fallback", RuntimeOpenClaw, "", RuntimeOpenClaw},
		{"empty_uses_fallback_hermes", "", RuntimeHermes, RuntimeHermes},
		{"empty_uses_fallback_copaw", "", RuntimeCopaw, RuntimeCopaw},
		{"empty_uses_fallback_qwenpaw", "", RuntimeQwenPaw, RuntimeQwenPaw},
		{"empty_and_no_fallback_uses_openclaw", "", "", RuntimeOpenClaw},
		{"explicit_openclaw_preserved", RuntimeOpenClaw, RuntimeHermes, RuntimeOpenClaw},
		{"explicit_hermes_preserved", RuntimeHermes, RuntimeCopaw, RuntimeHermes},
		{"explicit_qwenpaw_preserved", RuntimeQwenPaw, RuntimeCopaw, RuntimeQwenPaw},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveRuntime(tc.reqRuntime, tc.fallback)
			if got != tc.want {
				t.Fatalf("ResolveRuntime(%q, %q) = %q, want %q", tc.reqRuntime, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestValidRuntime(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{RuntimeOpenClaw, true},
		{RuntimeCopaw, true},
		{RuntimeHermes, true},
		{RuntimeQwenPaw, true},
		{"unknown", false},
	}
	for _, tc := range cases {
		if got := ValidRuntime(tc.in); got != tc.want {
			t.Fatalf("ValidRuntime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestImageRefChanged(t *testing.T) {
	cases := []struct {
		name    string
		desired string
		actual  string
		want    bool
	}{
		{"both empty unknown", "", "", false},
		{"actual unknown", "agentteams/worker:v2", "", false},
		{"same exact", "agentteams/worker:v2", "agentteams/worker:v2", false},
		{"implicit latest", "agentteams/worker", "agentteams/worker:latest", false},
		{"registry port is not tag", "localhost:5000/agentteams/worker", "localhost:5000/agentteams/worker:latest", false},
		{"different tags", "agentteams/worker:v2", "agentteams/worker:v1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ImageRefChanged(tc.desired, tc.actual); got != tc.want {
				t.Fatalf("ImageRefChanged(%q, %q)=%v, want %v", tc.desired, tc.actual, got, tc.want)
			}
		})
	}
}

func TestImageChangedDetectsDockerDigestDrift(t *testing.T) {
	wb := &imageResolverBackend{ids: map[string]string{
		"agentteams/qwenpaw-worker:threadmill-current": "sha256:new",
	}}
	result := &WorkerResult{
		Image:   "agentteams/qwenpaw-worker:threadmill-current",
		ImageID: "sha256:old",
	}

	if !ImageChanged(context.Background(), wb, "agentteams/qwenpaw-worker:threadmill-current", result) {
		t.Fatal("same tag with different resolved image ID should be drift")
	}
}

func TestImageChangedSameTagSameDigestIsNotDrift(t *testing.T) {
	wb := &imageResolverBackend{ids: map[string]string{
		"agentteams/qwenpaw-worker:threadmill-current": "sha256:same",
	}}
	result := &WorkerResult{
		Image:   "agentteams/qwenpaw-worker:threadmill-current",
		ImageID: "sha256:same",
	}

	if ImageChanged(context.Background(), wb, "agentteams/qwenpaw-worker:threadmill-current", result) {
		t.Fatal("same tag with same resolved image ID should not be drift")
	}
}

func TestImageChangedUnknownDesiredDigestIsFailSafe(t *testing.T) {
	wb := &imageResolverBackend{err: errors.New("image not found")}
	result := &WorkerResult{
		Image:   "agentteams/qwenpaw-worker:threadmill-current",
		ImageID: "sha256:old",
	}

	if ImageChanged(context.Background(), wb, "agentteams/qwenpaw-worker:threadmill-current", result) {
		t.Fatal("unknown desired image ID should not be treated as drift")
	}
}

func TestImageChangedNonResolverBackendIgnoresDigestDrift(t *testing.T) {
	wb := &nonResolverBackend{}
	result := &WorkerResult{
		Image:   "agentteams/qwenpaw-worker:threadmill-current",
		ImageID: "sha256:old",
	}

	if ImageChanged(context.Background(), wb, "agentteams/qwenpaw-worker:threadmill-current", result) {
		t.Fatal("non-Docker backend should not infer local digest drift")
	}
}

type imageResolverBackend struct {
	ids map[string]string
	err error
}

func (b *imageResolverBackend) Name() string                   { return "docker" }
func (b *imageResolverBackend) DeploymentMode() string         { return DeployLocal }
func (b *imageResolverBackend) Available(context.Context) bool { return true }
func (b *imageResolverBackend) NeedsCredentialInjection() bool { return false }
func (b *imageResolverBackend) Create(context.Context, CreateRequest) (*WorkerResult, error) {
	return nil, nil
}
func (b *imageResolverBackend) Delete(context.Context, string) error { return nil }
func (b *imageResolverBackend) Start(context.Context, string) error  { return nil }
func (b *imageResolverBackend) Stop(context.Context, string) error   { return nil }
func (b *imageResolverBackend) Status(context.Context, string) (*WorkerResult, error) {
	return nil, nil
}
func (b *imageResolverBackend) ResolveImageID(_ context.Context, image string) (string, error) {
	if b.err != nil {
		return "", b.err
	}
	return b.ids[image], nil
}

type nonResolverBackend struct{}

func (b *nonResolverBackend) Name() string                   { return "k8s" }
func (b *nonResolverBackend) DeploymentMode() string         { return DeployCloud }
func (b *nonResolverBackend) Available(context.Context) bool { return true }
func (b *nonResolverBackend) NeedsCredentialInjection() bool { return false }
func (b *nonResolverBackend) Create(context.Context, CreateRequest) (*WorkerResult, error) {
	return nil, nil
}
func (b *nonResolverBackend) Delete(context.Context, string) error { return nil }
func (b *nonResolverBackend) Start(context.Context, string) error  { return nil }
func (b *nonResolverBackend) Stop(context.Context, string) error   { return nil }
func (b *nonResolverBackend) Status(context.Context, string) (*WorkerResult, error) {
	return nil, nil
}
