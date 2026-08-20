package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	ErrMCPBindingInvalid  = errors.New("invalid mcp credential binding")
	ErrMCPBindingNotFound = errors.New("mcp credential binding not found")
	ErrMCPBindingOwner    = errors.New("mcp credential binding is not owned by worker")
	ErrMCPBindingRevoked  = errors.New("mcp credential binding is revoked")
)

// MCPCredentialBinding is controller-owned credential material. Value is
// deliberately excluded from all Worker CRs, status objects, logs, and REST
// readbacks.
type MCPCredentialBinding struct {
	ID         string
	WorkerName string
	HeaderName string
	Value      string
	Revoked    bool
}

// MCPCredentialBindingView is the redacted control-plane readback.
type MCPCredentialBindingView struct {
	ID         string `json:"id"`
	WorkerName string `json:"workerName"`
	HeaderName string `json:"headerName"`
	State      string `json:"state"`
}

func (b MCPCredentialBinding) View() MCPCredentialBindingView {
	state := "active"
	if b.Revoked {
		state = "revoked"
	}
	return MCPCredentialBindingView{
		ID:         b.ID,
		WorkerName: b.WorkerName,
		HeaderName: b.HeaderName,
		State:      state,
	}
}

// MCPCredentialBindingStore owns private HTTP header material separately from
// WorkerCredentials, whose fixed Matrix/MinIO/Gateway schema cannot safely
// represent arbitrary worker-scoped MCP headers.
type MCPCredentialBindingStore interface {
	Create(context.Context, MCPCredentialBinding) (MCPCredentialBindingView, error)
	Get(context.Context, string) (MCPCredentialBindingView, error)
	Resolve(context.Context, string, string) (MCPCredentialBinding, error)
	Revoke(context.Context, string) error
}

func validateMCPCredentialBinding(binding MCPCredentialBinding) error {
	if strings.TrimSpace(binding.WorkerName) == "" || strings.TrimSpace(binding.HeaderName) == "" || binding.Value == "" {
		return ErrMCPBindingInvalid
	}
	return nil
}

func newMCPCredentialBindingID() (string, error) {
	// 16 random bytes leave room for the Kubernetes Secret name prefix while
	// retaining a 128-bit opaque identifier.
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate mcp credential binding id: %w", err)
	}
	return "mcpcred-" + hex.EncodeToString(raw), nil
}

// InMemoryMCPCredentialBindingStore is appropriate for unit tests and is not
// used by App bootstrap. Embedded deployments use FileMCPCredentialBindingStore.
type InMemoryMCPCredentialBindingStore struct {
	mu    sync.RWMutex
	items map[string]MCPCredentialBinding
}

func NewInMemoryMCPCredentialBindingStore() *InMemoryMCPCredentialBindingStore {
	return &InMemoryMCPCredentialBindingStore{items: map[string]MCPCredentialBinding{}}
}

func (s *InMemoryMCPCredentialBindingStore) Create(_ context.Context, binding MCPCredentialBinding) (MCPCredentialBindingView, error) {
	if err := validateMCPCredentialBinding(binding); err != nil {
		return MCPCredentialBindingView{}, err
	}
	if binding.ID == "" {
		id, err := newMCPCredentialBindingID()
		if err != nil {
			return MCPCredentialBindingView{}, err
		}
		binding.ID = id
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[binding.ID] = binding
	return binding.View(), nil
}

func (s *InMemoryMCPCredentialBindingStore) Get(_ context.Context, id string) (MCPCredentialBindingView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.items[id]
	if !ok {
		return MCPCredentialBindingView{}, ErrMCPBindingNotFound
	}
	return binding.View(), nil
}

func (s *InMemoryMCPCredentialBindingStore) Resolve(_ context.Context, id, workerName string) (MCPCredentialBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.items[id]
	if !ok {
		return MCPCredentialBinding{}, ErrMCPBindingNotFound
	}
	if binding.WorkerName != workerName {
		return MCPCredentialBinding{}, ErrMCPBindingOwner
	}
	if binding.Revoked {
		return MCPCredentialBinding{}, ErrMCPBindingRevoked
	}
	return binding, nil
}

func (s *InMemoryMCPCredentialBindingStore) Revoke(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.items[id]
	if !ok {
		return ErrMCPBindingNotFound
	}
	binding.Revoked = true
	binding.Value = ""
	s.items[id] = binding
	return nil
}

// FileMCPCredentialBindingStore is the embedded/Docker controller-owned
// backend. Dir must be controller-private and must not be a Worker workspace.
// Binding files are created with mode 0600; revocation clears secret material.
type FileMCPCredentialBindingStore struct {
	Dir string
	mu  sync.Mutex
}

func (s *FileMCPCredentialBindingStore) bindingPath(id string) (string, error) {
	if strings.TrimSpace(id) == "" || filepath.Base(id) != id {
		return "", ErrMCPBindingInvalid
	}
	return filepath.Join(s.Dir, id+".json"), nil
}

func (s *FileMCPCredentialBindingStore) Create(_ context.Context, binding MCPCredentialBinding) (MCPCredentialBindingView, error) {
	if err := validateMCPCredentialBinding(binding); err != nil {
		return MCPCredentialBindingView{}, err
	}
	if binding.ID == "" {
		id, err := newMCPCredentialBindingID()
		if err != nil {
			return MCPCredentialBindingView{}, err
		}
		binding.ID = id
	}
	path, err := s.bindingPath(binding.ID)
	if err != nil {
		return MCPCredentialBindingView{}, err
	}
	payload, err := json.Marshal(binding)
	if err != nil {
		return MCPCredentialBindingView{}, fmt.Errorf("marshal mcp credential binding: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.Dir, 0700); err != nil {
		return MCPCredentialBindingView{}, fmt.Errorf("create mcp credential store: %w", err)
	}
	if err := os.WriteFile(path, payload, 0600); err != nil {
		return MCPCredentialBindingView{}, fmt.Errorf("write mcp credential binding: %w", err)
	}
	return binding.View(), nil
}

func (s *FileMCPCredentialBindingStore) load(id string) (MCPCredentialBinding, string, error) {
	path, err := s.bindingPath(id)
	if err != nil {
		return MCPCredentialBinding{}, "", err
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return MCPCredentialBinding{}, "", ErrMCPBindingNotFound
		}
		return MCPCredentialBinding{}, "", fmt.Errorf("read mcp credential binding: %w", err)
	}
	var binding MCPCredentialBinding
	if err := json.Unmarshal(payload, &binding); err != nil {
		return MCPCredentialBinding{}, "", fmt.Errorf("decode mcp credential binding: %w", err)
	}
	return binding, path, nil
}

func (s *FileMCPCredentialBindingStore) Get(_ context.Context, id string) (MCPCredentialBindingView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, _, err := s.load(id)
	if err != nil {
		return MCPCredentialBindingView{}, err
	}
	return binding.View(), nil
}

func (s *FileMCPCredentialBindingStore) Resolve(_ context.Context, id, workerName string) (MCPCredentialBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, _, err := s.load(id)
	if err != nil {
		return MCPCredentialBinding{}, err
	}
	if binding.WorkerName != workerName {
		return MCPCredentialBinding{}, ErrMCPBindingOwner
	}
	if binding.Revoked {
		return MCPCredentialBinding{}, ErrMCPBindingRevoked
	}
	return binding, nil
}

func (s *FileMCPCredentialBindingStore) Revoke(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, path, err := s.load(id)
	if err != nil {
		return err
	}
	binding.Revoked = true
	binding.Value = ""
	payload, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("marshal revoked mcp credential binding: %w", err)
	}
	if err := os.WriteFile(path, payload, 0600); err != nil {
		return fmt.Errorf("write revoked mcp credential binding: %w", err)
	}
	return nil
}

// SecretMCPCredentialBindingStore is the Kubernetes backend. It keeps private
// material in a controller-owned Secret, never in the Worker CR or its status.
// The controller resolves a binding only while projecting private runtime
// configuration for its owning Worker.
type SecretMCPCredentialBindingStore struct {
	Client         kubernetes.Interface
	Namespace      string
	ControllerName string
}

func (s *SecretMCPCredentialBindingStore) secretName(id string) (string, error) {
	if strings.TrimSpace(id) == "" || filepath.Base(id) != id {
		return "", ErrMCPBindingInvalid
	}
	return "agentteams-mcp-" + id, nil
}

func (s *SecretMCPCredentialBindingStore) Create(ctx context.Context, binding MCPCredentialBinding) (MCPCredentialBindingView, error) {
	if s.Client == nil || strings.TrimSpace(s.Namespace) == "" {
		return MCPCredentialBindingView{}, fmt.Errorf("mcp credential binding secret store is not configured")
	}
	if err := validateMCPCredentialBinding(binding); err != nil {
		return MCPCredentialBindingView{}, err
	}
	if binding.ID == "" {
		id, err := newMCPCredentialBindingID()
		if err != nil {
			return MCPCredentialBindingView{}, err
		}
		binding.ID = id
	}
	name, err := s.secretName(binding.ID)
	if err != nil {
		return MCPCredentialBindingView{}, err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: s.Namespace,
			Labels: map[string]string{"agentteams.io/mcp-credential-binding": "true", "agentteams.io/worker": binding.WorkerName, "agentteams.io/controller": s.ControllerName},
		},
		Data: map[string][]byte{"id": []byte(binding.ID), "workerName": []byte(binding.WorkerName), "headerName": []byte(binding.HeaderName), "secretValue": []byte(binding.Value), "state": []byte("active")},
	}
	if _, err := s.Client.CoreV1().Secrets(s.Namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return MCPCredentialBindingView{}, fmt.Errorf("create mcp credential binding secret: %w", err)
	}
	return binding.View(), nil
}

func (s *SecretMCPCredentialBindingStore) load(ctx context.Context, id string) (*corev1.Secret, MCPCredentialBinding, error) {
	if s.Client == nil || strings.TrimSpace(s.Namespace) == "" {
		return nil, MCPCredentialBinding{}, fmt.Errorf("mcp credential binding secret store is not configured")
	}
	name, err := s.secretName(id)
	if err != nil {
		return nil, MCPCredentialBinding{}, err
	}
	secret, err := s.Client.CoreV1().Secrets(s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, MCPCredentialBinding{}, ErrMCPBindingNotFound
		}
		return nil, MCPCredentialBinding{}, fmt.Errorf("get mcp credential binding secret: %w", err)
	}
	binding := MCPCredentialBinding{ID: string(secret.Data["id"]), WorkerName: string(secret.Data["workerName"]), HeaderName: string(secret.Data["headerName"]), Value: string(secret.Data["secretValue"]), Revoked: string(secret.Data["state"]) == "revoked"}
	return secret, binding, nil
}

func (s *SecretMCPCredentialBindingStore) Get(ctx context.Context, id string) (MCPCredentialBindingView, error) {
	_, binding, err := s.load(ctx, id)
	if err != nil {
		return MCPCredentialBindingView{}, err
	}
	return binding.View(), nil
}

func (s *SecretMCPCredentialBindingStore) Resolve(ctx context.Context, id, workerName string) (MCPCredentialBinding, error) {
	_, binding, err := s.load(ctx, id)
	if err != nil {
		return MCPCredentialBinding{}, err
	}
	if binding.WorkerName != workerName {
		return MCPCredentialBinding{}, ErrMCPBindingOwner
	}
	if binding.Revoked {
		return MCPCredentialBinding{}, ErrMCPBindingRevoked
	}
	return binding, nil
}

func (s *SecretMCPCredentialBindingStore) Revoke(ctx context.Context, id string) error {
	secret, _, err := s.load(ctx, id)
	if err != nil {
		return err
	}
	secret.Data["state"] = []byte("revoked")
	delete(secret.Data, "secretValue")
	_, err = s.Client.CoreV1().Secrets(s.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("revoke mcp credential binding secret: %w", err)
	}
	return nil
}
