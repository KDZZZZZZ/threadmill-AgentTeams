package config

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	envDatabaseURL                = "THREADMILL_DATABASE_URL"
	envObjectStoreEndpoint        = "THREADMILL_OBJECT_STORE_ENDPOINT"
	envObjectStoreAccessKey       = "THREADMILL_OBJECT_STORE_ACCESS_KEY"
	envObjectStoreSecretKey       = "THREADMILL_OBJECT_STORE_SECRET_KEY"
	envObjectStoreBucket          = "THREADMILL_OBJECT_STORE_BUCKET"
	envObjectStoreSecure          = "THREADMILL_OBJECT_STORE_SECURE"
	envHTTPAddr                   = "THREADMILL_HTTP_ADDR"
	envProjectID                  = "THREADMILL_PROJECT_ID"
	envWebDistDir                 = "THREADMILL_WEB_DIST_DIR"
	envAllowedOrigins             = "THREADMILL_ALLOWED_ORIGINS"
	envAgentTeamsControllerURL    = "THREADMILL_AGENTTEAMS_CONTROLLER_URL"
	envAgentTeamsControllerBearer = "THREADMILL_AGENTTEAMS_CONTROLLER_BEARER"
	envContainerMCPURL            = "THREADMILL_MCP_CONTAINER_URL"
	envAgentTeamsRoomID           = "THREADMILL_AGENTTEAMS_ROOM"
	envAgentTeamsContainers       = "THREADMILL_AGENTTEAMS_CONTAINERS"
	envAgentTeamsSharedBucket     = "THREADMILL_AGENTTEAMS_SHARED_BUCKET"
	envAgentTeamsSharedPrefix     = "THREADMILL_AGENTTEAMS_SHARED_PREFIX"
	envRepositoryPath             = "THREADMILL_REPOSITORY_PATH"
	envWorktreeParent             = "THREADMILL_WORKTREE_PARENT"
	envRuntimeTokenKey            = "THREADMILL_RUNTIME_TOKEN_KEY"
)

type Config struct {
	DatabaseURL                string
	ObjectStoreEndpoint        string
	ObjectStoreAccessKey       string
	ObjectStoreSecretKey       string
	ObjectStoreBucket          string
	ObjectStoreSecure          bool
	HTTPAddr                   string
	ProjectID                  string
	WebDistDir                 string
	AllowedOrigins             []string
	AgentTeamsControllerURL    string
	AgentTeamsControllerBearer string
	ContainerMCPURL            string
	AgentTeamsRoomID           string
	AgentTeamsContainers       map[string]string
	AgentTeamsSharedBucket     string
	AgentTeamsSharedPrefix     string
	RepositoryPath             string
	WorktreeParent             string
	RuntimeTokenKey            []byte
}

type Environment map[string]string

type Diagnostic struct {
	OK          bool     `json:"ok"`
	Code        string   `json:"code,omitempty"`
	Message     string   `json:"message"`
	Missing     []string `json:"missing,omitempty"`
	Recoverable bool     `json:"recoverable"`
}

type DiagnosticError struct {
	Diagnostic Diagnostic
}

func (e DiagnosticError) Error() string {
	return e.Diagnostic.Message
}

func AsDiagnostic(err error) (Diagnostic, bool) {
	if err == nil {
		return Diagnostic{}, false
	}
	diagErr, ok := err.(DiagnosticError)
	if !ok {
		return Diagnostic{}, false
	}
	return diagErr.Diagnostic, true
}

func Load(env Environment) (Config, error) {
	cfg := Config{
		DatabaseURL:                lookup(env, envDatabaseURL),
		ObjectStoreEndpoint:        lookup(env, envObjectStoreEndpoint),
		ObjectStoreAccessKey:       lookup(env, envObjectStoreAccessKey),
		ObjectStoreSecretKey:       lookup(env, envObjectStoreSecretKey),
		ObjectStoreBucket:          lookup(env, envObjectStoreBucket),
		ObjectStoreSecure:          true,
		HTTPAddr:                   lookup(env, envHTTPAddr),
		ProjectID:                  lookup(env, envProjectID),
		WebDistDir:                 lookup(env, envWebDistDir),
		AgentTeamsControllerURL:    lookup(env, envAgentTeamsControllerURL),
		AgentTeamsControllerBearer: lookup(env, envAgentTeamsControllerBearer),
		ContainerMCPURL:            lookup(env, envContainerMCPURL),
		AgentTeamsRoomID:           lookup(env, envAgentTeamsRoomID),
		AgentTeamsSharedBucket:     lookup(env, envAgentTeamsSharedBucket),
		AgentTeamsSharedPrefix:     lookup(env, envAgentTeamsSharedPrefix),
		RepositoryPath:             lookup(env, envRepositoryPath),
		WorktreeParent:             lookup(env, envWorktreeParent),
	}
	allowedOrigins, err := parseAllowedOrigins(lookup(env, envAllowedOrigins))
	if err != nil {
		return Config{}, err
	}
	cfg.AllowedOrigins = allowedOrigins
	containers, err := parseHostContainers(lookup(env, envAgentTeamsContainers))
	if err != nil {
		return Config{}, err
	}
	cfg.AgentTeamsContainers = containers
	key, err := parseRuntimeTokenKey(lookup(env, envRuntimeTokenKey))
	if err != nil {
		return Config{}, err
	}
	cfg.RuntimeTokenKey = key
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.ProjectID == "" {
		cfg.ProjectID = "default-project"
	}
	if cfg.WebDistDir == "" {
		cfg.WebDistDir = "web/dist"
	}
	if value := lookup(env, envObjectStoreSecure); value != "" {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "y", "on":
			cfg.ObjectStoreSecure = true
		case "0", "false", "no", "n", "off":
			cfg.ObjectStoreSecure = false
		default:
			return Config{}, DiagnosticError{Diagnostic: Diagnostic{
				OK:          false,
				Code:        "configuration_invalid",
				Message:     fmt.Sprintf("invalid boolean configuration: %s", envObjectStoreSecure),
				Recoverable: true,
			}}
		}
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, envDatabaseURL)
	}
	if cfg.ObjectStoreEndpoint == "" {
		missing = append(missing, envObjectStoreEndpoint)
	}
	if cfg.ObjectStoreAccessKey == "" {
		missing = append(missing, envObjectStoreAccessKey)
	}
	if cfg.ObjectStoreSecretKey == "" {
		missing = append(missing, envObjectStoreSecretKey)
	}
	if cfg.ObjectStoreBucket == "" {
		missing = append(missing, envObjectStoreBucket)
	}
	if len(cfg.AllowedOrigins) == 0 {
		missing = append(missing, envAllowedOrigins)
	}
	for _, required := range []struct {
		key   string
		value string
	}{
		{envAgentTeamsControllerURL, cfg.AgentTeamsControllerURL},
		{envAgentTeamsControllerBearer, cfg.AgentTeamsControllerBearer},
		{envContainerMCPURL, cfg.ContainerMCPURL},
		{envAgentTeamsRoomID, cfg.AgentTeamsRoomID},
		{envAgentTeamsSharedBucket, cfg.AgentTeamsSharedBucket},
		{envAgentTeamsSharedPrefix, cfg.AgentTeamsSharedPrefix},
		{envRepositoryPath, cfg.RepositoryPath},
		{envWorktreeParent, cfg.WorktreeParent},
	} {
		if required.value == "" {
			missing = append(missing, required.key)
		}
	}
	if len(cfg.AgentTeamsContainers) == 0 {
		missing = append(missing, envAgentTeamsContainers)
	}
	if len(cfg.RuntimeTokenKey) == 0 {
		missing = append(missing, envRuntimeTokenKey)
	}
	if len(missing) > 0 {
		return Config{}, DiagnosticError{Diagnostic: Diagnostic{
			OK:          false,
			Code:        "configuration_missing",
			Message:     fmt.Sprintf("missing required configuration: %s", strings.Join(missing, ", ")),
			Missing:     missing,
			Recoverable: true,
		}}
	}

	if err := validateProductionConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func lookup(env Environment, key string) string {
	if env != nil {
		if value, ok := env[key]; ok {
			return value
		}
	}
	return os.Getenv(key)
}

func Check(cfg Config) Diagnostic {
	return Diagnostic{
		OK:          true,
		Code:        "ok",
		Message:     fmt.Sprintf("configuration valid; http address %s; project %s; web dist %s; object store endpoint %s bucket %s secure %t; allowed origins %d; AgentTeams controller %s room %s host mappings %d; container MCP %s; shared objects %s/%s; repository %s; worktrees %s", cfg.HTTPAddr, cfg.ProjectID, cfg.WebDistDir, cfg.ObjectStoreEndpoint, cfg.ObjectStoreBucket, cfg.ObjectStoreSecure, len(cfg.AllowedOrigins), cfg.AgentTeamsControllerURL, cfg.AgentTeamsRoomID, len(cfg.AgentTeamsContainers), cfg.ContainerMCPURL, cfg.AgentTeamsSharedBucket, cfg.AgentTeamsSharedPrefix, cfg.RepositoryPath, cfg.WorktreeParent),
		Recoverable: true,
	}
}

var safeProviderName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func parseHostContainers(raw string) (map[string]string, error) {
	result := make(map[string]string)
	containers := make(map[string]struct{})
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.Split(pair, "=")
		if len(parts) != 2 {
			return nil, invalidConfig(envAgentTeamsContainers)
		}
		host, container := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		// These values flow to controller path parameters and docker exec. Keep
		// them aligned with the AgentTeams adapter's provider/container grammar.
		if !safeProviderName.MatchString(host) || !safeProviderName.MatchString(container) {
			return nil, invalidConfig(envAgentTeamsContainers)
		}
		if _, exists := result[host]; exists {
			return nil, invalidConfig(envAgentTeamsContainers)
		}
		if _, exists := containers[container]; exists {
			return nil, invalidConfig(envAgentTeamsContainers)
		}
		result[host] = container
		containers[container] = struct{}{}
	}
	return result, nil
}

func parseRuntimeTokenKey(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, invalidConfig(envRuntimeTokenKey)
	}
	return key, nil
}

func validateProductionConfig(cfg Config) error {
	for _, endpoint := range []struct {
		key   string
		value string
	}{
		{envAgentTeamsControllerURL, cfg.AgentTeamsControllerURL},
		{envContainerMCPURL, cfg.ContainerMCPURL},
	} {
		parsed, err := url.Parse(endpoint.value)
		if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return invalidConfig(endpoint.key)
		}
		if endpoint.key == envContainerMCPURL && parsed.Path != "/mcp" {
			return invalidConfig(endpoint.key)
		}
	}
	if strings.IndexFunc(cfg.AgentTeamsControllerBearer, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
		return invalidConfig(envAgentTeamsControllerBearer)
	}
	if !safeOpaqueIdentifier(cfg.AgentTeamsRoomID, 256) {
		return invalidConfig(envAgentTeamsRoomID)
	}
	if !safeOpaqueIdentifier(cfg.AgentTeamsSharedBucket, 128) || strings.ContainsAny(cfg.AgentTeamsSharedBucket, `/\`) {
		return invalidConfig(envAgentTeamsSharedBucket)
	}
	prefix := cfg.AgentTeamsSharedPrefix
	if prefix != strings.Trim(prefix, "/") || strings.Contains(prefix, "\\") || strings.Contains(prefix, "//") || strings.Contains(prefix, "..") {
		return invalidConfig(envAgentTeamsSharedPrefix)
	}
	for _, value := range []struct {
		key   string
		value string
	}{
		{envRepositoryPath, cfg.RepositoryPath},
		{envWorktreeParent, cfg.WorktreeParent},
	} {
		if !filepath.IsAbs(value.value) || strings.TrimSpace(value.value) != value.value {
			return invalidConfig(value.key)
		}
	}
	return nil
}

func safeOpaqueIdentifier(value string, limit int) bool {
	return value != "" && len(value) <= limit && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, func(r rune) bool { return r <= 0x20 || r == 0x7f }) < 0
}

func invalidConfig(key string) error {
	return DiagnosticError{Diagnostic: Diagnostic{
		OK: false, Code: "configuration_invalid",
		Message: fmt.Sprintf("invalid configuration: %s", key), Recoverable: true,
	}}
}

func parseAllowedOrigins(raw string) ([]string, error) {
	seen := make(map[string]struct{})
	origins := make([]string, 0)
	for _, candidate := range strings.Split(raw, ",") {
		origin := strings.TrimSpace(candidate)
		if origin == "" {
			continue
		}
		if !validOrigin(origin) {
			return nil, DiagnosticError{Diagnostic: Diagnostic{
				OK:          false,
				Code:        "configuration_invalid",
				Message:     fmt.Sprintf("invalid origin configuration: %s", envAllowedOrigins),
				Recoverable: true,
			}}
		}
		if _, exists := seen[origin]; exists {
			continue
		}
		seen[origin] = struct{}{}
		origins = append(origins, origin)
	}
	return origins, nil
}

func validOrigin(origin string) bool {
	parsed, err := url.Parse(origin)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	return parsed.Opaque == "" && parsed.Path == "" && parsed.RawPath == "" && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.User == nil
}
