package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const (
	envDatabaseURL          = "THREADMILL_DATABASE_URL"
	envObjectStoreEndpoint  = "THREADMILL_OBJECT_STORE_ENDPOINT"
	envObjectStoreAccessKey = "THREADMILL_OBJECT_STORE_ACCESS_KEY"
	envObjectStoreSecretKey = "THREADMILL_OBJECT_STORE_SECRET_KEY"
	envObjectStoreBucket    = "THREADMILL_OBJECT_STORE_BUCKET"
	envObjectStoreSecure    = "THREADMILL_OBJECT_STORE_SECURE"
	envHTTPAddr             = "THREADMILL_HTTP_ADDR"
	envProjectID            = "THREADMILL_PROJECT_ID"
	envWebDistDir           = "THREADMILL_WEB_DIST_DIR"
	envAllowedOrigins       = "THREADMILL_ALLOWED_ORIGINS"
)

type Config struct {
	DatabaseURL          string
	ObjectStoreEndpoint  string
	ObjectStoreAccessKey string
	ObjectStoreSecretKey string
	ObjectStoreBucket    string
	ObjectStoreSecure    bool
	HTTPAddr             string
	ProjectID            string
	WebDistDir           string
	AllowedOrigins       []string
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
		DatabaseURL:          lookup(env, envDatabaseURL),
		ObjectStoreEndpoint:  lookup(env, envObjectStoreEndpoint),
		ObjectStoreAccessKey: lookup(env, envObjectStoreAccessKey),
		ObjectStoreSecretKey: lookup(env, envObjectStoreSecretKey),
		ObjectStoreBucket:    lookup(env, envObjectStoreBucket),
		ObjectStoreSecure:    true,
		HTTPAddr:             lookup(env, envHTTPAddr),
		ProjectID:            lookup(env, envProjectID),
		WebDistDir:           lookup(env, envWebDistDir),
	}
	allowedOrigins, err := parseAllowedOrigins(lookup(env, envAllowedOrigins))
	if err != nil {
		return Config{}, err
	}
	cfg.AllowedOrigins = allowedOrigins
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
	if len(missing) > 0 {
		return Config{}, DiagnosticError{Diagnostic: Diagnostic{
			OK:          false,
			Code:        "configuration_missing",
			Message:     fmt.Sprintf("missing required configuration: %s", strings.Join(missing, ", ")),
			Missing:     missing,
			Recoverable: true,
		}}
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
		Message:     fmt.Sprintf("configuration valid; http address %s; project %s; web dist %s; object store endpoint %s bucket %s secure %t; allowed origins %d", cfg.HTTPAddr, cfg.ProjectID, cfg.WebDistDir, cfg.ObjectStoreEndpoint, cfg.ObjectStoreBucket, cfg.ObjectStoreSecure, len(cfg.AllowedOrigins)),
		Recoverable: true,
	}
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
