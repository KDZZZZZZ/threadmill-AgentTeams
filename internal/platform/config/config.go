package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	envDatabaseURL         = "THREADMILL_DATABASE_URL"
	envObjectStoreEndpoint = "THREADMILL_OBJECT_STORE_ENDPOINT"
	envHTTPAddr            = "THREADMILL_HTTP_ADDR"
)

type Config struct {
	DatabaseURL         string
	ObjectStoreEndpoint string
	HTTPAddr            string
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
		DatabaseURL:         lookup(env, envDatabaseURL),
		ObjectStoreEndpoint: lookup(env, envObjectStoreEndpoint),
		HTTPAddr:            lookup(env, envHTTPAddr),
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, envDatabaseURL)
	}
	if cfg.ObjectStoreEndpoint == "" {
		missing = append(missing, envObjectStoreEndpoint)
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
		Message:     fmt.Sprintf("configuration valid; http address %s", cfg.HTTPAddr),
		Recoverable: true,
	}
}
