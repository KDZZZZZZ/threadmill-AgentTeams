package auth

import (
	"crypto/subtle"
	"net/http"
	"net/url"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const CSRFHeaderName = "X-Threadmill-CSRF"

type StateChangeGuard struct {
	AllowedOrigins map[string]struct{}
}

func NewStateChangeGuard(origins []string) StateChangeGuard {
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		allowed[origin] = struct{}{}
	}
	return StateChangeGuard{AllowedOrigins: allowed}
}

func (g StateChangeGuard) Check(r *http.Request, session SessionRecord) error {
	if isSafeMethod(r.Method) {
		return nil
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return kernel.Error{Code: kernel.CodeOriginInvalid, Message: "state changing request is missing Origin"}
	}
	if _, ok := g.AllowedOrigins[origin]; !ok {
		return kernel.Error{Code: kernel.CodeOriginInvalid, Message: "state changing request origin is not allowed"}
	}
	csrf := r.Header.Get(CSRFHeaderName)
	csrfHash := HashOpaqueSecret(csrf)
	if csrf == "" || subtle.ConstantTimeCompare(csrfHash, session.CSRFHash) != 1 {
		return kernel.Error{Code: kernel.CodeCSRFInvalid, Message: "state changing request has invalid CSRF token"}
	}
	return nil
}

func OriginFromURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if !parsed.IsAbs() || parsed.Host == "" {
		return "", kernel.InvalidArgument("origin must be an absolute http or https URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", kernel.InvalidArgument("origin scheme must be http or https")
	}
	if parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return "", kernel.InvalidArgument("origin must not include path, query, fragment, or user info")
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

func isSafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}
