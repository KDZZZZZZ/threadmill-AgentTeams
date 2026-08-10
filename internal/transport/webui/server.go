package webui

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type Options struct {
	DistDir string
}

type Server struct {
	realRoot  string
	indexPath string
}

func New(options Options) (*Server, error) {
	if strings.TrimSpace(options.DistDir) == "" {
		return nil, errors.New("webui dist dir is required")
	}
	root, err := filepath.Abs(options.DistDir)
	if err != nil {
		return nil, fmt.Errorf("resolve webui dist dir: %w", err)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("webui dist dir is not readable: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("webui dist dir must not be a symbolic link")
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("webui dist dir is not readable: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("webui dist dir must be a directory")
	}
	indexPath, err := safeJoin(root, "index.html")
	if err != nil {
		return nil, fmt.Errorf("webui index.html is invalid: %w", err)
	}
	indexInfo, err := os.Stat(indexPath)
	if err != nil {
		return nil, fmt.Errorf("webui index.html is required: %w", err)
	}
	if indexInfo.IsDir() {
		return nil, errors.New("webui index.html must be a file")
	}
	return &Server{realRoot: root, indexPath: indexPath}, nil
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.ServeHTTP)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestPath, ok := cleanURLPath(r.URL)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if isReservedRoute(requestPath) {
		http.NotFound(w, r)
		return
	}
	if requestPath != "/" {
		rel := strings.TrimPrefix(requestPath, "/")
		if filePath, ok := s.staticFilePath(rel); ok {
			s.serveFile(w, r, filePath, cacheControlFor(requestPath))
			return
		}
	}
	s.serveFile(w, r, s.indexPath, "no-cache")
}

func cleanURLPath(u *url.URL) (string, bool) {
	raw := u.EscapedPath()
	if raw == "" {
		raw = "/"
	}
	unescaped, err := url.PathUnescape(raw)
	if err != nil {
		return "", false
	}
	if !strings.HasPrefix(unescaped, "/") || strings.ContainsAny(unescaped, "\\\x00") {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(unescaped, "/"), "/")
	for _, part := range parts {
		if part == "." || part == ".." {
			return "", false
		}
	}
	return path.Clean("/" + strings.TrimPrefix(unescaped, "/")), true
}

func isReservedRoute(requestPath string) bool {
	switch requestPath {
	case "/v1", "/mcp", "/healthz", "/readyz":
		return true
	}
	return strings.HasPrefix(requestPath, "/v1/") || strings.HasPrefix(requestPath, "/mcp/")
}

func (s *Server) staticFilePath(rel string) (string, bool) {
	filePath, err := safeJoin(s.realRoot, rel)
	if err != nil {
		return "", false
	}
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		return "", false
	}
	return filePath, true
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request, filePath, cacheControl string) {
	file, err := os.Open(filePath)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	if cacheControl != "" {
		w.Header().Set("Cache-Control", cacheControl)
	}
	http.ServeContent(w, r, info.Name(), info.ModTime(), file)
}

func safeJoin(root, rel string) (string, error) {
	rel = filepath.FromSlash(rel)
	if filepath.IsAbs(rel) {
		return "", errors.New("absolute paths are not allowed")
	}
	candidate := filepath.Join(root, rel)
	inside, err := isInside(root, candidate)
	if err != nil {
		return "", err
	}
	if !inside {
		return "", errors.New("path escapes webui dist dir")
	}
	current := root
	for _, segment := range strings.Split(rel, string(filepath.Separator)) {
		if segment == "" || segment == "." {
			continue
		}
		current = filepath.Join(current, segment)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return candidate, nil
			}
			return "", statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("symbolic links are not allowed below webui dist dir")
		}
	}
	return candidate, nil
}

func isInside(root, candidate string) (bool, error) {
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false, err
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))), nil
}

func cacheControlFor(requestPath string) string {
	if strings.HasPrefix(requestPath, "/assets/") {
		return "public, max-age=31536000, immutable"
	}
	return "no-cache"
}
