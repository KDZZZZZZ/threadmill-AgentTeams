package agentteams

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/kernel"
)

const qwenPawDockerHTTPBridge = `
import base64
import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError

payload = json.loads(sys.stdin.read())
body = base64.b64decode(payload.get("body", ""))
request = Request(
    payload["url"],
    data=body if payload["method"] not in ("GET", "HEAD") else None,
    method=payload["method"],
    headers=payload.get("headers") or {},
)
try:
    response = urlopen(request, timeout=payload.get("timeout", 10))
    status = response.getcode()
    headers = dict(response.headers.items())
    raw = response.read(payload.get("limit", 1048576) + 1)
except HTTPError as err:
    status = err.code
    headers = dict(err.headers.items())
    raw = err.read(payload.get("limit", 1048576) + 1)
print(json.dumps({
    "status": status,
    "headers": headers,
    "body": base64.b64encode(raw).decode("ascii"),
}, ensure_ascii=False))
`

type DockerExecRoundTripper struct {
	DockerBinary  string
	PythonBinary  string
	Container     string
	ResponseLimit int64
	Timeout       time.Duration
}

func NewDockerExecRoundTripper(container, dockerBinary, pythonBinary string) (*DockerExecRoundTripper, error) {
	container = strings.TrimSpace(container)
	if !safeContainerName(container) {
		return nil, kernel.InvalidArgument("QwenPaw container name is invalid")
	}
	if strings.TrimSpace(dockerBinary) == "" {
		dockerBinary = "docker"
	}
	if strings.TrimSpace(pythonBinary) == "" {
		pythonBinary = "/opt/venv/qwenpaw/bin/python"
	}
	return &DockerExecRoundTripper{
		DockerBinary:  dockerBinary,
		PythonBinary:  pythonBinary,
		Container:     container,
		ResponseLimit: qwenPawResponseLimit,
		Timeout:       10 * time.Second,
	}, nil
}

func (t *DockerExecRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil {
		return nil, kernel.Error{Code: kernel.CodeInternalError, Message: "QwenPaw docker HTTP transport is not configured"}
	}
	var rawBody []byte
	if req.Body != nil {
		var err error
		rawBody, err = io.ReadAll(io.LimitReader(req.Body, 2<<20+1))
		if err != nil {
			return nil, err
		}
	}
	if len(rawBody) > 2<<20 {
		return nil, kernel.InvalidArgument("QwenPaw API request body exceeds the size limit")
	}
	headers := map[string]string{}
	for key, values := range req.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	payload, err := json.Marshal(map[string]any{
		"method":  req.Method,
		"url":     req.URL.String(),
		"headers": headers,
		"body":    base64.StdEncoding.EncodeToString(rawBody),
		"timeout": int(t.Timeout.Seconds()),
		"limit":   t.ResponseLimit,
	})
	if err != nil {
		return nil, err
	}
	ctx := req.Context()
	cmd := exec.CommandContext(ctx, t.DockerBinary, "exec", "-i", t.Container, t.PythonBinary, "-c", qwenPawDockerHTTPBridge)
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdin = bytes.NewReader(payload)
	stdout := newCappedOutput(int(t.ResponseLimit) + 4096)
	stderr := newCappedOutput(64 << 10)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw docker HTTP bridge failed", Recoverable: true}
	}
	var bridged struct {
		Status  int               `json:"status"`
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &bridged); err != nil {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw docker HTTP bridge returned invalid JSON", Recoverable: true}
	}
	body, err := base64.StdEncoding.DecodeString(bridged.Body)
	if err != nil {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw docker HTTP bridge returned invalid body", Recoverable: true}
	}
	if int64(len(body)) > t.ResponseLimit {
		return nil, kernel.Error{Code: kernel.CodeExecutorUnavailable, Message: "QwenPaw API response exceeded the limit", Recoverable: true}
	}
	header := http.Header{}
	for key, value := range bridged.Headers {
		header.Set(key, value)
	}
	return &http.Response{
		StatusCode: bridged.Status,
		Status:     strconv.Itoa(bridged.Status),
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}, nil
}
