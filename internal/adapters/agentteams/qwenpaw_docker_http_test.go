package agentteams

import (
	"strings"
	"testing"
)

func TestDockerExecRoundTripperKeepsRequestBodyOnStdin(t *testing.T) {
	transport, err := NewDockerExecRoundTripper("qwenpaw-worker", "docker", "python")
	if err != nil {
		t.Fatal(err)
	}
	if transport.Container != "qwenpaw-worker" || transport.DockerBinary != "docker" || transport.PythonBinary != "python" {
		t.Fatalf("transport = %#v", transport)
	}
	for _, forbidden := range []string{
		"sys.argv",
		"Authorization",
		"Bearer",
		"OPENAI_API_KEY",
		"DEEPSEEK_API_KEY",
	} {
		if strings.Contains(qwenPawDockerHTTPBridge, forbidden) {
			t.Fatalf("docker HTTP bridge contains forbidden request/secret surface %q", forbidden)
		}
	}
	for _, required := range []string{"sys.stdin.read()", "urlopen(request", "base64.b64decode", "base64.b64encode"} {
		if !strings.Contains(qwenPawDockerHTTPBridge, required) {
			t.Fatalf("docker HTTP bridge missing stdin/base64 boundary %q", required)
		}
	}
	if _, err := NewDockerExecRoundTripper("worker;bad", "docker", "python"); err == nil {
		t.Fatal("unsafe container name accepted")
	}
}
