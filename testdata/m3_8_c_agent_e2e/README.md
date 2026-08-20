# M3.8-C QwenPaw agent E2E fixture

This is a local, test-only fixture. It exposes a deterministic OpenAI-compatible
streaming provider and a real Threadmill MCP HTTP server on one local process.
It must only be used with a local QwenPaw worker and test credentials.

The provider emits two distinct OpenAI tool-call IDs:

- `call-artifact-register`
- `call-submit-phase-output`

The second call is emitted only after the next model request contains the
ArtifactRef returned by the real first tool execution. Runtime traces record
only JSON-RPC method, tool name, response category, and non-secret invocation
ID; no token/header value is recorded.
