# M4-D physical rehydration Docker fixture

This local-only fixture validates the M4-D carrier replacement boundary with
the official embedded AgentTeams Controller and a real QwenPaw worker. It is
not a production launcher and must never be used with shared credentials.

`run.ps1` starts the embedded Controller using the previously-built local
images, waits for its controller-issued CLI token without printing it, and
runs `runner.go`. The runner:

1. starts the production Threadmill Phase MCP HTTP handler on a dynamic local
   port;
2. uses one `BindingRegistryAuthorizationIssuer` and the same registry-backed
   handler to issue the fresh epoch-2 token;
3. asks the Controller to create the worker-scoped MCP credential and Worker;
4. waits for the Controller's redacted applied-generation readback;
5. performs a real `tools/list` request through the QwenPaw MCP client;
6. persists epoch-B before delegation, then uses the official TeamHarness MCP
   server for `delegate_task` and `check_task`; and
7. observes worker `ack_task` through the real taskflow state before the
   provisioner CASes the `WaitingRecord` from `rehydrating` to `running`.

The runner also executes a separate final-CAS conflict path. It confirms the
real TeamHarness task is cancelled, the Worker/credential/token/lease are
released, and the failed epoch-B record remains as redacted evidence.

The fixture deliberately records only token SHA-256 prefixes, never token
material. It uses fixed local test credentials only; they are not production
credentials. Containers, credentials, workers and local state are removed by
the PowerShell `finally` block after evidence is written.

Run from this worktree on Windows PowerShell with Docker Desktop running:

```powershell
./testdata/m4_d_rehydration_e2e/run.ps1
```

The runner requires the local images `threadmill/agentteams-embedded:m4d` and
`threadmill/qwenpaw-worker:m3c-observability`; it does not build or modify
AgentTeams sources.
