# C4-6 restart recovery fixtures

These Windows/PowerShell fixtures validate the C4-6 execution-plane and
process-restart boundary. They reuse the audited M4 embedded AgentTeams
bootstrap in `testdata/m4_d_rehydration_e2e/run.ps1`; they do not duplicate
its AppService, Matrix, MinIO, Docker-network, readiness, credential, or final
cleanup logic.

The local images `threadmill/agentteams-embedded:m4d` and
`threadmill/qwenpaw-worker:m4d-current` must already exist. The fixtures use
only isolated test credentials and remove their Worker, credential,
Controller container, network, and volume in `finally` cleanup.

## Entry points

### Controller smoke

```powershell
./testdata/c4_6_restart_e2e/run-smoke.ps1
```

Runs the shared bootstrap in `-BootstrapOnly` mode. It verifies Docker and the
cached images, starts the real embedded Controller topology, obtains the
Controller-issued CLI token without printing it, calls the real
`GET /api/v1/workers` endpoint, and performs final cleanup. It does not create
a Worker or open Runtime SQLite.

### Worker lifecycle and durable descriptor smoke

```powershell
./testdata/c4_6_restart_e2e/run-worker-smoke.ps1
```

Runs the shared bootstrap in `-WorkerSmoke` mode and verifies:

1. creation of a real deterministic Worker and Controller `Ready/running`;
2. desired/applied runtime generation and MCP applied readback;
3. real TeamHarness `delegate_task`, worker `ack_task`, and leader
   `check_task` reaching `in_progress`;
4. a redacted execution descriptor containing Worker/task/runtime/MCP
   identity but no capability value;
5. descriptor handoff across an OS-process boundary to Runtime A;
6. Runtime A opening SQLite itself and persisting durable input,
   continuation, Waiting, PhysicalExecution, package receipt, and atomic
   activation to `running`;
7. cold reopen with one PhysicalExecution, one receipt, and no PhaseOutput;
8. real Worker deletion and final fixture cleanup.

### Active Runtime crash and healthy-carrier recovery

```powershell
./testdata/c4_6_restart_e2e/run-active-runtime-smoke.ps1
```

Runs the shared bootstrap in `-ActiveRuntimeSmoke` mode. It extends the Worker
smoke with the current C4-6 healthy-carrier checkpoint:

1. Runtime A reaches a receipt-backed durable active state and cold-reopens
   its repository;
2. the parent kills Runtime A with OS `Process.Kill`, without Runtime Close or
   teardown;
3. fresh Controller readback confirms the Worker remains `Ready/running`,
   runtime generation is applied, and MCP is applied;
4. a fresh real TeamHarness `check_task` confirms the same task remains
   `in_progress` after the kill;
5. an independent Runtime B process reopens the same SQLite and discovers
   exactly one recovery candidate;
6. Runtime B composes `RuntimeStateRepository`, durable discovery,
   `RecoveryCoordinator`, production `ControllerPhysicalExecutionObserver`,
   and `RestartRecoverySupervisor`; recovery-claim persistence is supplied by
   the repository's `RecoveryStateStore`;
7. production Controller/TeamHarness readback verifies deterministic Worker
   and task identity, runtime generation, MCP, and durable physical identity;
8. `ReconcileInvocation` returns `carrier_still_active` and the supervisor
   reports `stable_active`;
9. the RecoveryClaim is released and the original ExecutionEpoch is retained;
10. no replacement, second PhysicalExecution, duplicate receipt, PhaseOutput,
    or teardown progress is produced;
11. only after recovery assertions pass does final cleanup run.

The process and repository assertions live in
`internal/runtime/restart_process_harness_test.go`. Runtime B's production
observer composition and no-duplicate assertions live in
`internal/runtime/restart_production_observer_test.go`.

The healthy fixture does not prove lost-carrier replacement. Lost/failed
Worker observation, successor epoch reservation, fresh reprovisioning, and
stale old-epoch rejection remain the next C4-6 Docker/process slice.
