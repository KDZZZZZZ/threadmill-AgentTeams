# Local-only M4-D focused fixture. All credentials below are isolated test
# credentials only; they are not for production.
param([switch]$BootstrapOnly, [switch]$WorkerSmoke, [switch]$ActiveRuntimeSmoke)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$docker = 'C:\Users\ASUS1\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe'
if (-not (Test-Path $docker)) { $docker = (Get-Command docker -ErrorAction Stop).Source }
$name = 'threadmill-m4d-e2e-controller'
$network = 'threadmill-m4d-e2e-net'
$volume = 'threadmill-m4d-e2e-data'
$controllerImage = 'threadmill/agentteams-embedded:m4d'
$workerImage = 'threadmill/qwenpaw-worker:m4d-current'
$result = Join-Path $env:TEMP 'threadmill-m4d-e2e-result.json'
$goCache = Join-Path $env:TEMP 'threadmill-m4d-e2e-gocache'
$provider = Join-Path $env:TEMP 'threadmill-m4e2-provider.exe'
$providerTrace = Join-Path $env:TEMP 'threadmill-m4e2-provider-trace.jsonl'
$c46Descriptor = Join-Path $env:TEMP ("threadmill-c46-worker-descriptor-" + [guid]::NewGuid().ToString('N') + '.json')

function Cleanup {
  # Docker reports an absent resource on stderr. That is normal both before
  # setup and after a partially completed fixture, so cleanup must be best
  # effort even while the fixture otherwise uses ErrorActionPreference=Stop.
  foreach ($cleanup in @(
	{ & $docker rm -f 'agentteams-worker-tm-m4d-invocation-g3-e1' 2>$null | Out-Null },
	{ & $docker rm -f 'agentteams-worker-tm-m4d-invocation-g3-e2' 2>$null | Out-Null },
	{ & $docker rm -f 'agentteams-worker-tm-m4d-conflict-invocation-g3-e2' 2>$null | Out-Null },
    { & $docker rm -f $name 2>$null | Out-Null },
    { & $docker network rm $network 2>$null | Out-Null },
    { & $docker volume rm $volume 2>$null | Out-Null }
  )) {
    try { & $cleanup } catch { }
  }
  Get-Process threadmill-m4e2-provider -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}

& $docker info | Out-Null
& $docker image inspect $controllerImage | Out-Null
& $docker image inspect $workerImage | Out-Null
Cleanup

# Mirrors install/agentteams-install.sh exactly: AppService tokens are created
# once with `openssl rand -hex 32`, then provided to both the Controller and
# its runtime Matrix registration. They remain process-local test material.
$asToken = (& $docker run --rm --entrypoint sh $controllerImage -lc 'openssl rand -hex 32' | Out-String).Trim()
$hsToken = (& $docker run --rm --entrypoint sh $controllerImage -lc 'openssl rand -hex 32' | Out-String).Trim()
if ($asToken.Length -ne 64 -or $hsToken.Length -ne 64) { throw 'official AppService token generation failed' }

try {
	Remove-Item $providerTrace -Force -ErrorAction SilentlyContinue
	$env:M4E2_PROVIDER_TRACE = $providerTrace
	& go build -o $provider (Join-Path $PSScriptRoot 'provider.go')
	if ($LASTEXITCODE -ne 0) { throw 'failed to build deterministic M4-E2 provider' }
	Start-Process -FilePath $provider -WindowStyle Hidden
	for ($i = 0; $i -lt 30; $i++) {
		try { if ((Invoke-WebRequest -UseBasicParsing http://127.0.0.1:18092/v1/models).StatusCode -eq 200) { break } } catch { }
		Start-Sleep -Milliseconds 250
	}
  & $docker network create $network | Out-Null
  & $docker volume create $volume | Out-Null
  # These variables mirror the official embedded installer local topology.
  & $docker run -d --name $name --network $network --network-alias agentteams-controller `
    --network-alias matrix-local.agentteams.io --network-alias aigw-local.agentteams.io --network-alias fs-local.agentteams.io `
    -p 18080:8080 -p 18090:8090 -v "${volume}:/data" -v '//var/run/docker.sock:/var/run/docker.sock' `
    -e AGENTTEAMS_ADMIN_USER='admin' -e AGENTTEAMS_ADMIN_PASSWORD='threadmill-it-admin' `
    -e AGENTTEAMS_MANAGER_PASSWORD='threadmill-it-manager' -e AGENTTEAMS_REGISTRATION_TOKEN='threadmill-it-registration' `
    -e AGENTTEAMS_MINIO_USER='threadmill-it' -e AGENTTEAMS_MINIO_PASSWORD='threadmill-it-secret' `
	-e AGENTTEAMS_LLM_PROVIDER='openai-compat' -e AGENTTEAMS_LLM_API_KEY='test-only-key' -e AGENTTEAMS_DEFAULT_MODEL='qwen-plus' `
	-e AGENTTEAMS_OPENAI_BASE_URL='http://host.docker.internal:18092/v1' `
    -e AGENTTEAMS_MANAGER_GATEWAY_KEY='test-only-gateway-key' `
    -e AGENTTEAMS_DEFAULT_WORKER_RUNTIME='qwenpaw' `
    -e AGENTTEAMS_QWENPAW_WORKER_IMAGE=$workerImage -e AGENTTEAMS_WORKER_IMAGE=$workerImage `
    -e AGENTTEAMS_MATRIX_DOMAIN='matrix-local.agentteams.io:8080' -e AGENTTEAMS_ELEMENT_HOMESERVER_URL='http://127.0.0.1:8080' `
    -e AGENTTEAMS_MATRIX_URL='http://127.0.0.1:6167' `
    -e AGENTTEAMS_MATRIX_E2EE='0' -e AGENTTEAMS_MINIO_ENDPOINT='http://127.0.0.1:9000' `
    -e AGENTTEAMS_MINIO_BUCKET='agentteams-storage' -e AGENTTEAMS_STORAGE_PREFIX='agentteams/agentteams-storage' `
    -e AGENTTEAMS_MATRIX_APPSERVICE_ENABLED='true' `
    -e AGENTTEAMS_MATRIX_APPSERVICE_AS_TOKEN=$asToken `
    -e AGENTTEAMS_MATRIX_APPSERVICE_HS_TOKEN=$hsToken `
    -e AGENTTEAMS_AI_GATEWAY_URL='http://aigw-local.agentteams.io:8080' `
    -e AGENTTEAMS_CONTROLLER_URL='http://agentteams-controller:8090' `
    -e AGENTTEAMS_DOCKER_NETWORK=$network `
    -e AGENTTEAMS_MANAGER_ENABLED='false' -e AGENTTEAMS_PORT_MANAGER_CONSOLE='18888' `
    -e AGENTTEAMS_FS_ENDPOINT='http://127.0.0.1:9000' `
    -e AGENTTEAMS_FS_ACCESS_KEY='threadmill-it' `
    -e AGENTTEAMS_FS_SECRET_KEY='threadmill-it-secret' `
    $controllerImage | Out-Null

  $token = $null
  for ($i = 0; $i -lt 180; $i++) {
    $token = (& $docker exec $name sh -lc 'test -s /var/run/agentteams/cli-token && cat /var/run/agentteams/cli-token' 2>$null | Out-String).Trim()
    if ($token) { break }
    Start-Sleep -Seconds 1
  }
  if (-not $token) { throw 'embedded Controller did not generate its CLI token; inspect docker logs threadmill-m4d-e2e-controller' }
  $tokenMode = (& $docker exec $name sh -lc 'stat -c %a /var/run/agentteams/cli-token' 2>$null | Out-String).Trim()
  if ($tokenMode -notmatch '^[0-7]{3,4}$') { throw 'embedded Controller CLI token file has no usable mode' }
  # Do not print logs or token text. A direct string containment assertion is
  # enough to prove this fixture's ephemeral AppService values were not logged.
  $bootstrapLog = (& $docker logs $name 2>&1 | Out-String)
  if ($bootstrapLog.Contains($asToken) -or $bootstrapLog.Contains($hsToken)) { throw 'embedded bootstrap leaked an AppService token to logs' }
  $env:M4D_CONTROLLER_URL = 'http://127.0.0.1:18090'
  $env:M4D_CONTROLLER_TOKEN = $token
	$env:M4D_MATRIX_URL = 'http://127.0.0.1:18080'
	$env:M4D_MATRIX_ADMIN_USER = 'admin'
	$env:M4D_MATRIX_ADMIN_PASSWORD = 'threadmill-it-admin'
	$env:M4D_PROVIDER_URL = 'http://127.0.0.1:18092'
  $env:M4D_DOCKER = $docker
  $env:M4D_RESULT = $result
  $env:GOCACHE = $goCache
  $env:GOPROXY = 'https://goproxy.cn,direct'
  if ($BootstrapOnly) {
    $response = Invoke-WebRequest -UseBasicParsing -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers" -Headers @{ Authorization = "Bearer $token" }
    if ($response.StatusCode -ne 200) { throw "Controller workers API returned HTTP $($response.StatusCode)" }
    [pscustomobject]@{ controllerReady = $true; cliTokenPresent = $true; cliTokenMode = $tokenMode; workersStatus = $response.StatusCode; appServiceTokensRedacted = $true } | ConvertTo-Json
    return
  }
  if ($WorkerSmoke -or $ActiveRuntimeSmoke) {
    $invocationID = "c46-" + [guid]::NewGuid().ToString('N').Substring(0, 12)
    $taskID = "tm-phase-$invocationID-g1-e1"
    $workerName = "tm-$invocationID-g1-e1"
    $credential = $null
    try {
      $credential = Invoke-RestMethod -Method Post -Uri "$env:M4D_CONTROLLER_URL/api/v1/mcp-credentials" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body (@{ workerName = $workerName; headerName = 'X-Threadmill-Execution-Token'; secretValue = 'c4-6-test-only-token' } | ConvertTo-Json -Compress)
      $body = @{ name = $workerName; workerName = $workerName; model = 'qwen-plus'; modelProvider = 'openai-compat'; runtime = 'qwenpaw'; image = $workerImage; mcpServers = @(@{ name = 'threadmill'; url = 'http://host.docker.internal:18091/mcp'; transport = 'streamable_http'; credentialBindingRef = $credential.id }) } | ConvertTo-Json -Depth 6 -Compress
      Invoke-RestMethod -Method Post -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body $body | Out-Null
      $ready = $false
      for ($i = 0; $i -lt 180; $i++) {
        try {
          Invoke-RestMethod -Method Post -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName/ensure-ready" -Headers @{ Authorization = "Bearer $token" } | Out-Null
          $status = Invoke-RestMethod -Method Get -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName/status" -Headers @{ Authorization = "Bearer $token" }
          if ($status.name -eq $workerName -and $status.phase -eq 'ready' -and $status.containerState -eq 'running') { $ready = $true; break }
        } catch { }
        Start-Sleep -Seconds 1
      }
      if (-not $ready) { throw 'Worker did not reach Controller Ready state' }
      $mcp = @($status.runtimeConfig.mcpServers | Where-Object { $_.name -eq 'threadmill' }) | Select-Object -First 1
      if (-not $mcp -or -not $mcp.applied -or $mcp.removed -or $mcp.error) { throw 'Worker MCP readback was not applied' }
      if (-not $status.runtimeConfig.desiredGeneration -or $status.runtimeConfig.desiredGeneration -ne $status.runtimeConfig.appliedGeneration) { throw 'Worker runtime generation readback was not applied' }
      # Invoke the official TeamHarness MCP stdio server in the real Worker.
      # This mirrors the existing Go adapter's public taskflow calls; task
      # state remains owned by TeamHarness and is polled below.
      $container = "agentteams-worker-$workerName"
      $server = '/opt/agentteams/qwenpaw-builtin/plugins/teamharness/teamharness/mcp/server.py'
      $baseArgs = @('exec','-i','-e','AGENTTEAMS_WORKER_MATRIX_TOKEN',$container,'/opt/venv/qwenpaw/bin/python',$server)
      $delegate = @{jsonrpc='2.0';id=1;method='tools/call';params=@{name='taskflow';arguments=@{role='leader';workspaceDir='/root/agentteams-fs';action='delegate_task';payload=@{projectId='threadmill-c46';taskId=$taskID;roomId=$status.roomID;assignedTo=$status.matrixUserID;title='C4-6 active carrier';spec='Keep this TeamHarness task active; do not submit a result or PhaseOutput.'}}}}
      ($delegate | ConvertTo-Json -Depth 12 -Compress) | & $docker @baseArgs | Out-Null
      if ($LASTEXITCODE -ne 0) { throw 'TeamHarness delegate_task failed' }
      $ack = @{jsonrpc='2.0';id=1;method='tools/call';params=@{name='taskflow';arguments=@{role='worker';workspaceDir='/root/agentteams-fs';action='ack_task';payload=@{taskId=$taskID}}}}
      ($ack | ConvertTo-Json -Depth 10 -Compress) | & $docker @baseArgs | Out-Null
      if ($LASTEXITCODE -ne 0) { throw 'TeamHarness ack_task failed' }
      $taskActive = $false
      for ($i = 0; $i -lt 60; $i++) {
        $check = @{jsonrpc='2.0';id=1;method='tools/call';params=@{name='taskflow';arguments=@{role='leader';workspaceDir='/root/agentteams-fs';action='check_task';payload=@{taskId=$taskID}}}}
        $taskReadback = (($check | ConvertTo-Json -Depth 10 -Compress) | & $docker @baseArgs | Out-String)
        if ($LASTEXITCODE -eq 0 -and $taskReadback -match 'in_progress') { $taskActive = $true; break }
        Start-Sleep -Milliseconds 500
      }
      if (-not $taskActive) { throw 'TeamHarness task did not reach in_progress' }
      $descriptor = [ordered]@{
		taskID = 'c46-real-worker'
		invocationID = $invocationID
		generation = 1
		executionEpoch = 1
        workerReady = $true
        workerName = $status.name
        workerID = $status.name
        workerPhase = $status.phase
        containerState = $status.containerState
        runtimeDesiredGeneration = [long]$status.runtimeConfig.desiredGeneration
        runtimeAppliedGeneration = [long]$status.runtimeConfig.appliedGeneration
        mcpClientID = $mcp.name
        mcpApplied = [bool]$mcp.applied
        credentialRef = $credential.id
        matrixRoomRef = if ($status.roomID) { "matrix:$($status.roomID)" } else { $null }
		teamHarnessTaskID = $taskID
      }
      if ($descriptor.workerName -ne $workerName -or $descriptor.workerID -ne $workerName -or $descriptor.workerPhase -ne 'ready' -or $descriptor.containerState -ne 'running' -or $descriptor.mcpClientID -eq '' -or $descriptor.credentialRef -eq '') { throw 'Worker execution descriptor identity mismatch' }
      # Cross a real process boundary with only the redacted descriptor. The
      # Go test parent starts Runtime A; Runtime A alone opens and writes the
      # temporary SQLite repository, closes it, and verifies a cold reopen.
      [System.IO.File]::WriteAllText($c46Descriptor, ($descriptor | ConvertTo-Json -Depth 5 -Compress), [System.Text.UTF8Encoding]::new($false))
      $env:THREADMILL_C46_WORKER_DESCRIPTOR = $c46Descriptor
      if ($ActiveRuntimeSmoke) {
        $env:THREADMILL_C46_ACTIVE_CRASH_POINT = '1'
        $env:THREADMILL_C46_CONTROLLER_URL = $env:M4D_CONTROLLER_URL
        $env:THREADMILL_C46_CONTROLLER_TOKEN = $token
        $env:THREADMILL_C46_DOCKER = $docker
        $env:THREADMILL_C46_WORKER_CONTAINER = $container
        $env:THREADMILL_C46_TEAMHARNESS_PYTHON = '/opt/venv/qwenpaw/bin/python'
        $env:THREADMILL_C46_TEAMHARNESS_SERVER = $server
        $env:THREADMILL_C46_TEAMHARNESS_WORKSPACE = '/root/agentteams-fs'
      }
      Remove-Item Env:M4D_CONTROLLER_TOKEN -ErrorAction SilentlyContinue
      Remove-Item Env:M4D_MATRIX_ADMIN_PASSWORD -ErrorAction SilentlyContinue
      & go test ./internal/runtime -run '^TestC46RealWorkerDescriptorDurablePhysicalExecution$' -count=1 -v
      if ($LASTEXITCODE -ne 0) { throw 'C4-6 real Worker descriptor durable Runtime A test failed' }
      $env:M4D_CONTROLLER_TOKEN = $token
      $env:M4D_MATRIX_ADMIN_PASSWORD = 'threadmill-it-admin'
      if ($ActiveRuntimeSmoke) {
        # This is a fresh Controller readback after Runtime A was killed by
        # its parent test process. It proves the fixture did not use Runtime
        # shutdown/teardown to reach the crash point.
        $afterCrash = Invoke-RestMethod -Method Get -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName/status" -Headers @{ Authorization = "Bearer $token" }
        $afterCrashMCP = @($afterCrash.runtimeConfig.mcpServers | Where-Object { $_.name -eq $descriptor.mcpClientID }) | Select-Object -First 1
        & $docker container inspect ("agentteams-worker-" + $workerName) | Out-Null
        $postKillTaskActive = $false
        $postKillTaskIdentity = $false
        for ($i = 0; $i -lt 60; $i++) {
          $check = @{jsonrpc='2.0';id=1;method='tools/call';params=@{name='taskflow';arguments=@{role='leader';workspaceDir='/root/agentteams-fs';action='check_task';payload=@{taskId=$descriptor.teamHarnessTaskID}}}}
          $postKillTask = (($check | ConvertTo-Json -Depth 10 -Compress) | & $docker @baseArgs | Out-String)
          if ($LASTEXITCODE -eq 0 -and $postKillTask -match [regex]::Escape($descriptor.teamHarnessTaskID) -and $postKillTask -match [regex]::Escape($workerName) -and $postKillTask -match 'in_progress') { $postKillTaskActive = $true; $postKillTaskIdentity = $true; break }
          Start-Sleep -Milliseconds 500
        }
        $postKillWorkerReady = $LASTEXITCODE -eq 0 -and $afterCrash.name -eq $workerName -and $afterCrash.phase -eq 'ready' -and $afterCrash.containerState -eq 'running' -and $afterCrash.runtimeConfig.desiredGeneration -eq $afterCrash.runtimeConfig.appliedGeneration -and $afterCrashMCP -and $afterCrashMCP.applied
        if (-not $postKillWorkerReady -or -not $postKillTaskActive) { throw 'Runtime crash point did not retain active Worker/task/controller evidence' }
        [pscustomobject]@{ runtimeACrashed=$true; postKillWorkerReady=$true; postKillTaskReadback=$true; postKillTaskActive=$postKillTaskActive; teamHarnessTaskIDMatches=$postKillTaskIdentity; observerIdentityInputsAligned=($postKillWorkerReady -and $postKillTaskIdentity); productionObserverReady=($postKillWorkerReady -and $postKillTaskActive -and $postKillTaskIdentity) } | ConvertTo-Json -Compress
      }
      Invoke-RestMethod -Method Delete -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName" -Headers @{ Authorization = "Bearer $token" } | Out-Null
      $deleted = $false
      for ($i = 0; $i -lt 60; $i++) { try { Invoke-RestMethod -Method Get -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName/status" -Headers @{ Authorization = "Bearer $token" } | Out-Null } catch { $deleted = $true; break }; Start-Sleep -Milliseconds 500 }
      if (-not $deleted) { throw 'Worker delete did not converge to not-found' }
      [pscustomobject]@{ workerCreated = $true; workerReady = $true; workerDeleted = $true; workerNameMatches = $true; descriptor = $descriptor } | ConvertTo-Json -Depth 5
      return
    } finally {
      try { Invoke-RestMethod -Method Delete -Uri "$env:M4D_CONTROLLER_URL/api/v1/workers/$workerName" -Headers @{ Authorization = "Bearer $token" } | Out-Null } catch { }
      if ($credential -and $credential.id) { try { Invoke-RestMethod -Method Post -Uri "$env:M4D_CONTROLLER_URL/api/v1/mcp-credentials/$($credential.id)/revoke" -Headers @{ Authorization = "Bearer $token" } | Out-Null } catch { } }
    }
  }
  Remove-Item $result -Force -ErrorAction SilentlyContinue
	& go run (Join-Path $PSScriptRoot 'runner.go')
	if ($LASTEXITCODE -ne 0) {
		if (Test-Path $providerTrace) { Get-Content $providerTrace }
		throw 'M4-D focused runner failed'
	}
  Get-Content -Raw $result
}
finally {
  Remove-Item Env:M4D_CONTROLLER_TOKEN -ErrorAction SilentlyContinue
	Remove-Item Env:M4D_MATRIX_ADMIN_PASSWORD -ErrorAction SilentlyContinue
	Remove-Item Env:M4D_MATRIX_ADMIN_USER -ErrorAction SilentlyContinue
	Remove-Item Env:M4D_MATRIX_URL -ErrorAction SilentlyContinue
	Remove-Item Env:M4D_PROVIDER_URL -ErrorAction SilentlyContinue
  Remove-Item Env:GOCACHE -ErrorAction SilentlyContinue
  Remove-Item Env:GOPROXY -ErrorAction SilentlyContinue
	Remove-Item Env:M4E2_PROVIDER_TRACE -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_WORKER_DESCRIPTOR -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_ACTIVE_CRASH_POINT -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_CONTROLLER_URL -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_CONTROLLER_TOKEN -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_DOCKER -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_WORKER_CONTAINER -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_TEAMHARNESS_PYTHON -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_TEAMHARNESS_SERVER -ErrorAction SilentlyContinue
  Remove-Item Env:THREADMILL_C46_TEAMHARNESS_WORKSPACE -ErrorAction SilentlyContinue
  Remove-Item $c46Descriptor -Force -ErrorAction SilentlyContinue
  Remove-Item $goCache -Recurse -Force -ErrorAction SilentlyContinue
	Remove-Item $provider -Force -ErrorAction SilentlyContinue
	Remove-Item $providerTrace -Force -ErrorAction SilentlyContinue
  $asToken = $null
  $hsToken = $null
  Cleanup
}
