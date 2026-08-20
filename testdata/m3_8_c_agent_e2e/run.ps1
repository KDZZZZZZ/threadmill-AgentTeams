# Local-only M3.8-C runner. Requires Docker Desktop and the locally built
# threadmill/qwenpaw-worker:m3c-observability image. It retains non-sensitive
# evidence in $env:TEMP. -MCPOnly stops before the agent loop.
param([switch]$MCPOnly,[switch]$DenySubmit,[switch]$DuplicateCallIDs)

$ErrorActionPreference='Stop'
$r=Join-Path $env:TEMP 'threadmill-m3c';$n='threadmill-m3c-net';$mi='threadmill-m3c-minio';$w='threadmill-m3c-worker';$img='threadmill/qwenpaw-worker:m3c-observability';$exe=Join-Path $env:TEMP 'threadmill-m3c-provider.exe'
function Clean {
  docker rm -f $w $mi 2>$null|Out-Null
  docker network rm $n 2>$null|Out-Null
  if($script:p){Stop-Process -Id $script:p.Id -Force -ErrorAction SilentlyContinue}
  Get-Process threadmill-m3c-provider -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}
Clean
Remove-Item "$r\mcp-trace.jsonl","$r\qwenpaw.log","$r\result.json","$r\token","$r\output.json","$r\events.jsonl" -Force -ErrorAction SilentlyContinue
try {
New-Item -ItemType Directory -Force -Path "$r\seed\agents\m3c-worker\runtime","$r\workspace\out"|Out-Null
$env:M3C_WORKSPACE="$r\workspace";$env:M3C_TOKEN_FILE="$r\token";$env:M3C_OUTPUT="$r\output.json";$env:M3C_EVENTS="$r\events.jsonl";$env:M3C_MCP_TRACE="$r\mcp-trace.jsonl";$env:M3C_DENY_SUBMIT=$(if($DenySubmit){'1'}else{''});$env:M3C_DUPLICATE_CALL_IDS=$(if($DuplicateCallIDs){'1'}else{''})
& go build -o $exe (Join-Path $PSScriptRoot 'provider.go')
if($LASTEXITCODE -ne 0){throw 'failed to build the local M3.8-C provider fixture'}
$script:p=Start-Process $exe -PassThru -WindowStyle Hidden
for($i=0;$i-lt 20;$i++){
  if((Test-Path $env:M3C_TOKEN_FILE) -and -not [string]::IsNullOrWhiteSpace((Get-Content -Raw $env:M3C_TOKEN_FILE))){break}
  sleep 1
}
$t=Get-Content -Raw $env:M3C_TOKEN_FILE
if([string]::IsNullOrWhiteSpace($t)){throw 'M3.8-C provider did not issue a trusted execution token'}
@"
metadata:
  generation: "1"
team:
  name: m3c
member:
  name: m3c-worker
  runtimeName: m3c-worker
  runtime: qwenpaw
  role: worker
desired:
  model:
    providerId: dashscope
    providerName: M3C
    model: qwen-plus
    baseUrl: http://host.docker.internal:18091/v1
    apiKeyEnv: AGENTTEAMS_WORKER_GATEWAY_KEY
  mcpServers:
    threadmill:
      url: http://host.docker.internal:18091/mcp
      transport: streamable_http
      headers:
        X-Threadmill-Execution-Token: $t
  channelPolicy: {}
credentials:
  gatewayKeyEnv: AGENTTEAMS_WORKER_GATEWAY_KEY
"@|Set-Content "$r\seed\agents\m3c-worker\runtime\runtime.yaml"
Set-Content "$r\seed\agents\m3c-worker\SOUL.md" '# M3C'
docker network create $n|Out-Null;docker run -d --name $mi --network $n --network-alias minio -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data|Out-Null
$mount="type=bind,src=$(($r+'\seed')-replace '\\','/'),dst=/tmp/seed,readonly";$mc='/usr/local/bin/mc.bin mb --ignore-existing local/agentteams-storage && /usr/local/bin/mc.bin cp --recursive /tmp/seed/ local/agentteams-storage/'
for($i=0;$i-lt 30;$i++){docker run --rm --network $n --mount $mount --entrypoint sh -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 $img -lc $mc *> $null;if($LASTEXITCODE-eq 0){break};sleep 2}
docker run -d --name $w --network $n -p 18088:8088 -e AGENTTEAMS_WORKER_NAME=m3c-worker -e AGENTTEAMS_WORKER_CR_NAME=m3c-worker -e AGENTTEAMS_FS_ENDPOINT=http://minio:9000 -e AGENTTEAMS_FS_ACCESS_KEY=minioadmin -e AGENTTEAMS_FS_SECRET_KEY=minioadmin -e AGENTTEAMS_FS_BUCKET=agentteams-storage -e AGENTTEAMS_WORKER_GATEWAY_KEY=test-only-key $img|Out-Null
if($MCPOnly){
  $entry=$null
  for($i=0;$i-lt 90;$i++){
    try{$entry=@((Invoke-RestMethod http://127.0.0.1:18088/api/mcp)|Where-Object {$_.key -eq 'threadmill' -or $_.name -eq 'threadmill'})[0];if($entry){break}}catch{}
    $workerLog=docker logs $w 2>&1|Out-String
    if($workerLog -match 'QwenPaw API POST /api/mcp failed with HTTP 422'){break}
    sleep 1
  }
  docker logs $w 2>&1|Set-Content "$r\qwenpaw.log"
  $headerNames=if($entry -and $entry.headers){@($entry.headers.PSObject.Properties.Name)}else{@()}
  [pscustomobject]@{workerLog="$r\qwenpaw.log";mcpTrace="$r\mcp-trace.jsonl";mcpOnly=$true;mcpClientFound=($null -ne $entry);enabled=$entry.enabled;transport=$entry.transport;url=$entry.url;headerNames=$headerNames}|ConvertTo-Json -Depth 4|Set-Content "$r\result.json"
  Clean
  exit 0
}
for($i=0;$i-lt 90;$i++){
  try{$client=@((Invoke-RestMethod http://127.0.0.1:18088/api/mcp)|Where-Object {$_.key -eq 'threadmill' -or $_.name -eq 'threadmill'})[0];if($client){break}}catch{}
  $workerLog=docker logs $w 2>&1|Out-String
  if($workerLog -match 'QwenPaw API POST /api/mcp failed with HTTP 422'){break}
  sleep 1
}
$body=@{input=@(@{role='user';content=@(@{type='text';text='run tools'})});session_id='m3c';user_id='fixture'}|ConvertTo-Json -Depth 8
$job=Start-Job {param($b)Invoke-WebRequest -Method Post -Uri http://127.0.0.1:18088/api/console/chat -Headers @{'X-Agent-Id'='default'} -ContentType application/json -Body $b -TimeoutSec 180}-ArgumentList $body
for($i=0;$i-lt 120 -and !(Test-Path $env:M3C_OUTPUT);$i++){try{$a=Invoke-RestMethod http://127.0.0.1:18088/api/approval/list;foreach($x in @($a.pending_approvals)){@{request_id=$x.request_id;session_id=$x.root_session_id;user_id='fixture'}|ConvertTo-Json|%{Invoke-RestMethod -Method Post -Uri http://127.0.0.1:18088/api/approval/approve -ContentType application/json -Body $_|Out-Null}}}catch{};sleep 1}
Receive-Job $job -Wait -AutoRemoveJob -ErrorAction SilentlyContinue|Out-Null;docker logs $w 2>&1|Set-Content "$r\qwenpaw.log";[pscustomobject]@{output=(Get-Content $env:M3C_OUTPUT -Raw -EA SilentlyContinue);events=(Get-Content $env:M3C_EVENTS -Raw -EA SilentlyContinue);workerLog="$r\qwenpaw.log"}|ConvertTo-Json -Depth 4|Set-Content "$r\result.json";Clean
} finally { Clean }
