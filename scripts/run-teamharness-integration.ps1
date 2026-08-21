param([string]$ToolsDir = (Join-Path $env:TEMP 'threadmill-agentteams-tools'))

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$mc = Join-Path $ToolsDir 'mc.exe'
$minio = Join-Path $ToolsDir 'minio.exe'
New-Item -ItemType Directory -Force -Path $ToolsDir | Out-Null
if (-not (Test-Path $mc)) { Invoke-WebRequest 'https://dl.min.io/client/mc/release/windows-amd64/mc.exe' -OutFile $mc }
if (-not (Test-Path $minio)) { Invoke-WebRequest 'https://dl.min.io/server/minio/release/windows-amd64/minio.exe' -OutFile $minio }
$data = Join-Path ([IO.Path]::GetTempPath()) ('threadmill-minio-' + [guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $data | Out-Null
# local integration test credential only; not for production
$env:MINIO_ROOT_USER = 'threadmill-it'; $env:MINIO_ROOT_PASSWORD = 'threadmill-it-secret'
$process = Start-Process -FilePath $minio -ArgumentList @('server', $data, '--address', '127.0.0.1:19000', '--console-address', '127.0.0.1:19001') -PassThru -WindowStyle Hidden
try {
  for ($attempt = 0; $attempt -lt 30; $attempt++) { try { Invoke-WebRequest 'http://127.0.0.1:19000/minio/health/ready' -UseBasicParsing | Out-Null; break } catch { Start-Sleep -Milliseconds 500 } }
  if ($attempt -eq 30) { throw 'MinIO did not become ready' }
  & $mc alias set threadmill-it http://127.0.0.1:19000 threadmill-it threadmill-it-secret | Out-Null
  & $mc mb --ignore-existing threadmill-it/threadmill-it | Out-Null
  $env:PATH = "$ToolsDir$([IO.Path]::PathSeparator)$env:PATH"
  $env:THREADMILL_IT_MINIO_ENDPOINT = 'http://127.0.0.1:19000'; $env:THREADMILL_IT_MINIO_ACCESS_KEY = 'threadmill-it'; $env:THREADMILL_IT_MINIO_SECRET_KEY = 'threadmill-it-secret'
  Push-Location $repoRoot; try { go test -count=1 -tags=integration ./internal/artifacts ./internal/agenthost/agentteams -v } finally { Pop-Location }
} finally {
  if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
  Remove-Item -LiteralPath $data -Recurse -Force -ErrorAction SilentlyContinue
}
