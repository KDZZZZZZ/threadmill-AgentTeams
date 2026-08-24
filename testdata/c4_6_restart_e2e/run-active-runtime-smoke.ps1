# C4-6 active crash-point fixture. The established M4 embedded bootstrap owns
# Docker topology and cleanup; this wrapper selects its Runtime-A crash mode.
$ErrorActionPreference = 'Stop'
$fixture = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..\m4_d_rehydration_e2e')).Path 'run.ps1'
if (-not (Test-Path $fixture)) { throw 'M4 embedded fixture bootstrap is missing' }
& $fixture -ActiveRuntimeSmoke
if ($LASTEXITCODE -ne 0) { throw 'C4-6 active Runtime crash-point fixture failed' }
