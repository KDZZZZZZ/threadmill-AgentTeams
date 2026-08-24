# The worker smoke is intentionally delegated to the established embedded
# bootstrap.  The latter owns all ephemeral Controller credentials and its
# finally block removes the Controller container, network and volume.
$ErrorActionPreference = 'Stop'
$fixture = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..\m4_d_rehydration_e2e')).Path 'run.ps1'
if (-not (Test-Path $fixture)) { throw 'M4 embedded fixture bootstrap is missing' }

# Worker creation needs the bootstrap's ephemeral CLI token, so this mode is
# implemented inside that script rather than exporting a token to a parent.
& $fixture -WorkerSmoke
if ($LASTEXITCODE -ne 0) { throw 'C4-6 Worker smoke failed' }
