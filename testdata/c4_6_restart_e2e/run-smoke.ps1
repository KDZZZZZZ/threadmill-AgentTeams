# Local-only C4-6 execution-plane smoke check.  It deliberately delegates
# startup and cleanup to the established M4 embedded fixture instead of
# copying Controller/AppService bootstrap parameters into a second script.
# No Runtime SQLite, Worker lifecycle, recovery, or crash semantics are used.
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..\m4_d_rehydration_e2e')).Path
$fixture = Join-Path $root 'run.ps1'

if (-not (Test-Path $fixture)) { throw 'M4 embedded fixture bootstrap is missing' }

# BootstrapOnly verifies Docker Desktop, both cached images, creates the
# isolated network/volume/container, waits for the controller-issued CLI
# token, calls the real Controller workers endpoint, and runs its finally
# cleanup. It emits no credential values.
& $fixture -BootstrapOnly
if ($LASTEXITCODE -ne 0) { throw 'C4-6 execution-plane smoke bootstrap failed' }
