param([string]$QwenPawURL = $env:THREADMILL_IT_QWENPAW_URL)

$ErrorActionPreference = 'Stop'
if (-not $QwenPawURL) {
  throw 'Set THREADMILL_IT_QWENPAW_URL to an official running QwenPaw app. AgentTeams currently supplies its official fixture as Docker/Linux shell image tests; this script does not create a fake API.'
}
$env:THREADMILL_IT_QWENPAW_URL = $QwenPawURL
go test -count=1 -tags=integration ./internal/agenthost/agentteams -run TestQwenPawMCPInjectorRealAPI -v
