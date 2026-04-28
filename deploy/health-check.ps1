param(
    [string]$BaseUrl = "http://localhost:3200"
)

$ErrorActionPreference = "Stop"
$health = Invoke-RestMethod -Uri "$BaseUrl/api/health" -Method Get
if ($health.status -ne "ok") {
    throw "Health check failed: $($health | ConvertTo-Json -Compress)"
}
Write-Host "muFinances health check passed at $BaseUrl."
