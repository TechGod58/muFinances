param(
    [string]$ServiceName = "muFinances"
)

$ErrorActionPreference = "Stop"

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Stop-Service -Name $ServiceName -ErrorAction SilentlyContinue
    sc.exe delete $ServiceName | Out-Null
    Write-Host "Removed $ServiceName."
} else {
    Write-Host "$ServiceName is not installed."
}
