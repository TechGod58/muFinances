param(
    [string]$ServiceName = "muFinances",
    [string]$AppPath = "C:\muFinances",
    [int]$Port = 3200
)

$ErrorActionPreference = "Stop"
$python = (Get-Command python).Source
$arguments = "-m uvicorn app.main:app --host 0.0.0.0 --port $Port"
$command = "`"$python`" $arguments"

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Stop-Service -Name $ServiceName -ErrorAction SilentlyContinue
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 2
}

New-Service `
    -Name $ServiceName `
    -BinaryPathName $command `
    -DisplayName "muFinances" `
    -Description "muFinances internal campus planning service" `
    -StartupType Automatic

Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName" -Name AppDirectory -Value $AppPath
Start-Service -Name $ServiceName
Write-Host "Installed and started $ServiceName on port $Port."
