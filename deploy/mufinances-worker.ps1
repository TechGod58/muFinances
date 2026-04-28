param(
  [string]$WorkerId = "mufinances-worker",
  [double]$Interval = 5
)

$ErrorActionPreference = "Stop"
Set-Location "C:\muFinances"
python -m app.worker --worker-id $WorkerId --interval $Interval
