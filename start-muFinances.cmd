@echo off
setlocal
cd /d C:\muFinances
powershell -NoProfile -ExecutionPolicy Bypass -Command "$existing = Get-NetTCPConnection -LocalPort 3200 -State Listen -ErrorAction SilentlyContinue; if (-not $existing) { Start-Process -FilePath python -ArgumentList @('-m','uvicorn','app.main:app','--host','0.0.0.0','--port','3200') -WorkingDirectory 'C:\muFinances' -WindowStyle Hidden; Start-Sleep -Seconds 2 }; Start-Process 'http://localhost:3200/?logout=1&v=login'"
endlocal
