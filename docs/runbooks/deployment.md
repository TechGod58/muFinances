# muFinances Deployment Runbook

## Localhost

Run `start-muFinances.cmd` from `C:\muFinances` or use the desktop shortcut.

## Internal Host

Install dependencies with `pip install -r requirements.txt`, then run:

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 3200
```

## Windows Service

Run PowerShell as Administrator:

```powershell
.\deploy\install-windows-service.ps1
```

## Docker

```powershell
docker compose up -d --build
```
