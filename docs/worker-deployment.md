# muFinances Background Worker

The durable worker processes scheduled and queued background jobs outside the
web request path.

## Local Run

```powershell
cd C:\muFinances
python -m app.worker --worker-id local-worker --interval 5
```

## One-Shot Smoke

```powershell
python -m app.worker --once --worker-id smoke-worker
```

## Windows Service Pattern

Use NSSM or the campus service wrapper to run:

```powershell
powershell.exe -ExecutionPolicy Bypass -File C:\muFinances\deploy\mufinances-worker.ps1
```

Set the same environment variables used by the web app, including database,
field key, SSO, and access guard settings.

## Docker Pattern

Run the same application image with command:

```text
python -m app.worker --worker-id docker-worker --interval 5
```

The worker is intentionally separate from Uvicorn so retries, dead letters, and
scheduled work continue without blocking users.
