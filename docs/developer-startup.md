# muFinances Developer Startup

This document is the B70 recovery path for getting a local developer machine back to a known-good state.

## 1. Verify the Windows process runtime

From a normal PowerShell window:

```powershell
Set-Location C:\muFinances
.\scripts\check_toolchain.ps1
```

The script checks:

- PowerShell and OS runtime
- `python`, `node`, `npm`, and `git`
- Python package visibility for `fastapi`, `uvicorn`, `pytest`, and `playwright`
- Node package visibility for Playwright

If the script fails before printing the first section, Windows is failing to create child processes. That must be repaired before automated tests, local scripts, Git commands, or Playwright can be trusted.

## 2. Start the app

```powershell
python -m uvicorn main:app --host 127.0.0.1 --port 3200
```

Open:

```text
http://localhost:3200
```

## 3. Run verification

```powershell
python -m pytest
npx playwright test
```

## 4. B70 completion criteria

B70 is complete only when all of these are true:

- PowerShell can launch child processes.
- Python, Node, npm, and Git resolve from the terminal.
- The app starts on `localhost:3200`.
- Unit tests run.
- Playwright can open and inspect the app.
- This document remains accurate for a new developer.

