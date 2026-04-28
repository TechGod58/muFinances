$ErrorActionPreference = "Stop"

function Show-Step {
    param([string]$Name)
    Write-Host ""
    Write-Host "== $Name ==" -ForegroundColor Cyan
}

function Resolve-CommandPath {
    param([string]$CommandName)

    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        Write-Host "$CommandName: NOT FOUND" -ForegroundColor Red
        return $null
    }

    Write-Host "$CommandName: $($command.Source)" -ForegroundColor Green
    return $command.Source
}

Show-Step "Windows runtime"
Write-Host "PowerShell: $($PSVersionTable.PSVersion)"
Write-Host "OS: $([System.Environment]::OSVersion.VersionString)"
Write-Host "Machine: $env:COMPUTERNAME"

Show-Step "Required commands"
$python = Resolve-CommandPath "python"
$node = Resolve-CommandPath "node"
$npm = Resolve-CommandPath "npm"
$git = Resolve-CommandPath "git"

Show-Step "Version checks"
if ($python) { & python --version }
if ($node) { & node --version }
if ($npm) { & npm --version }
if ($git) { & git --version }

Show-Step "Python imports"
if ($python) {
    & python - <<'PY'
import importlib.util
for name in ("fastapi", "uvicorn", "pytest", "playwright"):
    print(f"{name}: {'OK' if importlib.util.find_spec(name) else 'MISSING'}")
PY
}

Show-Step "Node modules"
if ($node) {
    & node -e "for (const name of ['playwright']) { try { require.resolve(name); console.log(name + ': OK'); } catch { console.log(name + ': MISSING'); } }"
}

Show-Step "Application smoke commands"
Write-Host "Run after this script succeeds:"
Write-Host "  python -m pytest"
Write-Host "  npx playwright test"
Write-Host "  python -m uvicorn main:app --host 127.0.0.1 --port 3200"

