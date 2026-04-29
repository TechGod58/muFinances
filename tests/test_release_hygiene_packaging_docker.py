from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / 'scripts' / 'build_release_archive.py'


def _load_packager():
    spec = importlib.util.spec_from_file_location('build_release_archive', SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_b130_gitignore_and_dockerignore_block_runtime_and_secret_material() -> None:
    gitignore = (PROJECT_ROOT / '.gitignore').read_text(encoding='utf-8')
    dockerignore = (PROJECT_ROOT / '.dockerignore').read_text(encoding='utf-8')
    rotate_script = (PROJECT_ROOT / 'scripts' / 'rotate_deploy_secrets.ps1').read_text(encoding='utf-8')

    for pattern in ['*.db', 'data/', 'backups/', 'exports/', 'deploy/secrets/*.txt', '*.pem', '*.key', '*.zip']:
        assert pattern in gitignore
        assert pattern in dockerignore

    assert '!deploy/secrets/*.example' in gitignore
    assert '!deploy/secrets/*.example' in dockerignore
    assert 'RandomNumberGenerator' in rotate_script
    assert 'mufinances_field_key.txt' in rotate_script
    assert 'postgres_password.txt' in rotate_script


def test_b131_release_packager_uses_git_ls_files_and_rejects_prohibited_paths() -> None:
    packager = _load_packager()
    clean_files = [
        'README.md',
        'Dockerfile',
        'docker-compose.yml',
        'requirements.txt',
        'app/main.py',
        'app/db.py',
        'static/index.html',
        'static/app.js',
        'docs/guides/deployment-guide.md',
        'docs/guides/admin-guide.md',
        'docs/guides/security-guide.md',
        'docs/runbooks/deployment.md',
        'schema/postgresql/README.md',
        'migration_proof/runner.py',
        'deploy/health-check.ps1',
        'deploy/secrets/postgres_password.txt.example',
        'deploy/secrets/mufinances_field_key.txt.example',
    ]

    validation = packager.validate_package_files(clean_files)
    assert validation.ok is True

    dirty_validation = packager.validate_package_files(clean_files + ['data/campus_fpm.db', 'deploy/secrets/postgres_password.txt'])
    assert dirty_validation.ok is False
    assert 'data/campus_fpm.db' in dirty_validation.prohibited
    assert 'deploy/secrets/postgres_password.txt' in dirty_validation.prohibited


def test_b131_release_packager_dry_run_validates_current_tracked_source() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), '--dry-run', '--allow-dirty'],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert 'app/main.py' in result.stdout
    assert 'README.md' in result.stdout
    assert 'deploy/secrets/postgres_password.txt' not in result.stdout.splitlines()


def test_b132_docker_runtime_includes_operational_assets_and_excludes_runtime_secrets() -> None:
    dockerfile = (PROJECT_ROOT / 'Dockerfile').read_text(encoding='utf-8')
    dockerignore = (PROJECT_ROOT / '.dockerignore').read_text(encoding='utf-8')

    for copy_line in [
        'COPY app ./app',
        'COPY static ./static',
        'COPY services ./services',
        'COPY docs ./docs',
        'COPY schema ./schema',
        'COPY migration_proof ./migration_proof',
        'COPY deploy ./deploy',
    ]:
        assert copy_line in dockerfile

    assert 'deploy/secrets/*.txt' in dockerignore
    assert '*.db' in dockerignore
    assert 'data/' in dockerignore


def test_b132_optional_docker_build_smoke() -> None:
    if os.environ.get('MUFINANCES_RUN_DOCKER_BUILD') != '1':
        pytest.skip('Set MUFINANCES_RUN_DOCKER_BUILD=1 to run the Docker build smoke test.')
    if shutil.which('docker') is None:
        pytest.skip('Docker CLI is not installed.')

    info = subprocess.run(['docker', 'info'], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False)
    if info.returncode != 0:
        pytest.skip(f'Docker daemon is not available: {info.stderr.strip()}')

    result = subprocess.run(
        ['docker', 'build', '--pull=false', '-t', 'mufinances:b132-build-smoke', '.'],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
