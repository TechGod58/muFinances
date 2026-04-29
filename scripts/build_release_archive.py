from __future__ import annotations

import argparse
import fnmatch
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


DEFAULT_OUTPUT = Path('dist') / 'mufinances-release.zip'

PROHIBITED_PATTERNS = (
    '.git/*',
    '__pycache__/*',
    '*/__pycache__/*',
    '.pytest_cache/*',
    '*.pyc',
    '*.pyo',
    '*.db',
    '*.sqlite',
    '*.sqlite3',
    '*.db-shm',
    '*.db-wal',
    'data/*',
    'logs/*',
    'backups/*',
    'exports/*',
    'generated_exports/*',
    'deploy/secrets/*.txt',
    '*.pem',
    '*.pfx',
    '*.key',
    '*.crt',
    '*.token',
    '*.bak',
    '*.backup',
    '*.dump',
    '*.zip',
    '.tmp_*_review/*',
    'node_modules/*',
    'playwright-report/*',
    'test-results/*',
)

REQUIRED_PATHS = (
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
)


@dataclass(frozen=True)
class PackageValidation:
    files: list[str]
    missing_required: list[str]
    prohibited: list[str]

    @property
    def ok(self) -> bool:
        return not self.missing_required and not self.prohibited


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Build a clean muFinances handoff ZIP from tracked source files.')
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT, help='Release ZIP path.')
    parser.add_argument('--dry-run', action='store_true', help='Validate and print the file list without writing a ZIP.')
    parser.add_argument('--allow-dirty', action='store_true', help='Do not fail when tracked files have uncommitted changes.')
    args = parser.parse_args(argv)

    root = repo_root()
    tracked = git_tracked_files(root)
    validation = validate_package_files(tracked)
    if not validation.ok:
        print_validation_errors(validation)
        return 2

    if not args.allow_dirty and has_tracked_changes(root):
        print('Release archive blocked: tracked files have uncommitted changes. Commit or pass --allow-dirty.', file=sys.stderr)
        return 3

    if args.dry_run:
        for path in validation.files:
            print(path)
        return 0

    output = args.output
    if not output.is_absolute():
        output = root / output
    build_archive(root, validation.files, output)
    verify_archive(output)
    print(f'Created {output} with {len(validation.files)} tracked files.')
    return 0


def repo_root() -> Path:
    result = subprocess.run(
        ['git', 'rev-parse', '--show-toplevel'],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(result.stdout.strip()).resolve()


def git_tracked_files(root: Path) -> list[str]:
    result = subprocess.run(
        ['git', 'ls-files', '-z'],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted(path.replace('\\', '/') for path in result.stdout.decode('utf-8').split('\0') if path)


def has_tracked_changes(root: Path) -> bool:
    result = subprocess.run(
        ['git', 'status', '--porcelain', '--untracked-files=no'],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def validate_package_files(files: list[str]) -> PackageValidation:
    normalized = sorted(path.replace('\\', '/').lstrip('./') for path in files)
    prohibited = [path for path in normalized if is_prohibited(path)]
    missing_required = [path for path in REQUIRED_PATHS if path not in normalized]
    return PackageValidation(files=normalized, missing_required=missing_required, prohibited=prohibited)


def is_prohibited(path: str) -> bool:
    normalized = path.replace('\\', '/').lstrip('./')
    return any(fnmatch.fnmatchcase(normalized, pattern) for pattern in PROHIBITED_PATTERNS)


def print_validation_errors(validation: PackageValidation) -> None:
    if validation.missing_required:
        print('Release archive missing required deployment files:', file=sys.stderr)
        for path in validation.missing_required:
            print(f'  - {path}', file=sys.stderr)
    if validation.prohibited:
        print('Release archive contains prohibited runtime/secrets/cache material:', file=sys.stderr)
        for path in validation.prohibited:
            print(f'  - {path}', file=sys.stderr)


def build_archive(root: Path, files: list[str], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, 'w', compression=ZIP_DEFLATED) as archive:
        for path in files:
            source = root / path
            if not source.is_file():
                raise FileNotFoundError(f'Tracked file is missing from working tree: {path}')
            archive.write(source, path)


def verify_archive(output: Path) -> None:
    with ZipFile(output, 'r') as archive:
        names = sorted(archive.namelist())
    validation = validate_package_files(names)
    if not validation.ok:
        print_validation_errors(validation)
        raise RuntimeError('Release archive validation failed after ZIP creation.')


if __name__ == '__main__':
    raise SystemExit(main())
