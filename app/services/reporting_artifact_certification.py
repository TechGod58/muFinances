from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db
from app.services.financial_statement_accuracy_certification import run_accuracy_certification
from app.services.reporting import (
    list_export_artifact_validations,
    reporting_output_completion_status,
    run_reporting_output_completion,
)
from app.services.reporting_pixel_polish_certification import run_certification as run_pixel_polish_certification
from app.services.reporting_pixel_polish_certification import status as pixel_polish_status


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS reporting_artifact_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                output_completion_json TEXT NOT NULL,
                pixel_polish_json TEXT NOT NULL,
                accuracy_json TEXT NOT NULL,
                artifact_manifest_json TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                signoff_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_reporting_artifact_certification_runs_created
            ON reporting_artifact_certification_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    output = reporting_output_completion_status()
    pixel = pixel_polish_status()
    checks = {
        'pdf_artifact_certification_ready': output['checks']['real_pdf_artifacts_ready'],
        'excel_artifact_certification_ready': output['checks']['real_excel_artifacts_ready'],
        'powerpoint_artifact_certification_ready': output['checks']['real_powerpoint_artifacts_ready'],
        'board_package_artifact_certification_ready': output['checks']['board_artifacts_ready'],
        'pagination_and_footnote_certification_ready': output['checks']['pagination_ready'] and output['checks']['footnotes_ready'],
        'chart_export_certification_ready': output['checks']['embedded_charts_ready'] and pixel['checks']['charts_ready'],
        'retention_and_distribution_certification_ready': output['checks']['retention_ready'] and output['checks']['scheduled_distribution_ready'],
        'visual_regression_certification_ready': output['checks']['visual_regression_tests_ready'],
    }
    counts = {
        'artifact_certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM reporting_artifact_certification_runs')['count']),
        'export_artifacts': output['counts']['pdf_artifacts'] + output['counts']['excel_artifacts'] + output['counts']['powerpoint_artifacts'] + output['counts']['email_artifacts'],
        'pixel_polish_runs': pixel['counts']['pixel_polish_runs'],
        'validations': output['counts']['validations'],
    }
    return {
        'batch': 'B158',
        'title': 'Reporting Artifact Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'output_completion_status': output,
        'pixel_polish_status': pixel,
        'latest_run': _latest_run(),
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all(
        'SELECT * FROM reporting_artifact_certification_runs ORDER BY id DESC LIMIT ?',
        (max(1, min(limit, 500)),),
    )
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b158-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _default_scenario_id())
    output = run_reporting_output_completion(scenario_id, user)
    pixel = run_pixel_polish_certification({'run_key': f'{run_key}-pixel'}, user)
    accuracy = run_accuracy_certification({'run_key': f'{run_key}-accuracy'}, user)
    manifest = _artifact_manifest(output, pixel, accuracy)
    checks = {
        'pdf_artifacts_validated': _type_ready(manifest, 'pdf'),
        'excel_artifacts_validated': _type_ready(manifest, 'excel'),
        'powerpoint_artifacts_validated': _type_ready(manifest, 'pptx'),
        'email_artifacts_validated': _type_ready(manifest, 'email'),
        'chart_artifacts_validated': _type_ready(manifest, 'png') and bool((pixel.get('artifacts') or {}).get('chart_render')),
        'board_package_pagination_validated': all(item['page_count'] >= 1 for item in manifest if item['artifact_type'] in {'pdf', 'pptx', 'excel'}),
        'embedded_charts_validated': any(item['chart_image_embeds'] >= 1 for item in manifest if item['artifact_type'] in {'pdf', 'pptx', 'png'}),
        'footnotes_validated': bool((pixel.get('artifacts') or {}).get('footnotes')),
        'retention_validated': all(item['retention_until'] for item in manifest),
        'downloadable_files_validated': all(item['file_exists'] and item['download_url'] for item in manifest),
        'visual_hashes_validated': all(isinstance(item['visual_hash'], str) and len(item['visual_hash']) == 64 for item in manifest),
        'statement_accuracy_validated': accuracy['status'] == 'passed',
    }
    signoff = _signoff(payload, user, checks)
    status_value = 'passed' if all(checks.values()) and output['complete'] and pixel['complete'] and accuracy['complete'] else 'needs_review'
    completed = _now()
    row_id = db.execute(
        '''
        INSERT INTO reporting_artifact_certification_runs (
            run_key, scenario_id, status, output_completion_json, pixel_polish_json,
            accuracy_json, artifact_manifest_json, checks_json, signoff_json,
            created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            json.dumps(output, sort_keys=True),
            json.dumps(pixel, sort_keys=True),
            json.dumps(accuracy, sort_keys=True),
            json.dumps(manifest, sort_keys=True),
            json.dumps(checks, sort_keys=True),
            json.dumps(signoff, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('reporting_artifact_certification', run_key, status_value, user['email'], {'checks': checks, 'artifact_count': len(manifest)}, completed)
    return get_run(row_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM reporting_artifact_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Reporting artifact certification run not found.')
    return _format_run(row)


def _artifact_manifest(output: dict[str, Any], pixel: dict[str, Any], accuracy: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = list(output.get('artifacts') or [])
    pixel_artifacts = pixel.get('artifacts') or {}
    accuracy_artifacts = accuracy.get('artifacts') or {}
    for key in ('pdf_artifact', 'png_artifact'):
        artifact = pixel_artifacts.get(key)
        if artifact:
            artifacts.append(artifact)
    board_artifact = accuracy_artifacts.get('board_artifact')
    if board_artifact:
        artifacts.append(board_artifact)

    manifest = []
    for artifact in artifacts:
        metadata = artifact.get('metadata') or {}
        validations = list_export_artifact_validations(artifact_id=int(artifact['id']))
        path = Path(artifact['storage_path'])
        manifest.append(
            {
                'artifact_id': int(artifact['id']),
                'artifact_type': artifact['artifact_type'],
                'file_name': artifact['file_name'],
                'content_type': artifact['content_type'],
                'storage_path': artifact['storage_path'],
                'file_exists': path.exists(),
                'size_bytes': int(artifact.get('size_bytes') or 0),
                'download_url': artifact.get('download_url'),
                'validation_status': validations[0]['status'] if validations else metadata.get('validation_status'),
                'validation_count': len(validations),
                'page_count': int(metadata.get('page_count') or 0),
                'chart_image_embeds': int(metadata.get('chart_image_embeds') or 0),
                'retention_until': metadata.get('retention_until'),
                'visual_hash': metadata.get('visual_hash'),
            }
        )
    return manifest


def _type_ready(manifest: list[dict[str, Any]], artifact_type: str) -> bool:
    return any(
        item['artifact_type'] == artifact_type
        and item['validation_status'] == 'passed'
        and item['file_exists']
        and item['size_bytes'] > 0
        for item in manifest
    )


def _signoff(payload: dict[str, Any], user: dict[str, Any], checks: dict[str, bool]) -> dict[str, Any]:
    return {
        'signed_by': payload.get('signed_by') or user['email'],
        'signed_at': _now(),
        'all_checks_passed': all(checks.values()),
        'notes': payload.get('notes') or 'Reporting artifacts are validated for export, pagination, retention, chart embedding, and statement accuracy.',
    }


def _default_scenario_id() -> int:
    row = db.fetch_one('SELECT id FROM scenarios ORDER BY id DESC LIMIT 1')
    if row is None:
        raise ValueError('No scenario is available for reporting artifact certification.')
    return int(row['id'])


def _latest_run() -> dict[str, Any] | None:
    row = db.fetch_one('SELECT * FROM reporting_artifact_certification_runs ORDER BY id DESC LIMIT 1')
    return _format_run(row) if row else None


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['batch'] = 'B158'
    result['output_completion'] = json.loads(result.pop('output_completion_json') or '{}')
    result['pixel_polish'] = json.loads(result.pop('pixel_polish_json') or '{}')
    result['accuracy'] = json.loads(result.pop('accuracy_json') or '{}')
    result['artifact_manifest'] = json.loads(result.pop('artifact_manifest_json') or '[]')
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['signoff'] = json.loads(result.pop('signoff_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
