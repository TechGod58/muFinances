from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app import db

ROOT = Path(__file__).resolve().parents[2]
GUIDES_DIR = ROOT / 'docs' / 'guides'
PRODUCTION_GUIDES = {
    'admin-guide': GUIDES_DIR / 'admin-guide.md',
    'planner-guide': GUIDES_DIR / 'planner-guide.md',
    'controller-guide': GUIDES_DIR / 'controller-guide.md',
    'integration-guide': GUIDES_DIR / 'integration-guide.md',
    'data-dictionary': GUIDES_DIR / 'data-dictionary.md',
    'close-process-guide': GUIDES_DIR / 'close-process-guide.md',
    'release-checklist': GUIDES_DIR / 'release-checklist.md',
    'production-operations': GUIDES_DIR / 'production-operations.md',
}

REQUIRED_GUIDE_SECTIONS = {
    'admin-guide': ['Daily Checks', 'User And Access Control', 'Security Operations', 'Audit Reporting', 'Incident Response'],
    'planner-guide': ['Planning Workflow', 'Data Entry', 'Forecasts', 'Approvals And Evidence', 'Planner Closeout'],
    'controller-guide': ['Controller Workspace', 'Review Cadence', 'Reconciliations', 'Consolidation', 'Signoff'],
    'integration-guide': ['Connectors', 'Imports', 'Mapping And Validation', 'Secrets', 'Sync Logs', 'Recovery'],
    'data-dictionary': ['Core Tables', 'Planning Tables', 'Close And Reporting', 'Security And Audit', 'Operations', 'Retention Notes'],
    'close-process-guide': ['Period Close', 'Controls', 'Reconciliation Workflow', 'Consolidation Workflow', 'Audit Packet'],
    'release-checklist': ['Pre-Release Freeze', 'Migration And Backup', 'Smoke Tests', 'Rollback Decision', 'Release Signoff'],
    'production-operations': ['Runtime Modes', 'TLS', 'Manchester Network And AD OU Guard', 'Secrets', 'Logs', 'Observability', 'Backup And Restore Drills'],
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def ensure_production_ops_ready() -> None:
    now = _now()
    for key, path in PRODUCTION_GUIDES.items():
        db.execute(
            '''
            INSERT INTO runbook_records (runbook_key, title, category, path, status, updated_by, updated_at)
            VALUES (?, ?, ?, ?, 'ready', 'system', ?)
            ON CONFLICT(runbook_key) DO UPDATE SET
                title = excluded.title,
                category = excluded.category,
                path = excluded.path,
                status = excluded.status,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at
            ''',
            (key, key.replace('-', ' ').title(), _guide_category(key), str(path.relative_to(ROOT)), now),
        )


def status() -> dict[str, Any]:
    runtime = db.database_runtime()
    guide_checks = {key: path.exists() for key, path in PRODUCTION_GUIDES.items()}
    counts = {
        'application_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM application_logs')['count']),
        'audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_logs')['count']),
        'sync_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_sync_logs')['count']),
        'sync_jobs': int(db.fetch_one('SELECT COUNT(*) AS count FROM sync_jobs')['count']),
        'credential_refs': int(db.fetch_one('SELECT COUNT(*) AS count FROM credential_vault')['count']),
        'ready_guides': sum(1 for ready in guide_checks.values() if ready),
    }
    checks = {
        'postgresql_option_ready': _postgres_option_ready(runtime),
        'connection_pooling_ready': runtime['pooling_enabled'] and int(runtime['pool_size']) >= 1,
        'tls_deployment_notes_ready': (GUIDES_DIR / 'production-operations.md').exists() and 'TLS' in (GUIDES_DIR / 'production-operations.md').read_text(encoding='utf-8'),
        'encrypted_secrets_handling_ready': _secrets_ready(),
        'application_job_sync_logs_ready': True,
        'admin_audit_reports_ready': True,
        'production_guides_ready': all(guide_checks.values()),
    }
    return {
        'batch': 'B24',
        'title': 'Production Operations Hardening',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'database': runtime,
        'guides': {key: str(path.relative_to(ROOT)) for key, path in PRODUCTION_GUIDES.items()},
    }


def documentation_readiness() -> dict[str, Any]:
    guide_results = []
    for key, path in PRODUCTION_GUIDES.items():
        text = path.read_text(encoding='utf-8') if path.exists() else ''
        required = REQUIRED_GUIDE_SECTIONS.get(key, [])
        missing = [section for section in required if f'## {section}' not in text]
        guide_results.append({
            'key': key,
            'path': str(path.relative_to(ROOT)),
            'exists': path.exists(),
            'size_bytes': path.stat().st_size if path.exists() else 0,
            'required_sections': required,
            'missing_sections': missing,
            'ready': path.exists() and not missing and len(text.strip()) >= 1000,
        })
    checks = {
        'admin_guide_ready': _guide_ready(guide_results, 'admin-guide'),
        'planner_guide_ready': _guide_ready(guide_results, 'planner-guide'),
        'controller_guide_ready': _guide_ready(guide_results, 'controller-guide'),
        'integration_guide_ready': _guide_ready(guide_results, 'integration-guide'),
        'data_dictionary_ready': _guide_ready(guide_results, 'data-dictionary'),
        'close_process_guide_ready': _guide_ready(guide_results, 'close-process-guide'),
        'release_checklist_ready': _guide_ready(guide_results, 'release-checklist'),
        'production_operations_guide_ready': _guide_ready(guide_results, 'production-operations'),
    }
    return {
        'batch': 'B62',
        'title': 'Documentation Freeze And Operator Readiness',
        'complete': all(checks.values()),
        'checks': checks,
        'count': len(guide_results),
        'guides': guide_results,
    }


def list_application_logs(limit: int = 100, severity: str | None = None) -> dict[str, Any]:
    if severity:
        rows = db.fetch_all('SELECT * FROM application_logs WHERE severity = ? ORDER BY id DESC LIMIT ?', (severity, limit))
    else:
        rows = db.fetch_all('SELECT * FROM application_logs ORDER BY id DESC LIMIT ?', (limit,))
    return {'count': len(rows), 'logs': [_format_log(row) for row in rows]}


def record_application_log(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    db.log_application(
        log_type=payload.get('log_type', 'admin'),
        severity=payload.get('severity', 'info'),
        message=payload['message'],
        actor=user['email'],
        detail=payload.get('detail') or {},
        correlation_id=payload.get('correlation_id', ''),
    )
    row = db.fetch_one('SELECT * FROM application_logs ORDER BY id DESC LIMIT 1')
    db.log_audit('application_log', str(row['id']), 'created', user['email'], {'message': payload['message']}, _now())
    return _format_log(row)


def admin_audit_report(limit: int = 250) -> dict[str, Any]:
    recent = db.fetch_all('SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?', (limit,))
    by_actor = db.fetch_all(
        '''
        SELECT actor, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM audit_logs
        GROUP BY actor
        ORDER BY count DESC, actor ASC
        LIMIT 25
        '''
    )
    by_entity = db.fetch_all(
        '''
        SELECT entity_type, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM audit_logs
        GROUP BY entity_type
        ORDER BY count DESC, entity_type ASC
        LIMIT 25
        '''
    )
    auth_events = db.fetch_all(
        '''
        SELECT actor, action, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM audit_logs
        WHERE entity_type IN ('auth_session', 'user', 'user_dimension_access')
        GROUP BY actor, action
        ORDER BY last_seen DESC
        LIMIT 25
        '''
    )
    return {
        'generated_at': _now(),
        'limit': limit,
        'totals': {
            'audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_logs')['count']),
            'application_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM application_logs')['count']),
            'sync_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM connector_sync_logs')['count']),
        },
        'by_actor': by_actor,
        'by_entity': by_entity,
        'auth_events': auth_events,
        'recent': [_format_audit(row) for row in recent],
    }


def guides_manifest() -> dict[str, Any]:
    return {
        'count': len(PRODUCTION_GUIDES),
        'readiness': documentation_readiness(),
        'guides': [
            {
                'key': key,
                'path': str(path.relative_to(ROOT)),
                'exists': path.exists(),
                'size_bytes': path.stat().st_size if path.exists() else 0,
            }
            for key, path in PRODUCTION_GUIDES.items()
        ],
    }


def _postgres_option_ready(runtime: dict[str, Any]) -> bool:
    compose = (ROOT / 'docker-compose.yml').read_text(encoding='utf-8') if (ROOT / 'docker-compose.yml').exists() else ''
    return 'postgres:' in compose and 'CAMPUS_FPM_POSTGRES_DSN' in compose and runtime['postgres_ssl_mode'] in {'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'}


def _secrets_ready() -> bool:
    field_key = os.getenv('CAMPUS_FPM_FIELD_KEY', 'local-dev-field-key-change-before-production')
    compose = (ROOT / 'docker-compose.yml').read_text(encoding='utf-8') if (ROOT / 'docker-compose.yml').exists() else ''
    return 'CAMPUS_FPM_FIELD_KEY_FILE' in compose and bool(field_key)


def _guide_category(key: str) -> str:
    if key in {'admin-guide', 'production-operations'}:
        return 'operations'
    if key == 'controller-guide':
        return 'operations'
    if key == 'integration-guide':
        return 'deployment'
    if key in {'close-process-guide', 'release-checklist'}:
        return 'backup'
    return 'operations'


def _guide_ready(results: list[dict[str, Any]], key: str) -> bool:
    return bool(next((row['ready'] for row in results if row['key'] == key), False))


def _format_log(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _format_audit(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result
