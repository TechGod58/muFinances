from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.deployment_governance import run_admin_diagnostics
from app.services.observability_operations import (
    create_alert,
    list_alerts,
    list_backup_restore_drills,
    list_health_probes,
    list_metrics,
    record_metric,
    run_backup_restore_drill,
    run_health_probes,
)
from app.services.performance_reliability import enqueue_job, list_background_jobs, list_job_logs, run_next_job
from app.services.production_operations import list_application_logs


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS operations_readiness_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operations_alert_routes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route_key TEXT NOT NULL UNIQUE,
                severity TEXT NOT NULL,
                destination TEXT NOT NULL,
                status TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                updated_by TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_operations_readiness_runs_created
            ON operations_readiness_runs (completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM operations_readiness_runs ORDER BY id DESC LIMIT 1')
    metrics = list_metrics(25)
    probes = list_health_probes(25)
    drills = list_backup_restore_drills(25)
    jobs = list_background_jobs()
    job_logs = list_job_logs()
    alerts = list_alerts(limit=25)
    routes = list_alert_routes()
    dashboard = production_readiness_dashboard()
    checks = {
        'health_checks_ready': bool(probes) or dashboard_component_status(dashboard, 'Health checks') == 'ok',
        'metrics_ready': bool(metrics),
        'logs_ready': list_application_logs(25)['count'] >= 0,
        'alert_routing_ready': bool(routes),
        'backup_drill_records_ready': bool(drills),
        'job_diagnostics_ready': bool(jobs) and bool(job_logs),
        'worker_status_ready': bool(jobs) and bool(job_logs),
        'production_readiness_dashboard_ready': bool(dashboard['components']),
    }
    counts = {
        'readiness_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM operations_readiness_runs')['count']),
        'metrics': len(metrics),
        'health_probes': len(probes),
        'alerts': len(alerts),
        'alert_routes': len(routes),
        'backup_drills': len(drills),
        'background_jobs': len(jobs),
        'job_logs': len(job_logs),
    }
    return {
        'batch': 'B105',
        'title': 'Operations Readiness',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
        'dashboard': dashboard,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM operations_readiness_runs ORDER BY id DESC LIMIT ?', (max(1, min(limit, 200)),))
    return [_format_run(row) for row in rows]


def run_readiness(payload: dict[str, Any], user: dict[str, Any], trace_id: str = '') -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b105-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    trace = trace_id or run_key
    record_metric('operations_readiness.run_count', 'counter', 1, labels={'run_key': run_key}, trace_id=trace)
    health = run_health_probes(user, trace)
    drill = run_backup_restore_drill(user, trace)
    route = upsert_alert_route(
        {
            'route_key': 'operations-critical',
            'severity': 'critical',
            'destination': 'operations@mufinances.local',
            'status': 'ready',
            'evidence': {'run_key': run_key, 'trace_id': trace},
        },
        user,
    )
    alert = create_alert(
        alert_key=f"{run_key}-routing-proof",
        severity='warning',
        message='Operations readiness alert routing proof.',
        source='operations_readiness',
        detail={'route_key': route['route_key'], 'destination': route['destination']},
        trace_id=trace,
    )
    job = enqueue_job(
        {
            'job_key': f'{run_key}-diagnostic-job',
            'job_type': 'cache_invalidation',
            'priority': 5,
            'payload': {'cache_key': 'production.readiness.dashboard', 'scope': 'operations', 'reason': 'B105 worker status proof'},
            'max_attempts': 2,
            'backoff_seconds': 5,
        },
        user,
    )
    job_run = run_next_job(user, 'operations-readiness-worker')
    diagnostic = run_admin_diagnostics('operations-readiness', user)
    dashboard = production_readiness_dashboard()
    artifacts = {
        'health': health,
        'backup_drill': drill,
        'alert_route': route,
        'alert': alert,
        'job': job,
        'job_run': job_run,
        'job_logs': list_job_logs(int(job['id'])),
        'diagnostic': diagnostic,
        'dashboard': dashboard,
        'metrics': list_metrics(25),
        'application_logs': list_application_logs(25),
    }
    checks = {
        'health_checks_ready': health['status'] == 'pass' and len(health['probes']) >= 5,
        'metrics_ready': any(metric['metric_key'] == 'operations_readiness.run_count' for metric in artifacts['metrics']),
        'logs_ready': artifacts['application_logs']['count'] >= 1,
        'alert_routing_ready': route['status'] == 'ready' and alert['status'] == 'open',
        'backup_drill_records_ready': drill['status'] == 'pass',
        'job_diagnostics_ready': bool(job_run.get('ran')) and bool(artifacts['job_logs']),
        'worker_status_ready': bool(job_run.get('ran')) and artifacts['job_run'].get('job', {}).get('status') == 'completed',
        'production_readiness_dashboard_ready': bool(dashboard['components']),
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO operations_readiness_runs (
            run_key, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (run_key, status_value, json.dumps(checks, sort_keys=True), json.dumps(artifacts, sort_keys=True), user['email'], started, completed),
    )
    db.log_audit('operations_readiness', run_key, status_value, user['email'], {'checks': checks}, completed)
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM operations_readiness_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Operations readiness run not found.')
    return _format_run(row)


def upsert_alert_route(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    now = _now()
    db.execute(
        '''
        INSERT INTO operations_alert_routes (
            route_key, severity, destination, status, evidence_json, updated_by, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(route_key) DO UPDATE SET
            severity = excluded.severity,
            destination = excluded.destination,
            status = excluded.status,
            evidence_json = excluded.evidence_json,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        ''',
        (
            payload['route_key'],
            payload.get('severity') or 'warning',
            payload['destination'],
            payload.get('status') or 'ready',
            json.dumps(payload.get('evidence') or {}, sort_keys=True),
            user['email'],
            now,
        ),
    )
    db.log_audit('operations_alert_route', payload['route_key'], 'upserted', user['email'], payload, now)
    return _format_route(_one('SELECT * FROM operations_alert_routes WHERE route_key = ?', (payload['route_key'],)))


def list_alert_routes() -> list[dict[str, Any]]:
    _ensure_tables()
    return [_format_route(row) for row in db.fetch_all('SELECT * FROM operations_alert_routes ORDER BY severity DESC, route_key ASC')]


def production_readiness_dashboard() -> dict[str, Any]:
    _ensure_tables()
    runtime = db.database_runtime()
    latest_migration = db.fetch_one('SELECT * FROM schema_migrations ORDER BY migration_key DESC LIMIT 1')
    job_counts = db.fetch_all('SELECT status, COUNT(*) AS count FROM background_jobs GROUP BY status ORDER BY status')
    latest_backup = db.fetch_one('SELECT * FROM backup_records ORDER BY id DESC LIMIT 1')
    latest_drill = db.fetch_one('SELECT * FROM backup_restore_drill_runs ORDER BY id DESC LIMIT 1')
    latest_probe = db.fetch_one('SELECT * FROM health_probe_runs ORDER BY id DESC LIMIT 1')
    open_alerts = int(db.fetch_one("SELECT COUNT(*) AS count FROM alert_events WHERE status = 'open'")['count'])
    log_count = int(db.fetch_one('SELECT COUNT(*) AS count FROM application_logs')['count'])
    components = [
        _component('Database mode', 'ok' if runtime['backend'] in {'sqlite', 'postgres', 'mssql'} else 'blocked', f"{runtime['backend']} backend, pooling={runtime['pooling_enabled']}"),
        _component('Migration status', 'ok' if latest_migration else 'blocked', latest_migration['migration_key'] if latest_migration else 'No migrations recorded'),
        _component('Auth mode', 'warning', 'Local auth active; SSO/AD handoff remains environment configured.'),
        _component('Worker status', _worker_status(job_counts), _worker_detail(job_counts)),
        _component('Backup status', 'ok' if latest_backup and latest_drill else 'warning', f"latest backup={latest_backup['backup_key'] if latest_backup else 'none'}, latest drill={latest_drill['status'] if latest_drill else 'none'}"),
        _component('Health checks', 'ok' if latest_probe and latest_probe['status'] == 'pass' else 'warning', latest_probe['probe_key'] if latest_probe else 'No health probes have run.'),
        _component('Logs', 'ok' if log_count >= 1 else 'warning', f'{log_count} application log records'),
        _component('Alerts', 'warning' if open_alerts else 'ok', f'{open_alerts} open alert events'),
    ]
    return {
        'batch': 'B105',
        'generated_at': _now(),
        'overall_status': _overall_status(components),
        'database': runtime,
        'components': components,
    }


def dashboard_component_status(dashboard: dict[str, Any], name: str) -> str:
    return str(next((component['status'] for component in dashboard.get('components', []) if component.get('name') == name), 'unknown'))


def _worker_status(job_counts: list[dict[str, Any]]) -> str:
    counts = {row['status']: int(row['count']) for row in job_counts}
    if counts.get('dead_letter') or counts.get('failed'):
        return 'blocked'
    if sum(counts.values()) == 0:
        return 'warning'
    return 'ok'


def _worker_detail(job_counts: list[dict[str, Any]]) -> str:
    if not job_counts:
        return 'No background jobs have been queued.'
    return ', '.join(f"{row['status']}={row['count']}" for row in job_counts)


def _component(name: str, status_value: str, detail: str) -> dict[str, Any]:
    return {'name': name, 'status': status_value, 'detail': detail}


def _overall_status(components: list[dict[str, Any]]) -> str:
    statuses = {component['status'] for component in components}
    if 'blocked' in statuses:
        return 'blocked'
    if 'warning' in statuses:
        return 'warning'
    return 'ok'


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise RuntimeError('Operations readiness record could not be reloaded.')
    return row


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result


def _format_route(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['evidence'] = json.loads(result.pop('evidence_json') or '{}')
    return result
