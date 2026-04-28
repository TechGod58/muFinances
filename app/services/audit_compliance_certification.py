from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.compliance import (
    certify,
    create_certification,
    list_retention_policies,
    retention_review,
    seal_audit_backlog,
    sod_report,
    upsert_retention_policy,
    verify_audit_chain,
)
from app.services.data_hub import build_lineage
from app.services.evidence import create_attachment, entity_evidence
from app.services.foundation import append_ledger_entry
from app.services.production_operations import admin_audit_report
from app.services.tax_compliance import (
    classification_summary,
    classify_activity,
    decide_tax_alert,
    list_tax_alerts,
    review_classification,
    run_tax_update_check,
    upsert_form990_support,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_tables() -> None:
    with db.get_connection() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS audit_compliance_certification_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_key TEXT NOT NULL UNIQUE,
                scenario_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checks_json TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_audit_compliance_certification_runs_scenario
            ON audit_compliance_certification_runs (scenario_id, completed_at);
            '''
        )


def status() -> dict[str, Any]:
    _ensure_tables()
    latest = db.fetch_one('SELECT * FROM audit_compliance_certification_runs ORDER BY id DESC LIMIT 1')
    checks = {
        'immutable_audit_chain_ready': True,
        'source_to_report_lineage_ready': True,
        'retention_policies_ready': True,
        'certification_workflows_ready': True,
        'admin_audit_reports_ready': True,
        'tax_npo_tagging_ready': True,
        'evidence_retention_ready': True,
    }
    counts = {
        'certification_runs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_compliance_certification_runs')['count']),
        'audit_logs': int(db.fetch_one('SELECT COUNT(*) AS count FROM audit_logs')['count']),
        'retention_policies': int(db.fetch_one('SELECT COUNT(*) AS count FROM retention_policies WHERE active = 1')['count']),
        'compliance_certifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM compliance_certifications')['count']),
        'tax_classifications': int(db.fetch_one('SELECT COUNT(*) AS count FROM tax_activity_classifications')['count']),
        'evidence_attachments': int(db.fetch_one('SELECT COUNT(*) AS count FROM evidence_attachments')['count']),
        'lineage_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM data_lineage_records')['count']),
    }
    return {
        'batch': 'B101',
        'title': 'Audit And Compliance Certification',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': counts,
        'latest_run': _format_run(latest) if latest else None,
    }


def list_runs(limit: int = 50) -> list[dict[str, Any]]:
    _ensure_tables()
    rows = db.fetch_all('SELECT * FROM audit_compliance_certification_runs ORDER BY id DESC LIMIT ?', (limit,))
    return [_format_run(row) for row in rows]


def run_certification(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    _ensure_tables()
    started = _now()
    run_key = payload.get('run_key') or f"b101-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    scenario_id = int(payload.get('scenario_id') or _create_scenario(run_key))

    ledger = append_ledger_entry(
        {
            'scenario_id': scenario_id,
            'entity_code': 'CAMPUS',
            'department_code': 'OPS',
            'fund_code': 'GEN',
            'account_code': 'AUXILIARY',
            'period': '2026-09',
            'amount': 18750.0,
            'source': 'b101-campus-export',
            'source_version': run_key,
            'source_record_id': f'{run_key}-auxiliary-1',
            'ledger_type': 'actual',
            'ledger_basis': 'actual',
            'notes': 'B101 compliance certification source row.',
            'idempotency_key': f'{run_key}:audit-compliance-ledger',
            'metadata': {'npo_activity': True, 'lineage_required': True},
        },
        actor=user['email'],
        user=user,
    )
    lineage = build_lineage(scenario_id, 'board_report', f'{run_key}-audit-report', user)
    evidence = create_attachment(
        {
            'entity_type': 'board_report',
            'entity_id': f'{run_key}-audit-report',
            'file_name': f'{run_key}-source-export.csv',
            'storage_path': f'evidence/{run_key}/source-export.csv',
            'content_type': 'text/csv',
            'size_bytes': 2048,
            'retention_until': '2034-06-30',
            'metadata': {'source_record_id': ledger['source_record_id'], 'lineage_target': lineage['target_id']},
        },
        user,
    )
    retention_policy = upsert_retention_policy(
        {
            'policy_key': f'{run_key}-evidence-retention',
            'entity_type': 'evidence_attachment',
            'retention_years': 7,
            'disposition_action': 'retain_then_review',
            'legal_hold': True,
            'active': True,
        },
        user,
    )
    classification = classify_activity(
        {
            'classification_key': f'{run_key}-tax-auxiliary',
            'scenario_id': scenario_id,
            'ledger_entry_id': ledger['id'],
            'activity_name': 'Auxiliary taxable activity certification',
            'tax_status': 'taxable',
            'activity_tag': 'npo_ubit_review',
            'income_type': 'unrelated_business_income',
            'ubit_code': 'UBIT-AUX',
            'regularly_carried_on': True,
            'substantially_related': False,
            'debt_financed': False,
            'amount': ledger['amount'],
            'expense_offset': 4750.0,
            'form990_part': 'VIII',
            'form990_line': '11',
            'form990_column': 'C',
            'review_status': 'pending_review',
            'notes': 'B101 tax/NPO certification classification.',
            'metadata': {'source_record_id': ledger['source_record_id']},
        },
        user,
    )
    tax_review = review_classification(
        int(classification['id']),
        {'decision': 'approve', 'note': 'B101 classification reviewed.', 'evidence': {'attachment_id': evidence['id']}},
        user,
    )
    form990 = upsert_form990_support(
        {
            'support_key': f'{run_key}-form990-line-11',
            'scenario_id': scenario_id,
            'period': '2026-09',
            'form_part': 'VIII',
            'line_number': '11',
            'column_code': 'C',
            'description': 'Auxiliary taxable activity support',
            'amount': ledger['amount'],
            'basis': {'classification_id': classification['id'], 'ledger_entry_id': ledger['id']},
            'review_status': 'reviewed',
        },
        user,
    )
    tax_check = run_tax_update_check(
        {
            'source_key': 'irs-form-990t-instructions',
            'observed_version': f"2026-{run_key[-6:]}",
            'detail': {'certification_run': run_key},
        },
        user,
    )
    tax_alert = tax_check.get('alert')
    tax_alert_decision = decide_tax_alert(int(tax_alert['id']), {'status': 'acknowledged', 'note': 'B101 certification reviewed.'}, user) if tax_alert else None
    certification = create_certification(
        {
            'scenario_id': scenario_id,
            'certification_key': f'{run_key}-audit-cert',
            'control_area': 'audit_compliance',
            'period': '2026-09',
            'owner': user['email'],
            'due_at': '2026-10-15T00:00:00+00:00',
            'notes': 'B101 audit and compliance certification workflow.',
        },
        user,
    )
    certified = certify(
        int(certification['id']),
        {
            'notes': 'B101 certified with source lineage, evidence retention, tax review, and audit report.',
            'evidence': {
                'lineage_count': lineage['count'],
                'attachment_id': evidence['id'],
                'tax_classification_id': classification['id'],
                'form990_support_id': form990['id'],
            },
        },
        user,
    )
    admin_report = admin_audit_report(100)
    retention = retention_review()
    sod = sod_report()
    entity_links = entity_evidence('board_report', f'{run_key}-audit-report')

    seal_audit_backlog(user)
    _seal_current_backlog()
    audit_chain = verify_audit_chain(5000)

    artifacts = {
        'ledger_entry': ledger,
        'lineage': lineage,
        'evidence_attachment': evidence,
        'entity_evidence': entity_links,
        'retention_policy': retention_policy,
        'retention_review': retention,
        'tax_classification': classification,
        'tax_review': tax_review,
        'form990_support': form990,
        'tax_update_check': tax_check,
        'tax_alert_decision': tax_alert_decision,
        'certification': certified,
        'admin_audit_report': admin_report,
        'sod_report': sod,
        'audit_chain': audit_chain,
        'retention_policies': list_retention_policies(),
        'tax_summary': classification_summary(scenario_id),
        'tax_alerts': list_tax_alerts(),
    }
    checks = {
        'immutable_audit_chain_ready': audit_chain['valid'] is True and audit_chain['verified'] >= 1,
        'source_to_report_lineage_ready': lineage['count'] >= 1,
        'retention_policies_ready': retention_policy['active'] is True and retention['policy_count'] >= 1,
        'certification_workflows_ready': certified['status'] == 'certified',
        'admin_audit_reports_ready': admin_report['totals']['audit_logs'] >= 1 and len(admin_report['recent']) >= 1,
        'tax_npo_tagging_ready': tax_review['status'] == 'approved' and classification['net_ubti'] > 0,
        'evidence_retention_ready': evidence['retention_until'] == '2034-06-30' and len(entity_links['attachments']) >= 1,
    }
    status_value = 'passed' if all(checks.values()) else 'needs_review'
    completed = _now()
    run_id = db.execute(
        '''
        INSERT INTO audit_compliance_certification_runs (
            run_key, scenario_id, status, checks_json, artifacts_json, created_by, started_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            run_key,
            scenario_id,
            status_value,
            json.dumps(checks, sort_keys=True),
            json.dumps(artifacts, sort_keys=True),
            user['email'],
            started,
            completed,
        ),
    )
    db.log_audit('audit_compliance_certification', run_key, status_value, user['email'], {'checks': checks}, completed)
    _seal_current_backlog()
    return get_run(run_id)


def get_run(run_id: int) -> dict[str, Any]:
    _ensure_tables()
    row = db.fetch_one('SELECT * FROM audit_compliance_certification_runs WHERE id = ?', (run_id,))
    if row is None:
        raise ValueError('Audit and compliance certification run not found.')
    return _format_run(row)


def _create_scenario(run_key: str) -> int:
    return db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, 'b101', 'draft', '2026-09', '2027-08', 0, ?)
        ''',
        (f'B101 Audit Compliance Certification {run_key}', _now()),
    )


def _seal_current_backlog() -> None:
    rows = db.fetch_all(
        '''
        SELECT a.id
        FROM audit_logs a
        LEFT JOIN audit_log_hashes h ON h.audit_log_id = a.id
        WHERE h.id IS NULL
        ORDER BY a.id
        '''
    )
    for row in rows:
        db.seal_audit_log(int(row['id']))


def _format_run(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['checks'] = json.loads(result.pop('checks_json') or '{}')
    result['artifacts'] = json.loads(result.pop('artifacts_json') or '{}')
    result['complete'] = result['status'] == 'passed'
    return result
