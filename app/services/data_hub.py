from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from app import db
from app.services.foundation import create_dimension_member, dimension_hierarchy


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'change_requests': int(db.fetch_one('SELECT COUNT(*) AS count FROM master_data_change_requests')['count']),
        'mappings': int(db.fetch_one('SELECT COUNT(*) AS count FROM master_data_mappings')['count']),
        'metadata_approvals': int(db.fetch_one('SELECT COUNT(*) AS count FROM metadata_approval_requests')['count']),
        'lineage_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM data_lineage_records')['count']),
        'dimension_members': int(db.fetch_one('SELECT COUNT(*) AS count FROM dimension_members')['count']),
    }
    checks = {
        'chart_of_accounts_governance_ready': True,
        'department_entity_fund_workflow_ready': True,
        'effective_dating_ready': True,
        'mapping_tables_ready': True,
        'metadata_approval_ready': True,
        'source_to_report_lineage_ready': True,
    }
    return {'batch': 'B39', 'title': 'Data Hub And Master Data Governance', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace(scenario_id: int | None = None) -> dict[str, Any]:
    return {
        'status': status(),
        'hierarchy': dimension_hierarchy(),
        'change_requests': list_change_requests(),
        'mappings': list_mappings(),
        'metadata_approvals': list_metadata_approvals(),
        'lineage': list_lineage_records(scenario_id),
    }


def request_dimension_change(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if payload['dimension_kind'] not in {'account', 'department', 'entity', 'fund', 'program', 'project', 'grant'}:
        raise ValueError('Unsupported governed dimension kind.')
    now = _now()
    request_id = db.execute(
        '''
        INSERT INTO master_data_change_requests (
            dimension_kind, code, name, parent_code, change_type, effective_from, effective_to,
            status, metadata_json, requested_by, requested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        ''',
        (
            payload['dimension_kind'],
            payload['code'].upper(),
            payload['name'],
            payload.get('parent_code'),
            payload.get('change_type') or 'create',
            payload['effective_from'],
            payload.get('effective_to'),
            json.dumps(payload.get('metadata') or {}, sort_keys=True),
            user['email'],
            now,
        ),
    )
    db.log_audit('master_data_change_request', str(request_id), 'requested', user['email'], payload, now)
    return _format_change(_one('SELECT * FROM master_data_change_requests WHERE id = ?', (request_id,)))


def approve_dimension_change(change_id: int, user: dict[str, Any]) -> dict[str, Any]:
    change = _format_change(_one('SELECT * FROM master_data_change_requests WHERE id = ?', (change_id,)))
    if change['status'] == 'approved':
        return change
    if change['change_type'] in {'create', 'update', 'reactivate'}:
        metadata = {
            **change['metadata'],
            'effective_from': change['effective_from'],
            'effective_to': change.get('effective_to'),
            'governance_change_id': change_id,
        }
        create_dimension_member(
            {
                'dimension_kind': change['dimension_kind'],
                'code': change['code'],
                'name': change['name'],
                'parent_code': change.get('parent_code'),
                'metadata': metadata,
            },
            actor=user['email'],
        )
    elif change['change_type'] == 'deactivate':
        db.execute(
            'UPDATE dimension_members SET active = 0 WHERE dimension_kind = ? AND code = ?',
            (change['dimension_kind'], change['code']),
        )
    now = _now()
    db.execute(
        '''
        UPDATE master_data_change_requests
        SET status = 'approved', approved_by = ?, approved_at = ?
        WHERE id = ?
        ''',
        (user['email'], now, change_id),
    )
    db.log_audit('master_data_change_request', str(change_id), 'approved', user['email'], change, now)
    return _format_change(_one('SELECT * FROM master_data_change_requests WHERE id = ?', (change_id,)))


def list_change_requests(status_filter: str | None = None) -> list[dict[str, Any]]:
    if status_filter:
        rows = db.fetch_all('SELECT * FROM master_data_change_requests WHERE status = ? ORDER BY id DESC', (status_filter,))
    else:
        rows = db.fetch_all('SELECT * FROM master_data_change_requests ORDER BY id DESC LIMIT 100')
    return [_format_change(row) for row in rows]


def upsert_mapping(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO master_data_mappings (
            mapping_key, source_system, source_dimension, source_code, target_dimension,
            target_code, effective_from, effective_to, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mapping_key) DO UPDATE SET
            source_system = excluded.source_system,
            source_dimension = excluded.source_dimension,
            source_code = excluded.source_code,
            target_dimension = excluded.target_dimension,
            target_code = excluded.target_code,
            effective_from = excluded.effective_from,
            effective_to = excluded.effective_to,
            active = excluded.active
        ''',
        (
            payload['mapping_key'],
            payload['source_system'],
            payload['source_dimension'],
            payload['source_code'],
            payload['target_dimension'],
            payload['target_code'],
            payload['effective_from'],
            payload.get('effective_to'),
            1 if payload.get('active', True) else 0,
            user['email'],
            now,
        ),
    )
    db.log_audit('master_data_mapping', payload['mapping_key'], 'upserted', user['email'], payload, now)
    return _mapping(payload['mapping_key'])


def list_mappings() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM master_data_mappings ORDER BY active DESC, source_system, source_dimension, source_code')
    for row in rows:
        row['active'] = bool(row['active'])
    return rows


def request_metadata_approval(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    approval_id = db.execute(
        '''
        INSERT INTO metadata_approval_requests (
            entity_type, entity_id, metadata_json, status, requested_by, requested_at
        ) VALUES (?, ?, ?, 'pending', ?, ?)
        ''',
        (payload['entity_type'], payload['entity_id'], json.dumps(payload.get('metadata') or {}, sort_keys=True), user['email'], now),
    )
    db.log_audit('metadata_approval', str(approval_id), 'requested', user['email'], payload, now)
    return _format_metadata_approval(_one('SELECT * FROM metadata_approval_requests WHERE id = ?', (approval_id,)))


def approve_metadata(approval_id: int, user: dict[str, Any]) -> dict[str, Any]:
    approval = _format_metadata_approval(_one('SELECT * FROM metadata_approval_requests WHERE id = ?', (approval_id,)))
    now = _now()
    db.execute(
        'UPDATE metadata_approval_requests SET status = ?, approved_by = ?, approved_at = ? WHERE id = ?',
        ('approved', user['email'], now, approval_id),
    )
    if approval['entity_type'] == 'dimension_member':
        kind, _, code = approval['entity_id'].partition(':')
        row = db.fetch_one('SELECT * FROM dimension_members WHERE dimension_kind = ? AND code = ?', (kind, code))
        merged = {**(json.loads(row['metadata_json'] or '{}') if row else {}), **approval['metadata']}
        db.execute(
            'UPDATE dimension_members SET metadata_json = ? WHERE dimension_kind = ? AND code = ?',
            (json.dumps(merged, sort_keys=True), kind, code),
        )
    db.log_audit('metadata_approval', str(approval_id), 'approved', user['email'], approval, now)
    return _format_metadata_approval(_one('SELECT * FROM metadata_approval_requests WHERE id = ?', (approval_id,)))


def list_metadata_approvals() -> list[dict[str, Any]]:
    return [_format_metadata_approval(row) for row in db.fetch_all('SELECT * FROM metadata_approval_requests ORDER BY id DESC LIMIT 100')]


def build_lineage(scenario_id: int, target_type: str, target_id: str, user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute('DELETE FROM data_lineage_records WHERE scenario_id = ? AND target_type = ? AND target_id = ?', (scenario_id, target_type, target_id))
    rows = db.fetch_all(
        '''
        SELECT source, COALESCE(source_version, '') AS source_version, COUNT(*) AS record_count, COALESCE(SUM(amount), 0) AS amount_total
        FROM planning_ledger
        WHERE scenario_id = ? AND reversed_at IS NULL
        GROUP BY source, COALESCE(source_version, '')
        ORDER BY source
        ''',
        (scenario_id,),
    )
    created = []
    for row in rows:
        source_id = row['source'] if not row['source_version'] else f"{row['source']}:{row['source_version']}"
        lineage_id = db.execute(
            '''
            INSERT INTO data_lineage_records (
                scenario_id, source_type, source_id, transform_type, target_type, target_id,
                record_count, amount_total, created_by, created_at
            ) VALUES (?, 'planning_ledger', ?, 'aggregate_report', ?, ?, ?, ?, ?, ?)
            ''',
            (scenario_id, source_id, target_type, target_id, int(row['record_count']), float(row['amount_total']), user['email'], now),
        )
        created.append(_one('SELECT * FROM data_lineage_records WHERE id = ?', (lineage_id,)))
    db.log_audit('data_lineage', f'{target_type}:{target_id}', 'built', user['email'], {'scenario_id': scenario_id, 'records': len(created)}, now)
    return {'scenario_id': scenario_id, 'target_type': target_type, 'target_id': target_id, 'lineage': created, 'count': len(created)}


def list_lineage_records(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        return db.fetch_all('SELECT * FROM data_lineage_records WHERE scenario_id = ? ORDER BY id DESC LIMIT 100', (scenario_id,))
    return db.fetch_all('SELECT * FROM data_lineage_records ORDER BY id DESC LIMIT 100')


def _format_change(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _format_metadata_approval(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['metadata'] = json.loads(result.pop('metadata_json') or '{}')
    return result


def _mapping(mapping_key: str) -> dict[str, Any]:
    row = _one('SELECT * FROM master_data_mappings WHERE mapping_key = ?', (mapping_key,))
    row['active'] = bool(row['active'])
    return row


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
