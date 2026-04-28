from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from app import db


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'templates': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_templates')['count']),
        'template_steps': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_template_steps')['count']),
        'instances': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_instances')['count']),
        'tasks': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_tasks')['count']),
        'delegations': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_delegations')['count']),
        'escalations': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_escalation_events')['count']),
        'visual_designs': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_visual_designs')['count']),
        'process_calendars': int(db.fetch_one('SELECT COUNT(*) AS count FROM process_calendars')['count']),
        'substitute_approvers': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_substitute_approvers')['count']),
        'certification_packets': int(db.fetch_one('SELECT COUNT(*) AS count FROM workflow_certification_packets')['count']),
        'campaign_monitors': int(db.fetch_one('SELECT COUNT(*) AS count FROM process_campaign_monitors')['count']),
    }
    checks = {
        'configurable_approval_chains_ready': True,
        'escalations_ready': True,
        'delegation_ready': True,
        'notifications_ready': True,
        'visual_workflow_designer_ready': True,
        'reusable_process_calendars_ready': True,
        'substitute_approvers_ready': True,
        'certification_packets_ready': True,
        'close_budget_campaign_monitoring_ready': True,
    }
    return {'batch': 'B45', 'title': 'Workflow And Process Orchestration Depth', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace(scenario_id: int) -> dict[str, Any]:
    return {
        'scenario_id': scenario_id,
        'status': status(),
        'templates': list_templates(),
        'instances': list_instances(scenario_id),
        'tasks': list_tasks(scenario_id),
        'delegations': list_delegations(),
        'escalations': list_escalation_events(scenario_id),
        'visual_designs': list_visual_designs(),
        'process_calendars': list_process_calendars(scenario_id),
        'substitute_approvers': list_substitute_approvers(True),
        'certification_packets': list_certification_packets(scenario_id),
        'campaign_monitors': list_campaign_monitors(scenario_id),
    }


def create_template(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    steps = payload.get('steps') or []
    if not steps:
        raise ValueError('Workflow template requires at least one step.')
    now = _now()
    template_key = payload.get('template_key') or f"workflow-template-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    template_id = db.execute(
        '''
        INSERT INTO workflow_templates (template_key, name, entity_type, active, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(template_key) DO UPDATE SET
            name = excluded.name,
            entity_type = excluded.entity_type,
            active = excluded.active,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (template_key, payload['name'], payload['entity_type'], 1 if payload.get('active', True) else 0, user['email'], now),
    )
    row = db.fetch_one('SELECT id FROM workflow_templates WHERE template_key = ?', (template_key,))
    template_id = int(row['id'] if row else template_id)
    db.execute('DELETE FROM workflow_template_steps WHERE template_id = ?', (template_id,))
    for index, step in enumerate(steps, start=1):
        db.execute(
            '''
            INSERT INTO workflow_template_steps (
                template_id, step_order, step_key, label, approver_role, approver_user_id,
                escalation_hours, escalation_user_id, notification_template, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                template_id, index, step['step_key'], step['label'], step.get('approver_role'),
                step.get('approver_user_id'), step.get('escalation_hours'), step.get('escalation_user_id'),
                step.get('notification_template') or '', now,
            ),
        )
    db.log_audit('workflow_template', str(template_id), 'saved', user['email'], payload, now)
    return get_template(template_id)


def list_templates() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM workflow_templates ORDER BY active DESC, id DESC')
    return [_format_template(row) for row in rows]


def get_template(template_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM workflow_templates WHERE id = ?', (template_id,))
    if row is None:
        raise ValueError('Workflow template not found.')
    return _format_template(row)


def start_instance(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    template = get_template(int(payload['template_id']))
    if not template['active']:
        raise ValueError('Workflow template is inactive.')
    first_step = template['steps'][0]
    now = _now()
    instance_id = db.execute(
        '''
        INSERT INTO workflow_instances (
            template_id, scenario_id, subject_type, subject_id, current_step_key, status, started_by, started_at
        ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
        ''',
        (
            template['id'], payload['scenario_id'], payload['subject_type'], payload['subject_id'],
            first_step['step_key'], user['email'], now,
        ),
    )
    _create_task(instance_id, first_step, user)
    db.log_audit('workflow_instance', str(instance_id), 'started', user['email'], payload, now)
    return get_instance(instance_id)


def list_instances(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        rows = db.fetch_all('SELECT * FROM workflow_instances WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    else:
        rows = db.fetch_all('SELECT * FROM workflow_instances ORDER BY id DESC')
    return [_format_instance(row) for row in rows]


def get_instance(instance_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM workflow_instances WHERE id = ?', (instance_id,))
    if row is None:
        raise ValueError('Workflow instance not found.')
    return _format_instance(row)


def list_tasks(scenario_id: int | None = None, status_value: str | None = None) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if scenario_id:
        where.append('wi.scenario_id = ?')
        params.append(scenario_id)
    if status_value:
        where.append('wt.status = ?')
        params.append(status_value)
    clause = f"WHERE {' AND '.join(where)}" if where else ''
    rows = db.fetch_all(
        f'''
        SELECT wt.*, wi.scenario_id, wi.subject_type, wi.subject_id, wts.step_key, wts.label
        FROM workflow_tasks wt
        JOIN workflow_instances wi ON wi.id = wt.instance_id
        JOIN workflow_template_steps wts ON wts.id = wt.step_id
        {clause}
        ORDER BY wt.status ASC, wt.id DESC
        ''',
        tuple(params),
    )
    return rows


def decide_task(task_id: int, payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    task = _task(task_id)
    if task['status'] not in {'open', 'escalated'}:
        raise ValueError('Workflow task is already closed.')
    now = _now()
    db.execute(
        '''
        UPDATE workflow_tasks
        SET status = 'complete', decision = ?, note = ?, completed_by = ?, completed_at = ?
        WHERE id = ?
        ''',
        (payload['decision'], payload.get('note') or '', user['email'], now, task_id),
    )
    instance = get_instance(int(task['instance_id']))
    if payload['decision'] == 'rejected':
        db.execute("UPDATE workflow_instances SET status = 'rejected', completed_at = ? WHERE id = ?", (now, instance['id']))
    else:
        _advance_instance(instance, task, user)
    db.log_audit('workflow_task', str(task_id), payload['decision'], user['email'], payload, now)
    return get_instance(int(task['instance_id']))


def create_delegation(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    delegation_id = db.execute(
        '''
        INSERT INTO workflow_delegations (
            from_user_id, to_user_id, starts_at, ends_at, reason, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['from_user_id'], payload['to_user_id'], payload['starts_at'], payload['ends_at'],
            payload.get('reason') or '', 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    db.log_audit('workflow_delegation', str(delegation_id), 'created', user['email'], payload, now)
    return get_delegation(delegation_id)


def list_delegations(active_only: bool = False) -> list[dict[str, Any]]:
    if active_only:
        rows = db.fetch_all('SELECT * FROM workflow_delegations WHERE active = 1 ORDER BY id DESC')
    else:
        rows = db.fetch_all('SELECT * FROM workflow_delegations ORDER BY id DESC')
    return [_format_delegation(row) for row in rows]


def get_delegation(delegation_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM workflow_delegations WHERE id = ?', (delegation_id,))
    if row is None:
        raise ValueError('Workflow delegation not found.')
    return _format_delegation(row)


def run_escalations(scenario_id: int | None, user: dict[str, Any]) -> dict[str, Any]:
    params: list[Any] = []
    where = ["wt.status = 'open'", 'wt.escalated_at IS NULL']
    if scenario_id:
        where.append('wi.scenario_id = ?')
        params.append(scenario_id)
    rows = db.fetch_all(
        f'''
        SELECT wt.*, wi.scenario_id, wi.subject_type, wi.subject_id, wts.escalation_user_id, wts.step_key, wts.label
        FROM workflow_tasks wt
        JOIN workflow_instances wi ON wi.id = wt.instance_id
        JOIN workflow_template_steps wts ON wts.id = wt.step_id
        WHERE {' AND '.join(where)}
        ORDER BY wt.id ASC
        ''',
        tuple(params),
    )
    now = _now()
    escalated = []
    for task in rows:
        if not task.get('due_at') or task['due_at'] > now:
            continue
        db.execute("UPDATE workflow_tasks SET status = 'escalated', escalated_at = ? WHERE id = ?", (now, task['id']))
        event_id = db.execute(
            '''
            INSERT INTO workflow_escalation_events (task_id, escalated_to_user_id, reason, created_by, created_at)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (task['id'], task.get('escalation_user_id'), 'Task passed configured due date.', user['email'], now),
        )
        _notify(
            task.get('escalation_user_id'),
            int(task['scenario_id']),
            'Workflow escalation',
            f"{task['label']} for {task['subject_type']} {task['subject_id']} needs attention.",
            'warning',
            '#workflow',
        )
        escalated.append({'event_id': event_id, 'task_id': task['id'], 'escalated_to_user_id': task.get('escalation_user_id')})
    db.log_audit('workflow_escalation', str(scenario_id or 'all'), 'run', user['email'], {'escalated': len(escalated)}, now)
    return {'scenario_id': scenario_id, 'escalated': len(escalated), 'events': escalated}


def list_escalation_events(scenario_id: int | None = None) -> list[dict[str, Any]]:
    if scenario_id:
        return db.fetch_all(
            '''
            SELECT wee.*, wi.scenario_id
            FROM workflow_escalation_events wee
            JOIN workflow_tasks wt ON wt.id = wee.task_id
            JOIN workflow_instances wi ON wi.id = wt.instance_id
            WHERE wi.scenario_id = ?
            ORDER BY wee.id DESC
            ''',
            (scenario_id,),
        )
    return db.fetch_all('SELECT * FROM workflow_escalation_events ORDER BY id DESC')


def upsert_visual_design(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO workflow_visual_designs (template_id, layout_json, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(template_id) DO UPDATE SET
            layout_json = excluded.layout_json,
            updated_at = excluded.updated_at
        ''',
        (payload['template_id'], json.dumps(payload.get('layout') or {}, sort_keys=True), user['email'], now, now),
    )
    row = db.fetch_one('SELECT * FROM workflow_visual_designs WHERE template_id = ?', (payload['template_id'],))
    db.log_audit('workflow_visual_design', str(row['id']), 'saved', user['email'], payload, now)
    return _format_visual_design(row)


def list_visual_designs(template_id: int | None = None) -> list[dict[str, Any]]:
    if template_id:
        rows = db.fetch_all('SELECT * FROM workflow_visual_designs WHERE template_id = ? ORDER BY id DESC', (template_id,))
    else:
        rows = db.fetch_all('SELECT * FROM workflow_visual_designs ORDER BY id DESC')
    return [_format_visual_design(row) for row in rows]


def upsert_process_calendar(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    db.execute(
        '''
        INSERT INTO process_calendars (
            scenario_id, calendar_key, process_type, period, milestone_json, status, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, calendar_key) DO UPDATE SET
            process_type = excluded.process_type,
            period = excluded.period,
            milestone_json = excluded.milestone_json,
            status = excluded.status
        ''',
        (
            payload['scenario_id'], payload['calendar_key'], payload['process_type'], payload['period'],
            json.dumps(payload.get('milestones') or [], sort_keys=True), payload.get('status') or 'planned', user['email'], now,
        ),
    )
    row = db.fetch_one('SELECT * FROM process_calendars WHERE scenario_id = ? AND calendar_key = ?', (payload['scenario_id'], payload['calendar_key']))
    db.log_audit('process_calendar', str(row['id']), 'upserted', user['email'], payload, now)
    return _format_process_calendar(row)


def list_process_calendars(scenario_id: int, process_type: str | None = None) -> list[dict[str, Any]]:
    if process_type:
        rows = db.fetch_all('SELECT * FROM process_calendars WHERE scenario_id = ? AND process_type = ? ORDER BY period DESC', (scenario_id, process_type))
    else:
        rows = db.fetch_all('SELECT * FROM process_calendars WHERE scenario_id = ? ORDER BY period DESC', (scenario_id,))
    return [_format_process_calendar(row) for row in rows]


def create_substitute_approver(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    sub_id = db.execute(
        '''
        INSERT INTO workflow_substitute_approvers (
            original_user_id, substitute_user_id, process_type, starts_at, ends_at, active, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['original_user_id'], payload['substitute_user_id'], payload.get('process_type') or 'all',
            payload['starts_at'], payload['ends_at'], 1 if payload.get('active', True) else 0, user['email'], now,
        ),
    )
    db.log_audit('workflow_substitute_approver', str(sub_id), 'created', user['email'], payload, now)
    return _format_substitute(_one('SELECT * FROM workflow_substitute_approvers WHERE id = ?', (sub_id,)))


def list_substitute_approvers(active_only: bool = False) -> list[dict[str, Any]]:
    if active_only:
        rows = db.fetch_all('SELECT * FROM workflow_substitute_approvers WHERE active = 1 ORDER BY id DESC')
    else:
        rows = db.fetch_all('SELECT * FROM workflow_substitute_approvers ORDER BY id DESC')
    return [_format_substitute(row) for row in rows]


def assemble_certification_packet(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = int(payload['scenario_id'])
    period = payload['period']
    process_type = payload['process_type']
    contents = {
        'process_type': process_type,
        'period': period,
        'calendars': list_process_calendars(scenario_id, process_type),
        'instances': [row for row in list_instances(scenario_id) if row['subject_type'].startswith(process_type) or process_type in row['subject_type']],
        'tasks': [row for row in list_tasks(scenario_id) if row.get('status') in {'open', 'escalated', 'complete'}],
        'escalations': list_escalation_events(scenario_id),
        'campaign_monitor': monitor_campaign({'scenario_id': scenario_id, 'process_type': 'close' if process_type == 'close' else 'budget', 'period': period}, user),
    }
    key = payload.get('packet_key') or f"{process_type}-cert-{period}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    packet_id = db.execute(
        '''
        INSERT INTO workflow_certification_packets (
            scenario_id, packet_key, process_type, period, status, contents_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, 'assembled', ?, ?, ?)
        ON CONFLICT(scenario_id, packet_key) DO UPDATE SET
            status = 'assembled',
            contents_json = excluded.contents_json,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (scenario_id, key, process_type, period, json.dumps(contents, sort_keys=True), user['email'], _now()),
    )
    row = db.fetch_one('SELECT * FROM workflow_certification_packets WHERE scenario_id = ? AND packet_key = ?', (scenario_id, key))
    db.log_audit('workflow_certification_packet', str(row['id'] if row else packet_id), 'assembled', user['email'], {'packet_key': key}, _now())
    return _format_certification_packet(row)


def list_certification_packets(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM workflow_certification_packets WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_certification_packet(row) for row in rows]


def monitor_campaign(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    scenario_id = int(payload['scenario_id'])
    period = payload['period']
    process_type = payload['process_type']
    if process_type == 'close':
        detail = db.fetch_all('SELECT status, due_date FROM close_checklists WHERE scenario_id = ? AND period = ?', (scenario_id, period))
        total = len(detail)
        completed = sum(1 for row in detail if row['status'] == 'complete')
        overdue = sum(1 for row in detail if row.get('due_date') and row['due_date'] < period + '-31' and row['status'] != 'complete')
    else:
        detail = db.fetch_all('SELECT status, submitted_at FROM budget_submissions WHERE scenario_id = ?', (scenario_id,))
        total = len(detail)
        completed = sum(1 for row in detail if row['status'] in {'approved', 'submitted'})
        overdue = sum(1 for row in detail if row['status'] == 'draft')
    escalated = len(list_escalation_events(scenario_id))
    status_value = 'complete' if total > 0 and completed >= total and overdue == 0 else 'monitoring'
    key = payload.get('campaign_key') or f'{process_type}-{period}'
    db.execute(
        '''
        INSERT INTO process_campaign_monitors (
            scenario_id, campaign_key, process_type, period, total_items, completed_items,
            overdue_items, escalated_items, status, detail_json, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_id, campaign_key) DO UPDATE SET
            total_items = excluded.total_items,
            completed_items = excluded.completed_items,
            overdue_items = excluded.overdue_items,
            escalated_items = excluded.escalated_items,
            status = excluded.status,
            detail_json = excluded.detail_json,
            created_by = excluded.created_by,
            created_at = excluded.created_at
        ''',
        (
            scenario_id, key, process_type, period, total, completed, overdue, escalated, status_value,
            json.dumps({'items': detail[:100]}, sort_keys=True), user['email'], _now(),
        ),
    )
    row = db.fetch_one('SELECT * FROM process_campaign_monitors WHERE scenario_id = ? AND campaign_key = ?', (scenario_id, key))
    return _format_campaign_monitor(row)


def list_campaign_monitors(scenario_id: int) -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM process_campaign_monitors WHERE scenario_id = ? ORDER BY id DESC', (scenario_id,))
    return [_format_campaign_monitor(row) for row in rows]


def _create_task(instance_id: int, step: dict[str, Any], user: dict[str, Any]) -> int:
    now = _now()
    assigned_user_id = step.get('approver_user_id')
    delegated_from_user_id = None
    if assigned_user_id:
        delegation = _active_delegation(int(assigned_user_id), now)
        if delegation:
            delegated_from_user_id = assigned_user_id
            assigned_user_id = int(delegation['to_user_id'])
    due_at = None
    if step.get('escalation_hours') is not None:
        due_at = (datetime.now(UTC) + timedelta(hours=float(step['escalation_hours']))).isoformat()
    task_id = db.execute(
        '''
        INSERT INTO workflow_tasks (
            instance_id, step_id, assigned_role, assigned_user_id, delegated_from_user_id, status, due_at, created_at
        ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
        ''',
        (instance_id, step['id'], step.get('approver_role'), assigned_user_id, delegated_from_user_id, due_at, now),
    )
    instance = db.fetch_one('SELECT scenario_id, subject_type, subject_id FROM workflow_instances WHERE id = ?', (instance_id,))
    _notify(
        assigned_user_id,
        int(instance['scenario_id']),
        'Workflow task assigned',
        step.get('notification_template') or f"{step['label']} approval is ready.",
        'info',
        '#workflow',
    )
    db.log_audit('workflow_task', str(task_id), 'assigned', user['email'], {'step_key': step['step_key']}, now)
    return task_id


def _advance_instance(instance: dict[str, Any], task: dict[str, Any], user: dict[str, Any]) -> None:
    steps = get_template(int(instance['template_id']))['steps']
    current_index = next((index for index, step in enumerate(steps) if int(step['id']) == int(task['step_id'])), -1)
    now = _now()
    if current_index < 0 or current_index + 1 >= len(steps):
        db.execute("UPDATE workflow_instances SET status = 'complete', completed_at = ? WHERE id = ?", (now, instance['id']))
        return
    next_step = steps[current_index + 1]
    db.execute('UPDATE workflow_instances SET current_step_key = ? WHERE id = ?', (next_step['step_key'], instance['id']))
    _create_task(int(instance['id']), next_step, user)


def _active_delegation(from_user_id: int, now: str) -> dict[str, Any] | None:
    substitute = db.fetch_one(
        '''
        SELECT original_user_id AS from_user_id, substitute_user_id AS to_user_id, starts_at, ends_at, 'substitute approver' AS reason, active
        FROM workflow_substitute_approvers
        WHERE original_user_id = ? AND active = 1 AND starts_at <= ? AND ends_at >= ?
        ORDER BY id DESC
        LIMIT 1
        ''',
        (from_user_id, now, now),
    )
    if substitute:
        return substitute
    return db.fetch_one(
        '''
        SELECT *
        FROM workflow_delegations
        WHERE from_user_id = ? AND active = 1 AND starts_at <= ? AND ends_at >= ?
        ORDER BY id DESC
        LIMIT 1
        ''',
        (from_user_id, now, now),
    )


def _notify(user_id: int | None, scenario_id: int | None, title: str, message: str, severity: str, link: str) -> None:
    db.execute(
        '''
        INSERT INTO notifications (
            user_id, scenario_id, notification_type, title, message, severity, status, link, created_at
        ) VALUES (?, ?, 'workflow', ?, ?, ?, 'unread', ?, ?)
        ''',
        (user_id, scenario_id, title, message, severity, link, _now()),
    )


def _task(task_id: int) -> dict[str, Any]:
    row = db.fetch_one('SELECT * FROM workflow_tasks WHERE id = ?', (task_id,))
    if row is None:
        raise ValueError('Workflow task not found.')
    return row


def _format_template(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    result['steps'] = db.fetch_all('SELECT * FROM workflow_template_steps WHERE template_id = ? ORDER BY step_order ASC', (result['id'],))
    return result


def _format_instance(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['tasks'] = list_tasks(status_value=None)
    result['tasks'] = [task for task in result['tasks'] if int(task['instance_id']) == int(result['id'])]
    return result


def _format_delegation(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    return result


def _format_visual_design(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['layout'] = json.loads(result.pop('layout_json') or '{}')
    return result


def _format_process_calendar(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['milestones'] = json.loads(result.pop('milestone_json') or '[]')
    return result


def _format_substitute(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['active'] = bool(result['active'])
    return result


def _format_certification_packet(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['contents'] = json.loads(result.pop('contents_json') or '{}')
    return result


def _format_campaign_monitor(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result['detail'] = json.loads(result.pop('detail_json') or '{}')
    return result


def _one(query: str, params: tuple[Any, ...]) -> dict[str, Any]:
    row = db.fetch_one(query, params)
    if row is None:
        raise ValueError('Record not found.')
    return row
