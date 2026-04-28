from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app import db


ROLE_CHECKLISTS = {
    'admin': {
        'checklist_key': 'admin-launch',
        'title': 'Admin launch checklist',
        'role_key': 'admin',
        'tasks': [
            {'task_key': 'verify-sso', 'title': 'Verify SSO and AD/OU access rules', 'target': '#enterprise-security'},
            {'task_key': 'seed-indexes', 'title': 'Seed performance index recommendations', 'target': '#performance-reliability'},
            {'task_key': 'run-backup', 'title': 'Create a backup and run restore automation', 'target': '#operations'},
            {'task_key': 'review-audit', 'title': 'Verify audit chain and SoD policies', 'target': '#compliance'},
        ],
    },
    'planner': {
        'checklist_key': 'planner-budget-cycle',
        'title': 'Department planner budget cycle',
        'role_key': 'planner',
        'tasks': [
            {'task_key': 'choose-period', 'title': 'Confirm scenario and fiscal period', 'target': '#productivity'},
            {'task_key': 'enter-budget', 'title': 'Enter or paste department budget lines', 'target': '#guidedStart'},
            {'task_key': 'add-comments', 'title': 'Add comments or evidence for material changes', 'target': '#evidence'},
            {'task_key': 'submit-budget', 'title': 'Submit the department budget package', 'target': '#operating-budget'},
        ],
    },
    'controller': {
        'checklist_key': 'controller-close',
        'title': 'Controller close and reporting checklist',
        'role_key': 'controller',
        'tasks': [
            {'task_key': 'review-actuals', 'title': 'Review actuals versus budget and forecast', 'target': '#ledger-depth'},
            {'task_key': 'run-reconciliations', 'title': 'Run reconciliation and exception review', 'target': '#close'},
            {'task_key': 'run-consolidation', 'title': 'Run consolidation and statutory checks', 'target': '#close'},
            {'task_key': 'assemble-board-book', 'title': 'Assemble reports and board narratives', 'target': '#reporting'},
        ],
    },
}

FIELD_HELP = [
    {'field_key': 'scenario', 'label': 'Scenario', 'help_text': 'A scenario is a working version of the budget, forecast, or what-if plan.'},
    {'field_key': 'ledger_basis', 'label': 'Ledger basis', 'help_text': 'Separates actual, budget, forecast, and scenario values in the dimensional ledger.'},
    {'field_key': 'department_code', 'label': 'Department', 'help_text': 'The campus responsibility center that owns the line or submission.'},
    {'field_key': 'fund_code', 'label': 'Fund', 'help_text': 'The funding source used for reporting, restrictions, and margin analysis.'},
    {'field_key': 'account_code', 'label': 'Account', 'help_text': 'The chart-of-accounts code used for reporting and rollups.'},
    {'field_key': 'period', 'label': 'Fiscal period', 'help_text': 'Use YYYY-MM. Closed periods prevent posting unless reopened by an authorized user.'},
    {'field_key': 'variance_threshold', 'label': 'Variance threshold', 'help_text': 'Controls when explanations become required for budget or forecast differences.'},
]

WALKTHROUGHS = [
    {
        'walkthrough_key': 'budget-submission',
        'title': 'Submit a department budget',
        'role_key': 'planner',
        'steps': [
            'Choose the planning scenario and fiscal period.',
            'Use Enter data or Bulk paste to add budget lines.',
            'Review inline validation messages.',
            'Attach comments or evidence for unusual changes.',
            'Submit the budget workspace for approval.',
        ],
    },
    {
        'walkthrough_key': 'close-cycle',
        'title': 'Complete a close cycle',
        'role_key': 'controller',
        'steps': [
            'Load actuals and review ledger basis totals.',
            'Prepare reconciliations and clear exceptions.',
            'Run consolidation, eliminations, and statutory checks.',
            'Verify audit packets and lock the period.',
        ],
    },
    {
        'walkthrough_key': 'admin-readiness',
        'title': 'Prepare for internal deployment',
        'role_key': 'admin',
        'steps': [
            'Validate domain/VPN enforcement and AD/OU mappings.',
            'Confirm SSO production settings.',
            'Run performance benchmarks and restore automation.',
            'Review production logs and admin audit reports.',
        ],
    },
]

PLAYBOOKS = [
    {
        'playbook_key': 'annual-budget',
        'title': 'Annual campus operating budget',
        'summary': 'Open a budget scenario, collect department submissions, review transfers, explain variances, and publish the board package.',
        'sections': ['Scenario setup', 'Department collection', 'Variance explanation', 'Board package'],
    },
    {
        'playbook_key': 'enrollment-tuition',
        'title': 'Enrollment and tuition forecast',
        'summary': 'Combine headcount, FTE, residency, discounts, yield, and retention assumptions into tuition revenue forecasts.',
        'sections': ['Term setup', 'Rate table', 'Forecast inputs', 'Revenue review'],
    },
    {
        'playbook_key': 'monthly-close',
        'title': 'Monthly close and reconciliation',
        'summary': 'Track close tasks, reconcile accounts, confirm entities, run consolidation, and retain evidence.',
        'sections': ['Close calendar', 'Reconciliations', 'Consolidation', 'Audit packet'],
    },
    {
        'playbook_key': 'grant-review',
        'title': 'Grant and fund review',
        'summary': 'Review award budgets, burn rates, restrictions, profitability, and evidence retention.',
        'sections': ['Grant setup', 'Burn rate', 'Fund margin', 'Evidence'],
    },
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def status() -> dict[str, Any]:
    counts = {
        'checklists': len(ROLE_CHECKLISTS),
        'field_help': len(FIELD_HELP),
        'walkthroughs': len(WALKTHROUGHS),
        'playbooks': len(PLAYBOOKS),
        'progress_records': int(db.fetch_one('SELECT COUNT(*) AS count FROM guidance_task_progress')['count']),
        'training_sessions': int(db.fetch_one('SELECT COUNT(*) AS count FROM training_mode_sessions')['count']),
    }
    checks = {
        'role_based_onboarding_ready': True,
        'guided_task_checklists_ready': counts['checklists'] >= 3,
        'field_help_ready': counts['field_help'] >= 6,
        'process_walkthroughs_ready': counts['walkthroughs'] >= 3,
        'campus_playbooks_ready': counts['playbooks'] >= 4,
        'training_mode_ready': True,
    }
    return {'batch': 'B48', 'title': 'In-App Guidance And Finance Training Layer', 'complete': all(checks.values()), 'checks': checks, 'counts': counts}


def workspace(user: dict[str, Any], scenario_id: int | None = None) -> dict[str, Any]:
    role_key = recommended_role(user)
    return {
        'status': status(),
        'recommended_role': role_key,
        'user_roles': user.get('roles', []),
        'checklists': _checklists_for_user(user),
        'field_help': FIELD_HELP,
        'walkthroughs': _visible_by_role(WALKTHROUGHS, role_key),
        'playbooks': PLAYBOOKS,
        'training_modes': [
            {'mode_key': 'admin', 'label': 'Admin training', 'target_role': 'finance.admin'},
            {'mode_key': 'planner', 'label': 'Planner training', 'target_role': 'department.planner'},
            {'mode_key': 'controller', 'label': 'Controller training', 'target_role': 'controller'},
        ],
        'training_sessions': list_training_sessions(user),
        'scenario_id': scenario_id,
    }


def complete_task(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    task = _find_task(payload['checklist_key'], payload['task_key'])
    if task is None:
        raise ValueError('Guidance task not found.')
    now = _now()
    db.execute(
        '''
        INSERT INTO guidance_task_progress (user_id, checklist_key, task_key, status, completed_at, updated_at)
        VALUES (?, ?, ?, 'completed', ?, ?)
        ON CONFLICT(user_id, checklist_key, task_key) DO UPDATE SET
            status = 'completed',
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        ''',
        (int(user['id']), payload['checklist_key'], payload['task_key'], now, now),
    )
    db.log_audit('guidance_task', f"{payload['checklist_key']}:{payload['task_key']}", 'completed', user['email'], payload, now)
    row = db.fetch_one(
        'SELECT * FROM guidance_task_progress WHERE user_id = ? AND checklist_key = ? AND task_key = ?',
        (int(user['id']), payload['checklist_key'], payload['task_key']),
    )
    if row is None:
        raise RuntimeError('Guidance task progress could not be reloaded.')
    return row


def start_training_mode(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    mode_key = payload['mode_key']
    if mode_key not in ROLE_CHECKLISTS:
        raise ValueError('Training mode not found.')
    now = _now()
    session_id = db.execute(
        '''
        INSERT INTO training_mode_sessions (user_id, scenario_id, mode_key, role_key, status, started_at)
        VALUES (?, ?, ?, ?, 'active', ?)
        ''',
        (int(user['id']), payload.get('scenario_id'), mode_key, ROLE_CHECKLISTS[mode_key]['role_key'], now),
    )
    db.log_audit('training_mode', mode_key, 'started', user['email'], payload, now)
    row = db.fetch_one('SELECT * FROM training_mode_sessions WHERE id = ?', (session_id,))
    if row is None:
        raise RuntimeError('Training session could not be reloaded.')
    return row


def list_training_sessions(user: dict[str, Any]) -> list[dict[str, Any]]:
    return db.fetch_all(
        '''
        SELECT *
        FROM training_mode_sessions
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT 25
        ''',
        (int(user['id']),),
    )


def recommended_role(user: dict[str, Any]) -> str:
    roles = set(user.get('roles', []))
    if 'finance.admin' in roles:
        return 'admin'
    if 'budget.office' in roles or 'auditor' in roles:
        return 'controller'
    return 'planner'


def _checklists_for_user(user: dict[str, Any]) -> list[dict[str, Any]]:
    role_key = recommended_role(user)
    keys = ['planner', role_key]
    if role_key == 'admin':
        keys = ['admin', 'planner', 'controller']
    progress = {
        (row['checklist_key'], row['task_key']): row
        for row in db.fetch_all('SELECT * FROM guidance_task_progress WHERE user_id = ?', (int(user['id']),))
    }
    visible = []
    for key in dict.fromkeys(keys):
        item = ROLE_CHECKLISTS[key]
        tasks = []
        for task in item['tasks']:
            row = progress.get((item['checklist_key'], task['task_key']))
            tasks.append({**task, 'status': row['status'] if row else 'open', 'completed_at': row['completed_at'] if row else None})
        complete_count = sum(1 for task in tasks if task['status'] == 'completed')
        visible.append({**item, 'tasks': tasks, 'complete_count': complete_count, 'task_count': len(tasks)})
    return visible


def _visible_by_role(items: list[dict[str, Any]], role_key: str) -> list[dict[str, Any]]:
    if role_key == 'admin':
        return items
    return [item for item in items if item['role_key'] in {role_key, 'planner'}]


def _find_task(checklist_key: str, task_key: str) -> dict[str, Any] | None:
    for checklist in ROLE_CHECKLISTS.values():
        if checklist['checklist_key'] != checklist_key:
            continue
        return next((task for task in checklist['tasks'] if task['task_key'] == task_key), None)
    return None
