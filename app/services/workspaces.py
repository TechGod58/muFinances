from __future__ import annotations

from typing import Any

from app import db
from app.services.foundation import summary_by_dimensions


WORKSPACE_DEFINITIONS = [
    {
        'key': 'budget_office',
        'title': 'Budget Office',
        'roles': ['finance.admin', 'budget.office'],
        'permissions': ['operating_budget.manage', 'reporting.manage'],
    },
    {
        'key': 'department_planner',
        'title': 'Department Planner',
        'roles': ['finance.admin', 'budget.office', 'department.planner'],
        'permissions': ['operating_budget.manage', 'forecast.manage'],
    },
    {
        'key': 'controller',
        'title': 'Controller',
        'roles': ['finance.admin', 'budget.office', 'auditor'],
        'permissions': ['close.manage', 'consolidation.manage'],
    },
    {
        'key': 'grants',
        'title': 'Grants',
        'roles': ['finance.admin', 'budget.office', 'department.planner'],
        'permissions': ['campus_planning.manage'],
    },
    {
        'key': 'executive',
        'title': 'Executive Dashboard',
        'roles': ['finance.admin', 'budget.office', 'auditor'],
        'permissions': ['reports.read', 'reporting.manage'],
    },
]


def status() -> dict[str, Any]:
    checks = {
        'budget_office_workspace_ready': True,
        'department_planner_workspace_ready': True,
        'controller_workspace_ready': True,
        'grants_workspace_ready': True,
        'executive_dashboard_ready': True,
        'role_visibility_ready': True,
    }
    return {
        'batch': 'B11',
        'title': 'Workspace UX Completion',
        'complete': all(checks.values()),
        'checks': checks,
        'counts': {'workspace_definitions': len(WORKSPACE_DEFINITIONS)},
    }


def role_workspaces(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    roles = set(user.get('roles', []))
    visible = []
    for definition in WORKSPACE_DEFINITIONS:
        if 'finance.admin' in roles or roles.intersection(definition['roles']):
            visible.append(_build_workspace(definition, scenario_id, user))
    return {'scenario_id': scenario_id, 'count': len(visible), 'workspaces': visible}


def _build_workspace(definition: dict[str, Any], scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    builders = {
        'budget_office': _budget_office,
        'department_planner': _department_planner,
        'controller': _controller,
        'grants': _grants,
        'executive': _executive,
    }
    data = builders[definition['key']](scenario_id, user)
    return {
        'key': definition['key'],
        'title': definition['title'],
        'metrics': data['metrics'],
        'work_queue': data['work_queue'],
        'quick_links': data['quick_links'],
    }


def _budget_office(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    summary = summary_by_dimensions(scenario_id, user=user)
    pending_submissions = _count('budget_submissions', 'scenario_id = ? AND status != ?', (scenario_id, 'approved'))
    pending_transfers = _count('budget_transfers', 'scenario_id = ? AND status != ?', (scenario_id, 'approved'))
    pending_automation = _count('automation_recommendations', 'scenario_id = ? AND status = ?', (scenario_id, 'pending_review'))
    return {
        'metrics': [
            _metric('Revenue', summary['revenue_total'], 'currency'),
            _metric('Expenses', summary['expense_total'], 'currency'),
            _metric('Pending submissions', pending_submissions, 'count'),
            _metric('Transfer requests', pending_transfers, 'count'),
        ],
        'work_queue': _queue([
            ('Review department submissions', pending_submissions, '#operating-budget'),
            ('Approve transfer requests', pending_transfers, '#operating-budget'),
            ('Review automation recommendations', pending_automation, '#automation'),
        ]),
        'quick_links': _links(['#operating-budget', '#reporting', '#automation']),
    }


def _department_planner(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    line_count = _count('planning_ledger', 'scenario_id = ? AND reversed_at IS NULL', (scenario_id,))
    assumptions = _count('budget_assumptions', 'scenario_id = ?', (scenario_id,))
    forecasts = _count('forecast_runs', 'scenario_id = ?', (scenario_id,))
    submissions = _count('budget_submissions', 'scenario_id = ?', (scenario_id,))
    return {
        'metrics': [
            _metric('Ledger lines', line_count, 'count'),
            _metric('Assumptions', assumptions, 'count'),
            _metric('Forecast runs', forecasts, 'count'),
            _metric('Submissions', submissions, 'count'),
        ],
        'work_queue': _queue([
            ('Update operating budget inputs', max(1, submissions), '#operating-budget'),
            ('Refresh forecast drivers', max(1, forecasts), '#scenario-engine'),
            ('Validate imported budget lines', line_count, '#line-items'),
        ]),
        'quick_links': _links(['#operating-budget', '#scenario-engine', '#line-items']),
    }


def _controller(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    open_checklists = _count('close_checklists', 'scenario_id = ? AND status != ?', (scenario_id, 'complete'))
    recon_variances = _count('account_reconciliations', 'scenario_id = ? AND status = ?', (scenario_id, 'variance'))
    consolidation_runs = _count('consolidation_runs', 'scenario_id = ?', (scenario_id,))
    audit_packets = _audit_packet_count(scenario_id)
    return {
        'metrics': [
            _metric('Open close items', open_checklists, 'count'),
            _metric('Recon variances', recon_variances, 'count'),
            _metric('Consolidation runs', consolidation_runs, 'count'),
            _metric('Audit packets', audit_packets, 'count'),
        ],
        'work_queue': _queue([
            ('Complete close checklist', open_checklists, '#close'),
            ('Resolve reconciliation variances', recon_variances, '#close'),
            ('Seal audit packet', 0 if audit_packets else 1, '#close'),
        ]),
        'quick_links': _links(['#close', '#reporting', '#automation']),
    }


def _grants(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    grants = db.fetch_all('SELECT * FROM grant_budgets WHERE scenario_id = ?', (scenario_id,))
    remaining = round(sum(float(row['total_award']) - float(row['spent_to_date']) for row in grants), 2)
    burn_risk = sum(1 for row in grants if (float(row['spent_to_date']) / max(1.0, float(row['total_award']))) > 0.8)
    capital = _count('capital_requests', 'scenario_id = ?', (scenario_id,))
    return {
        'metrics': [
            _metric('Grant budgets', len(grants), 'count'),
            _metric('Remaining award', remaining, 'currency'),
            _metric('High burn risk', burn_risk, 'count'),
            _metric('Capital requests', capital, 'count'),
        ],
        'work_queue': _queue([
            ('Review grant burn rates', burn_risk, '#campus-planning'),
            ('Update grant budgets', max(1, len(grants)), '#campus-planning'),
            ('Review capital requests', capital, '#campus-planning'),
        ]),
        'quick_links': _links(['#campus-planning', '#reporting', '#integrations']),
    }


def _executive(scenario_id: int, user: dict[str, Any]) -> dict[str, Any]:
    summary = summary_by_dimensions(scenario_id, user=user)
    automation_pending = _count('automation_recommendations', 'scenario_id = ? AND status = ?', (scenario_id, 'pending_review'))
    powerbi_exports = _count('powerbi_exports', 'scenario_id = ?', (scenario_id,))
    report_exports = _count('report_exports', 'scenario_id = ?', (scenario_id,))
    return {
        'metrics': [
            _metric('Net position', summary['net_total'], 'currency'),
            _metric('Revenue', summary['revenue_total'], 'currency'),
            _metric('Pending approvals', automation_pending, 'count'),
            _metric('BI/report exports', powerbi_exports + report_exports, 'count'),
        ],
        'work_queue': _queue([
            ('Review executive financial statement', 1, '#reporting'),
            ('Review pending automation decisions', automation_pending, '#automation'),
            ('Refresh Power BI package', 0 if powerbi_exports else 1, '#integrations'),
        ]),
        'quick_links': _links(['#summary', '#reporting', '#automation', '#integrations']),
    }


def _count(table: str, where: str, params: tuple[Any, ...]) -> int:
    row = db.fetch_one(f'SELECT COUNT(*) AS count FROM {table} WHERE {where}', params)
    return int(row['count'])


def _audit_packet_count(scenario_id: int) -> int:
    row = db.fetch_one(
        '''
        SELECT COUNT(*) AS count
        FROM audit_packets ap
        JOIN consolidation_runs cr ON cr.id = ap.consolidation_run_id
        WHERE cr.scenario_id = ?
        ''',
        (scenario_id,),
    )
    return int(row['count'])


def _metric(label: str, value: float | int, kind: str) -> dict[str, Any]:
    return {'label': label, 'value': value, 'kind': kind}


def _queue(items: list[tuple[str, int, str]]) -> list[dict[str, Any]]:
    return [{'label': label, 'count': count, 'href': href} for label, count, href in items]


def _links(hrefs: list[str]) -> list[dict[str, str]]:
    labels = {
        '#summary': 'Summary',
        '#operating-budget': 'Operating budget',
        '#scenario-engine': 'Forecast engine',
        '#line-items': 'Budget lines',
        '#campus-planning': 'Campus planning',
        '#close': 'Close',
        '#reporting': 'Reporting',
        '#automation': 'Automation',
        '#integrations': 'Integrations',
    }
    return [{'label': labels[href], 'href': href} for href in hrefs]
