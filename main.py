from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app import db
from app.schemas import (
    AuditLogOut,
    DriverOut,
    ForecastRunResult,
    IntegrationOut,
    PlanLineItemCreate,
    PlanLineItemOut,
    ScenarioCreate,
    ScenarioOut,
    SummaryReport,
    WorkflowAdvance,
    WorkflowCreate,
)
from app.services.forecast_engine import run_forecast
from app.services.seed import seed_if_empty

app = FastAPI(title='Campus FPM Base', version='0.1.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

STATIC_DIR = Path(__file__).resolve().parent.parent / 'static'


def init_application() -> None:
    db.init_db()
    seed_if_empty()


init_application()

@app.get('/api/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'app': 'campus-fpm-base'}


@app.get('/api/capabilities')
def capabilities() -> dict[str, Any]:
    return {
        'platform_name': 'Campus FPM Base',
        'different_from_prophix': [
            'Driver graph engine instead of a template-first planning workflow.',
            'API-first modules for scenario, workflow, audit, and integrations.',
            'Local-first single-node deployment for campus internal hosting on port 3200.',
            'Composable dimension model for departments, funds, accounts, grants, projects, and enrollment drivers.',
        ],
        'current_modules': [
            'budgeting',
            'forecasting',
            'scenario planning',
            'workflow approvals',
            'reporting',
            'audit trail',
            'data integration registry',
            'security-ready role boundaries',
        ],
        'campus_extensions_to_build_next': [
            'faculty load planning',
            'enrollment and tuition modeling',
            'grant budgeting',
            'capital planning',
            'cash flow planning',
            'close and consolidation',
            'variance narratives',
            'governed AI copilots',
        ],
    }


@app.get('/api/dimensions')
def dimensions() -> dict[str, list[dict[str, Any]]]:
    rows = db.fetch_all('SELECT * FROM dimensions ORDER BY kind, code')
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row['kind'], []).append(row)
    return grouped


@app.get('/api/scenarios', response_model=list[ScenarioOut])
def get_scenarios() -> list[dict[str, Any]]:
    rows = db.fetch_all('SELECT * FROM scenarios ORDER BY id DESC')
    for row in rows:
        row['locked'] = bool(row['locked'])
    return rows


@app.post('/api/scenarios', response_model=ScenarioOut)
def create_scenario(payload: ScenarioCreate) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    scenario_id = db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, 'draft', ?, ?, 0, ?)
        ''',
        (payload.name, payload.version, payload.start_period, payload.end_period, now),
    )
    db.log_audit(
        entity_type='scenario',
        entity_id=str(scenario_id),
        action='created',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Scenario was created but could not be reloaded.')
    row['locked'] = bool(row['locked'])
    return row


@app.get('/api/scenarios/{scenario_id}/drivers', response_model=list[DriverOut])
def get_drivers(scenario_id: int) -> list[dict[str, Any]]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    return db.fetch_all(
        'SELECT driver_key, label, expression, value, unit FROM drivers WHERE scenario_id = ? ORDER BY id ASC',
        (scenario_id,),
    )


@app.get('/api/scenarios/{scenario_id}/line-items', response_model=list[PlanLineItemOut])
def get_line_items(scenario_id: int) -> list[dict[str, Any]]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    return db.fetch_all(
        '''
        SELECT * FROM plan_line_items
        WHERE scenario_id = ?
        ORDER BY period ASC, department_code ASC, account_code ASC, id ASC
        ''',
        (scenario_id,),
    )


@app.post('/api/scenarios/{scenario_id}/line-items', response_model=PlanLineItemOut)
def create_line_item(scenario_id: int, payload: PlanLineItemCreate) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT * FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    if bool(scenario['locked']):
        raise HTTPException(status_code=409, detail='Scenario is locked.')

    item_id = db.execute(
        '''
        INSERT INTO plan_line_items (
            scenario_id, department_code, fund_code, account_code,
            period, amount, notes, source, driver_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'manual', NULL)
        ''',
        (
            scenario_id,
            payload.department_code,
            payload.fund_code,
            payload.account_code,
            payload.period,
            payload.amount,
            payload.notes,
        ),
    )

    db.log_audit(
        entity_type='plan_line_item',
        entity_id=str(item_id),
        action='created',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=datetime.now(UTC).isoformat(),
    )
    row = db.fetch_one('SELECT * FROM plan_line_items WHERE id = ?', (item_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Line item was created but could not be reloaded.')
    return row


@app.post('/api/scenarios/{scenario_id}/forecast/run', response_model=ForecastRunResult)
def forecast_scenario(scenario_id: int) -> dict[str, Any]:
    try:
        return run_forecast(scenario_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get('/api/reports/summary', response_model=SummaryReport)
def summary_report(scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')

    rows = db.fetch_all(
        '''
        SELECT department_code, account_code, SUM(amount) AS total
        FROM plan_line_items
        WHERE scenario_id = ?
        GROUP BY department_code, account_code
        ORDER BY department_code, account_code
        ''',
        (scenario_id,),
    )

    by_department: dict[str, float] = {}
    by_account: dict[str, float] = {}
    revenue_total = 0.0
    expense_total = 0.0

    for row in rows:
        total = round(float(row['total']), 2)
        by_department[row['department_code']] = round(by_department.get(row['department_code'], 0.0) + total, 2)
        by_account[row['account_code']] = round(by_account.get(row['account_code'], 0.0) + total, 2)
        if total >= 0:
            revenue_total += total
        else:
            expense_total += total

    return {
        'scenario_id': scenario_id,
        'revenue_total': round(revenue_total, 2),
        'expense_total': round(expense_total, 2),
        'net_total': round(revenue_total + expense_total, 2),
        'by_department': by_department,
        'by_account': by_account,
    }


@app.get('/api/workflows')
def get_workflows(scenario_id: int | None = Query(None, ge=1)) -> list[dict[str, Any]]:
    if scenario_id is None:
        return db.fetch_all('SELECT * FROM workflows ORDER BY scenario_id, id')
    return db.fetch_all('SELECT * FROM workflows WHERE scenario_id = ? ORDER BY id', (scenario_id,))


@app.post('/api/workflows')
def create_workflow(payload: WorkflowCreate) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    workflow_id = db.execute(
        '''
        INSERT INTO workflows (scenario_id, name, step, status, owner, updated_at)
        VALUES (?, ?, 'draft', 'pending', ?, ?)
        ''',
        (payload.scenario_id, payload.name, payload.owner, now),
    )
    db.log_audit(
        entity_type='workflow',
        entity_id=str(workflow_id),
        action='created',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Workflow was created but could not be reloaded.')
    return row


@app.post('/api/workflows/{workflow_id}/advance')
def advance_workflow(workflow_id: int, payload: WorkflowAdvance) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    workflow = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if workflow is None:
        raise HTTPException(status_code=404, detail='Workflow not found.')
    db.execute(
        'UPDATE workflows SET step = ?, status = ?, updated_at = ? WHERE id = ?',
        (payload.step, payload.status, now, workflow_id),
    )
    db.log_audit(
        entity_type='workflow',
        entity_id=str(workflow_id),
        action='advanced',
        actor='api.user',
        detail=payload.model_dump(),
        created_at=now,
    )
    row = db.fetch_one('SELECT * FROM workflows WHERE id = ?', (workflow_id,))
    if row is None:
        raise HTTPException(status_code=500, detail='Workflow was advanced but could not be reloaded.')
    return row


@app.get('/api/audit-logs', response_model=list[AuditLogOut])
def get_audit_logs(limit: int = Query(50, ge=1, le=250)) -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?', (limit,))


@app.get('/api/integrations', response_model=list[IntegrationOut])
def get_integrations() -> list[dict[str, Any]]:
    return db.fetch_all('SELECT * FROM integrations ORDER BY id ASC')


@app.get('/')
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / 'index.html')


@app.get('/api/bootstrap')
def bootstrap() -> dict[str, Any]:
    scenarios = get_scenarios()
    active_scenario = scenarios[0] if scenarios else None
    scenario_id = active_scenario['id'] if active_scenario else None
    return {
        'scenarios': scenarios,
        'activeScenario': active_scenario,
        'dimensions': dimensions(),
        'workflows': get_workflows(scenario_id) if scenario_id else [],
        'drivers': get_drivers(scenario_id) if scenario_id else [],
        'summary': summary_report(scenario_id) if scenario_id else None,
        'lineItems': get_line_items(scenario_id) if scenario_id else [],
        'integrations': get_integrations(),
    }


@app.get('/api/roadmap')
def roadmap() -> dict[str, Any]:
    return {
        'phase_1': [
            'scenario manager',
            'budget entry',
            'approval workflow',
            'summary reporting',
            'audit trail',
            'seeded integrations',
        ],
        'phase_2': [
            'enrollment planning',
            'faculty planning',
            'position control',
            'cash flow',
            'variance explanations',
        ],
        'phase_3': [
            'consolidation',
            'reconciliation',
            'close management',
            'governed AI narrative generation',
            'self-service report builder',
        ],
    }


app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')
