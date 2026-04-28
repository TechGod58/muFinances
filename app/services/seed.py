from __future__ import annotations

from datetime import UTC, datetime

from app import db


def seed_if_empty() -> None:
    existing = db.fetch_one('SELECT COUNT(*) AS count FROM scenarios')
    if existing and existing['count'] > 0:
        return

    now = datetime.now(UTC).isoformat()

    dimensions = [
        ('department', 'SCI', 'College of Science'),
        ('department', 'ART', 'College of Arts'),
        ('department', 'OPS', 'Campus Operations'),
        ('fund', 'GEN', 'General Fund'),
        ('fund', 'GRANT', 'Grant Fund'),
        ('account', 'TUITION', 'Tuition Revenue'),
        ('account', 'AUXILIARY', 'Auxiliary Revenue'),
        ('account', 'SALARY', 'Salary Expense'),
        ('account', 'BENEFITS', 'Benefits Expense'),
        ('account', 'UTILITIES', 'Utilities Expense'),
        ('account', 'SUPPLIES', 'Supplies Expense'),
    ]
    db.executemany(
        'INSERT OR IGNORE INTO dimensions (kind, code, name) VALUES (?, ?, ?)',
        dimensions,
    )
    db.executemany(
        '''
        INSERT OR IGNORE INTO dimension_members (dimension_kind, code, name)
        VALUES (?, ?, ?)
        ''',
        dimensions,
    )

    fiscal_periods = [
        ('FY27', '2026-07', 1, 0),
        ('FY27', '2026-08', 2, 0),
        ('FY27', '2026-09', 3, 0),
        ('FY27', '2026-10', 4, 0),
        ('FY27', '2026-11', 5, 0),
        ('FY27', '2026-12', 6, 0),
    ]
    db.executemany(
        '''
        INSERT OR IGNORE INTO fiscal_periods (fiscal_year, period, period_index, is_closed)
        VALUES (?, ?, ?, ?)
        ''',
        fiscal_periods,
    )

    scenario_id = db.execute(
        '''
        INSERT INTO scenarios (name, version, status, start_period, end_period, locked, created_at)
        VALUES (?, ?, ?, ?, ?, 0, ?)
        ''',
        ('FY27 Operating Plan', 'v1', 'draft', '2026-07', '2026-12', now),
    )

    manual_lines = [
        (scenario_id, 'SCI', 'GEN', 'TUITION', '2026-07', 1450000.00, 'Baseline tuition', 'manual', None),
        (scenario_id, 'ART', 'GEN', 'TUITION', '2026-07', 980000.00, 'Baseline tuition', 'manual', None),
        (scenario_id, 'OPS', 'GEN', 'AUXILIARY', '2026-07', 210000.00, 'Housing and dining baseline', 'manual', None),
        (scenario_id, 'SCI', 'GEN', 'SALARY', '2026-07', -760000.00, 'Faculty and staff salaries', 'manual', None),
        (scenario_id, 'ART', 'GEN', 'SALARY', '2026-07', -590000.00, 'Faculty and staff salaries', 'manual', None),
        (scenario_id, 'OPS', 'GEN', 'BENEFITS', '2026-07', -205000.00, 'Benefits baseline', 'manual', None),
        (scenario_id, 'OPS', 'GEN', 'UTILITIES', '2026-07', -124000.00, 'Utilities baseline', 'manual', None),
        (scenario_id, 'OPS', 'GEN', 'SUPPLIES', '2026-07', -84000.00, 'Supplies baseline', 'manual', None),
    ]
    db.executemany(
        '''
        INSERT INTO planning_ledger (
            scenario_id, entity_code, department_code, fund_code, account_code,
            period, amount, notes, source, driver_key, ledger_type,
            posted_by, posted_at, metadata_json
        ) VALUES (?, 'CAMPUS', ?, ?, ?, ?, ?, ?, ?, ?, 'planning', 'seed', ?, '{}')
        ''',
        [line + (now,) for line in manual_lines],
    )

    drivers = [
        (scenario_id, 'student_growth', 'Student growth rate', None, 0.035, 'ratio'),
        (scenario_id, 'salary_step_increase', 'Salary step increase', None, 0.04, 'ratio'),
        (scenario_id, 'utilities_inflation', 'Utilities inflation', None, 0.06, 'ratio'),
        (scenario_id, 'auxiliary_growth', 'Auxiliary growth', None, 0.025, 'ratio'),
        (scenario_id, 'gross_margin_target', 'Gross margin target', 'round((student_growth - salary_step_increase) * 100, 2)', None, 'percent'),
        (scenario_id, 'revenue_risk_buffer', 'Revenue risk buffer', 'round(max(student_growth - 0.01, 0.0), 4)', None, 'ratio'),
    ]
    db.executemany(
        '''
        INSERT INTO drivers (scenario_id, driver_key, label, expression, value, unit)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        drivers,
    )

    workflows = [
        (scenario_id, 'Department Budget Submission', 'draft', 'active', 'Budget Office', now),
        (scenario_id, 'VP Finance Review', 'review', 'pending', 'VP Finance', now),
        (scenario_id, 'Board Publication', 'published', 'pending', 'Controller', now),
    ]
    db.executemany(
        'INSERT INTO workflows (scenario_id, name, step, status, owner, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
        workflows,
    )

    integrations = [
        ('Banner ERP', 'erp', 'planned', 'inbound', '/connectors/banner'),
        ('Workday HCM', 'hris', 'planned', 'inbound', '/connectors/workday'),
        ('Salesforce', 'crm', 'planned', 'inbound', '/connectors/salesforce'),
        ('Power BI Export', 'bi', 'active', 'outbound', '/exports/powerbi'),
    ]
    db.executemany(
        'INSERT INTO integrations (name, category, status, direction, endpoint_hint) VALUES (?, ?, ?, ?, ?)',
        integrations,
    )

    db.log_audit(
        entity_type='scenario',
        entity_id=str(scenario_id),
        action='seeded',
        actor='system',
        detail={'name': 'FY27 Operating Plan'},
        created_at=now,
    )
