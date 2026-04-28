CREATE TABLE IF NOT EXISTS performance_benchmark_runs (
    run_id text PRIMARY KEY,
    seed_plan_json text NOT NULL,
    result_json text NOT NULL,
    status text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS performance_query_plans (
    id bigserial PRIMARY KEY,
    run_id integer NOT NULL REFERENCES performance_benchmark_runs(id),
    benchmark_name text NOT NULL,
    plan_json text NOT NULL,
    duration_ms numeric NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_performance_runs_status_created
    ON performance_benchmark_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS ix_planning_ledger_period_department_account
    ON planning_ledger (period, department_code, account_code);

CREATE INDEX IF NOT EXISTS ix_planning_ledger_scenario_period
    ON planning_ledger (scenario_id, period);

CREATE INDEX IF NOT EXISTS ix_import_staged_rows_batch_status
    ON import_staged_rows (batch_id, status, row_number);

CREATE INDEX IF NOT EXISTS ix_export_artifacts_type_valid_created
    ON export_artifacts (artifact_type, status, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_background_jobs_status_run_after_priority
    ON background_jobs (status, scheduled_for, priority DESC);
