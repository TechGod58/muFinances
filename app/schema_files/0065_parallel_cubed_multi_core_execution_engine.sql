CREATE TABLE IF NOT EXISTS parallel_cubed_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_key TEXT NOT NULL UNIQUE,
    scenario_id INTEGER DEFAULT NULL,
    work_type TEXT NOT NULL,
    partition_strategy TEXT NOT NULL,
    executor_kind TEXT NOT NULL,
    logical_cores INTEGER NOT NULL,
    requested_workers INTEGER NOT NULL,
    worker_count INTEGER NOT NULL,
    partition_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    throughput_per_second REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    reduce_status TEXT NOT NULL DEFAULT 'pending',
    result_json TEXT NOT NULL DEFAULT '{}',
    benchmark_json TEXT NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT DEFAULT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS parallel_cubed_partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    partition_key TEXT NOT NULL,
    work_type TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    input_count INTEGER NOT NULL DEFAULT 0,
    output_count INTEGER NOT NULL DEFAULT 0,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES parallel_cubed_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_parallel_cubed_runs_scenario
    ON parallel_cubed_runs (scenario_id, started_at);

CREATE INDEX IF NOT EXISTS idx_parallel_cubed_partitions_run
    ON parallel_cubed_partitions (run_id, work_type);

INSERT OR IGNORE INTO schema_migrations (migration_key, description, checksum, applied_at)
VALUES (
    '0065_parallel_cubed_multi_core_execution_engine',
    'Create Parallel Cubed worker-pool execution, partitioned calculations, parallel import/report phases, safe merge/reduce history, CPU detection, and benchmark dashboard records.',
    'builtin-0065',
    datetime('now')
);
