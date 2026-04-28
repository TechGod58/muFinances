CREATE TABLE IF NOT EXISTS runtime_data_partitions (
    namespace text PRIMARY KEY,
    runtime_mode text NOT NULL,
    allows_demo_data boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_seed_runs (
    id text PRIMARY KEY,
    namespace text NOT NULL REFERENCES runtime_data_partitions(namespace),
    seed_name text NOT NULL,
    status text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

