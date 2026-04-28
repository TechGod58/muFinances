CREATE TABLE IF NOT EXISTS production_readiness_reviews (
    review_id text PRIMARY KEY,
    runtime_mode text NOT NULL,
    status text NOT NULL,
    findings_json text NOT NULL DEFAULT '[]',
    reviewed_by text,
    reviewed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS security_masking_policy (
    field_name text PRIMARY KEY,
    masking_strategy text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    updated_by text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS segregation_of_duties_rules (
    rule_id text PRIMARY KEY,
    left_permission text NOT NULL,
    right_permission text NOT NULL,
    severity text NOT NULL DEFAULT 'warning',
    active boolean NOT NULL DEFAULT true,
    updated_by text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS admin_impersonation_sessions (
    session_id text PRIMARY KEY,
    admin_user_id text NOT NULL,
    target_user_id text NOT NULL,
    reason text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    ended_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_readiness_reviews_status
    ON production_readiness_reviews (status, reviewed_at DESC);

CREATE INDEX IF NOT EXISTS ix_impersonation_admin_status
    ON admin_impersonation_sessions (admin_user_id, status, started_at DESC);

