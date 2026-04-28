CREATE TABLE IF NOT EXISTS environment_promotions (
    promotion_id text PRIMARY KEY,
    source_environment text NOT NULL,
    target_environment text NOT NULL,
    release_version text NOT NULL,
    status text NOT NULL,
    checklist_json text NOT NULL DEFAULT '{}',
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    promoted_at timestamptz,
    rolled_back_at timestamptz
);

CREATE TABLE IF NOT EXISTS release_notes (
    release_version text PRIMARY KEY,
    notes_markdown text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rollback_plans (
    release_version text PRIMARY KEY,
    plan_json text NOT NULL,
    created_by text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS operational_signoffs (
    id bigserial PRIMARY KEY,
    promotion_id text NOT NULL REFERENCES environment_promotions(promotion_id),
    role_name text NOT NULL,
    user_id text NOT NULL,
    approved boolean NOT NULL,
    notes text NOT NULL DEFAULT '',
    signed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_environment_promotions_status
    ON environment_promotions (status, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_operational_signoffs_promotion
    ON operational_signoffs (promotion_id, role_name);

