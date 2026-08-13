CREATE TABLE IF NOT EXISTS scheduler_capacity_ledger (
    project_id TEXT PRIMARY KEY,
    desired_concurrency INTEGER NOT NULL CHECK (desired_concurrency >= 0),
    healthy_capacity INTEGER NOT NULL CHECK (healthy_capacity >= 0),
    active_invocations INTEGER NOT NULL DEFAULT 0 CHECK (active_invocations >= 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (desired_concurrency <= healthy_capacity)
);

CREATE TABLE IF NOT EXISTS scheduler_budget_ledger (
    project_id TEXT NOT NULL,
    ledger_id TEXT NOT NULL,
    max_tokens INTEGER CHECK (max_tokens IS NULL OR max_tokens >= 0),
    max_cost_usd NUMERIC(18, 6) CHECK (max_cost_usd IS NULL OR max_cost_usd >= 0),
    max_wall_time_ms INTEGER CHECK (max_wall_time_ms IS NULL OR max_wall_time_ms >= 0),
    max_agent_invocations INTEGER CHECK (max_agent_invocations IS NULL OR max_agent_invocations >= 0),
    max_retries INTEGER CHECK (max_retries IS NULL OR max_retries >= 0),
    verify_level TEXT NOT NULL CHECK (verify_level IN ('basic', 'standard', 'strict')),
    exploration_level TEXT NOT NULL CHECK (exploration_level IN ('none', 'targeted', 'broad')),
    tokens_used INTEGER NOT NULL DEFAULT 0 CHECK (tokens_used >= 0),
    cost_usd NUMERIC(18, 6) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
    wall_time_ms INTEGER NOT NULL DEFAULT 0 CHECK (wall_time_ms >= 0),
    agent_invocations_used INTEGER NOT NULL DEFAULT 0 CHECK (agent_invocations_used >= 0),
    retries_used INTEGER NOT NULL DEFAULT 0 CHECK (retries_used >= 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, ledger_id)
);
