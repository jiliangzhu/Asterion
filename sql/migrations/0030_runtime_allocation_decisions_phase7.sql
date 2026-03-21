ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS pre_budget_deployable_size DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS pre_budget_deployable_notional DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS pre_budget_deployable_expected_pnl DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS rerank_position BIGINT;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS rerank_reason_codes_json TEXT DEFAULT '[]';
