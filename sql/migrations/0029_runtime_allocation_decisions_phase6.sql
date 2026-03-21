ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS base_ranking_score DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS deployable_expected_pnl DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS deployable_notional DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS max_deployable_size DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS capital_scarcity_penalty DOUBLE DEFAULT 0.0;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS concentration_penalty DOUBLE DEFAULT 0.0;
