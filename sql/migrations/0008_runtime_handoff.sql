ALTER TABLE runtime.trade_tickets ADD COLUMN IF NOT EXISTS wallet_id TEXT;
ALTER TABLE runtime.trade_tickets ADD COLUMN IF NOT EXISTS execution_context_id TEXT;
