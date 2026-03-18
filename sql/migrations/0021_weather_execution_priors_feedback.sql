ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS cohort_type TEXT DEFAULT 'market';
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS cohort_key TEXT DEFAULT '';
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS feedback_status TEXT DEFAULT 'heuristic_only';
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS feedback_penalty DOUBLE DEFAULT 0.0;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS cohort_prior_version TEXT;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS miss_rate DOUBLE;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS distortion_rate DOUBLE;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS dominant_miss_reason_bucket TEXT;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS dominant_distortion_reason_bucket TEXT;
ALTER TABLE weather.weather_execution_priors ADD COLUMN IF NOT EXISTS last_feedback_materialization_id TEXT;

UPDATE weather.weather_execution_priors
SET cohort_key = market_id
WHERE COALESCE(cohort_key, '') = '';

UPDATE weather.weather_execution_priors
SET cohort_type = COALESCE(cohort_type, 'market'),
    feedback_status = COALESCE(feedback_status, 'heuristic_only'),
    feedback_penalty = COALESCE(feedback_penalty, 0.0);
