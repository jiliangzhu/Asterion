"""Weather forecast package."""

from .adapters import NWSAdapter, OpenMeteoAdapter
from .cache import InMemoryForecastCache
from .persistence import (
    WEATHER_FORECAST_RUN_COLUMNS,
    WEATHER_FORECAST_REPLAY_COLUMNS,
    WEATHER_FORECAST_REPLAY_DIFF_COLUMNS,
    enqueue_forecast_replay_diff_upserts,
    enqueue_forecast_replay_upserts,
    build_forecast_run_record,
    enqueue_forecast_run_upserts,
    forecast_run_to_row,
)
from .service import AdapterRouter, ForecastDistribution, ForecastService, build_forecast_request, normalize_forecast_source_name
from .replay import (
    build_forecast_replay_diff_records,
    build_forecast_replay_record,
    build_forecast_replay_request,
    load_original_pricing_outputs,
    load_replay_inputs,
    recompute_forecast_run,
    recompute_pricing_outputs,
    run_forecast_replay,
)

__all__ = [
    "AdapterRouter",
    "ForecastDistribution",
    "ForecastService",
    "InMemoryForecastCache",
    "NWSAdapter",
    "OpenMeteoAdapter",
    "WEATHER_FORECAST_REPLAY_COLUMNS",
    "WEATHER_FORECAST_REPLAY_DIFF_COLUMNS",
    "WEATHER_FORECAST_RUN_COLUMNS",
    "build_forecast_replay_diff_records",
    "build_forecast_replay_record",
    "build_forecast_replay_request",
    "build_forecast_run_record",
    "build_forecast_request",
    "enqueue_forecast_replay_diff_upserts",
    "enqueue_forecast_replay_upserts",
    "enqueue_forecast_run_upserts",
    "forecast_run_to_row",
    "load_original_pricing_outputs",
    "load_replay_inputs",
    "normalize_forecast_source_name",
    "recompute_forecast_run",
    "recompute_pricing_outputs",
    "run_forecast_replay",
]
