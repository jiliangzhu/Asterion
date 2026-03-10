from __future__ import annotations

import importlib.util

from .job_map import list_weather_cold_path_schedules, resolve_default_enabled_schedule_specs


DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None

if DAGSTER_AVAILABLE:  # pragma: no cover - optional dependency
    from dagster import DefaultScheduleStatus, ScheduleDefinition


def build_schedule_definitions(*, job_definitions: dict[str, object] | None = None):
    if not DAGSTER_AVAILABLE:
        return []
    definitions = job_definitions or {}
    schedules = []
    for spec in list_weather_cold_path_schedules():
        job_def = definitions.get(spec.job_name)
        if job_def is None:
            continue
        schedules.append(
            ScheduleDefinition(
                name=spec.schedule_key,
                job=job_def,
                cron_schedule=spec.cron_schedule,
                execution_timezone=spec.execution_timezone,
                default_status=DefaultScheduleStatus.RUNNING if spec.enabled_by_default else DefaultScheduleStatus.STOPPED,
            )
        )
    return schedules


def list_enabled_schedule_keys() -> list[str]:
    return [item.schedule_key for item in resolve_default_enabled_schedule_specs()]
