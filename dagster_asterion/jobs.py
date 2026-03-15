from __future__ import annotations

import importlib.util

from .job_map import list_weather_cold_path_jobs


DAGSTER_AVAILABLE = importlib.util.find_spec("dagster") is not None

if DAGSTER_AVAILABLE:  # pragma: no cover - optional dependency
    from dagster import job, op


def build_job_definitions():
    if not DAGSTER_AVAILABLE:
        return {}

    job_defs = {}
    for spec in list_weather_cold_path_jobs():
        job_defs[spec.job_name] = _build_single_job(spec)
    return job_defs


def _build_single_job(spec):
    @op(
        name=f"{spec.job_name}_op",
        description=spec.description,
        tags={"handler_name": spec.handler_name, "cold_path_mode": spec.mode},
    )
    def _cold_path_shell_op(context) -> dict[str, str]:
        context.log.info("cold-path shell job=%s handler=%s", spec.job_name, spec.handler_name)
        return {"job_name": spec.job_name, "handler_name": spec.handler_name}

    @job(name=spec.job_name, description=spec.description, tags={"cold_path_shell": "true"})
    def _cold_path_shell_job():
        _cold_path_shell_op()

    return _cold_path_shell_job
