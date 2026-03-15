from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

from asterion_core.contracts import CalibrationLookupKey, ForecastCalibrationSampleRecord, ForecastRunRecord, stable_object_id


def forecast_horizon_bucket(*, observation_date: date, forecast_target_time: datetime) -> str:
    horizon_days = max(0, (observation_date - forecast_target_time.date()).days)
    if horizon_days <= 1:
        return "0-1"
    if horizon_days <= 3:
        return "2-3"
    if horizon_days <= 7:
        return "4-7"
    return "8+"


def season_bucket(value: date | datetime) -> str:
    month = value.month
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def build_calibration_lookup_key(
    *,
    station_id: str,
    source: str,
    observation_date: date,
    forecast_target_time: datetime,
    metric: str,
) -> CalibrationLookupKey:
    return CalibrationLookupKey(
        station_id=station_id,
        source=str(source).strip().lower(),
        forecast_horizon_bucket=forecast_horizon_bucket(
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
        ),
        season_bucket=season_bucket(observation_date),
        metric=metric,
    )


def forecast_distribution_mean(distribution: dict[int, float]) -> float:
    if not distribution:
        raise ValueError("temperature distribution is required")
    total = sum(float(value) for value in distribution.values())
    if total <= 0:
        raise ValueError("temperature distribution total must be positive")
    return sum(float(temp) * float(prob) for temp, prob in distribution.items()) / total


def build_forecast_calibration_sample(
    *,
    forecast_run: ForecastRunRecord,
    observed_value: float,
    created_at: datetime | None = None,
) -> ForecastCalibrationSampleRecord:
    mean = forecast_distribution_mean(forecast_run.forecast_payload.get("temperature_distribution") or {})
    normalized_created_at = created_at or datetime.now(UTC)
    lookup_key = build_calibration_lookup_key(
        station_id=forecast_run.station_id,
        source=forecast_run.source,
        observation_date=forecast_run.observation_date,
        forecast_target_time=forecast_run.forecast_target_time,
        metric=forecast_run.metric,
    )
    return ForecastCalibrationSampleRecord(
        sample_id=stable_object_id(
            "fcal",
            {
                "market_id": forecast_run.market_id,
                "run_id": forecast_run.run_id,
                "observed_value": float(observed_value),
            },
        ),
        market_id=forecast_run.market_id,
        station_id=forecast_run.station_id,
        source=forecast_run.source,
        forecast_horizon_bucket=lookup_key.forecast_horizon_bucket,
        season_bucket=lookup_key.season_bucket,
        metric=forecast_run.metric,
        forecast_target_time=forecast_run.forecast_target_time,
        forecast_mean=mean,
        observed_value=float(observed_value),
        residual=float(observed_value) - mean,
        created_at=normalized_created_at,
    )


class ForecastStdDevProvider(Protocol):
    def resolve_std_dev(self, *, station_id: str, source: str, observation_date: date, forecast_target_time: datetime, metric: str) -> float | None:
        ...


@dataclass(frozen=True)
class DuckDBForecastStdDevProvider:
    db_path: str | Path

    def resolve_std_dev(
        self,
        *,
        station_id: str,
        source: str,
        observation_date: date,
        forecast_target_time: datetime,
        metric: str,
    ) -> float | None:
        try:
            import duckdb
        except ModuleNotFoundError:  # pragma: no cover
            return None
        db_path = Path(self.db_path)
        if not db_path.exists():
            return None
        lookup_key = build_calibration_lookup_key(
            station_id=station_id,
            source=source,
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
            metric=metric,
        )
        try:
            con = duckdb.connect(str(db_path), read_only=True)
        except Exception:  # noqa: BLE001
            return None
        try:
            row = con.execute(
                """
                SELECT
                    COUNT(*) AS sample_count,
                    AVG(ABS(residual)) AS avg_abs_residual,
                    STDDEV_SAMP(residual) AS residual_stddev
                FROM weather.forecast_calibration_samples
                WHERE station_id = ?
                  AND source = ?
                  AND forecast_horizon_bucket = ?
                  AND season_bucket = ?
                  AND metric = ?
                """,
                [
                    lookup_key.station_id,
                    lookup_key.source,
                    lookup_key.forecast_horizon_bucket,
                    lookup_key.season_bucket,
                    lookup_key.metric,
                ],
            ).fetchone()
        except Exception:  # noqa: BLE001
            return None
        finally:
            con.close()
        if row is None:
            return None
        sample_count = int(row[0] or 0)
        avg_abs_residual = float(row[1] or 0.0)
        residual_stddev = float(row[2] or 0.0)
        if sample_count <= 0:
            return None
        if residual_stddev > 0.0:
            return residual_stddev
        if avg_abs_residual > 0.0:
            return avg_abs_residual
        return None


__all__ = [
    "DuckDBForecastStdDevProvider",
    "ForecastStdDevProvider",
    "build_calibration_lookup_key",
    "build_forecast_calibration_sample",
    "forecast_distribution_mean",
    "forecast_horizon_bucket",
    "season_bucket",
]
