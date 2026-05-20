from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from fabric_project.common.http import build_http_session, get_json
from fabric_project.pipelines.context import PipelineContext
from fabric_project.pipelines.extensions.base import DatasetExtension

logger = logging.getLogger(__name__)


_WEATHER_CODE_LABELS = {
    0: "Clear",
    1: "Mostly clear",
    2: "Partly cloudy",
    3: "Cloudy",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    80: "Rain showers",
    95: "Thunderstorm",
}


class WeatherExtension(DatasetExtension):
    name = "weather"

    def extract_bronze(self, context: PipelineContext, cfg: dict[str, Any]) -> Path:
        bronze_dir = context.ensure_layer_dir("bronze", self.name)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        raw_path = bronze_dir / f"weather_raw_{timestamp}.json"

        endpoint = cfg.get("endpoint", "https://api.open-meteo.com/v1/forecast")
        metrics = cfg.get(
            "hourly_metrics",
            [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "cloud_cover",
                "wind_speed_10m",
                "weather_code",
            ],
        )

        params = {
            "latitude": cfg.get("latitude", 40.7128),
            "longitude": cfg.get("longitude", -74.0060),
            "timezone": cfg.get("timezone", "America/New_York"),
            "past_days": cfg.get("past_days", 1),
            "forecast_days": cfg.get("forecast_days", 3),
            "hourly": ",".join(metrics),
        }

        session = build_http_session()
        try:
            payload = get_json(session, endpoint, params=params, timeout=60)
            with raw_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            return raw_path
        except Exception as exc:
            allow_cached_fallback = bool(cfg.get("allow_cached_fallback", True))
            cached_candidates = sorted(bronze_dir.glob("weather_raw_*.json"))
            if allow_cached_fallback and cached_candidates:
                fallback = cached_candidates[-1]
                logger.warning(
                    "Weather API fetch failed (%s: %s); using cached bronze file fallback: %s",
                    type(exc).__name__,
                    exc,
                    fallback,
                )
                return fallback
            raise

    def transform_silver(
        self, context: PipelineContext, cfg: dict[str, Any], bronze_output: Path
    ) -> Path:
        silver_dir = context.ensure_layer_dir("silver", self.name)
        silver_path = silver_dir / "weather_enriched.parquet"

        with bronze_output.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)

        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])
        if not timestamps:
            raise ValueError("Weather API response does not include hourly time series")

        df = pd.DataFrame({"timestamp": pd.to_datetime(timestamps, utc=True)})

        def _metric(key: str, fallback: str | None = None) -> pd.Series:
            values = hourly.get(key)
            if values is None and fallback is not None:
                values = hourly.get(fallback)
            if values is None:
                return pd.Series([None] * len(df))
            return pd.Series(values)

        df["temperature_c"] = _metric("temperature_2m")
        df["humidity_pct"] = _metric("relative_humidity_2m")
        df["apparent_temperature_c"] = _metric("apparent_temperature")
        df["precipitation_mm"] = _metric("precipitation")
        df["cloud_cover_pct"] = _metric("cloud_cover")
        df["wind_speed_kmh"] = _metric("wind_speed_10m", fallback="windspeed_10m")
        df["weather_code"] = _metric("weather_code", fallback="weathercode")

        humidity_factor = 0.55 - (0.0055 * df["humidity_pct"].fillna(0))
        df["comfort_index"] = df["temperature_c"] - humidity_factor * (
            df["temperature_c"] - 14.5
        )
        df["weather_label"] = df["weather_code"].map(_WEATHER_CODE_LABELS).fillna("Unknown")
        df["is_raining"] = df["precipitation_mm"].fillna(0) > 0
        df["city"] = cfg.get("city_name", "New York")
        df["timezone"] = cfg.get("timezone", "America/New_York")
        df["ingested_at"] = datetime.now(UTC)

        df.to_parquet(silver_path, index=False)
        return silver_path

    def publish_gold(
        self, context: PipelineContext, cfg: dict[str, Any], silver_output: Path
    ) -> list[Path]:
        gold_dir = context.ensure_layer_dir("gold", self.name)
        hourly_path = gold_dir / "fact_weather_hourly.parquet"
        daily_path = gold_dir / "fact_weather_daily.parquet"

        df = pd.read_parquet(silver_output)
        df.to_parquet(hourly_path, index=False)

        daily = (
            df.assign(date=pd.to_datetime(df["timestamp"]).dt.date)
            .groupby(["date", "city"], as_index=False)
            .agg(
                avg_temperature_c=("temperature_c", "mean"),
                max_temperature_c=("temperature_c", "max"),
                min_temperature_c=("temperature_c", "min"),
                total_precipitation_mm=("precipitation_mm", "sum"),
                avg_humidity_pct=("humidity_pct", "mean"),
                max_wind_speed_kmh=("wind_speed_kmh", "max"),
            )
        )
        daily.to_parquet(daily_path, index=False)

        return [hourly_path, daily_path]
