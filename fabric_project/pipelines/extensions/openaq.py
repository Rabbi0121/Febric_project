from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from fabric_project.common.http import build_http_session, get_json
from fabric_project.pipelines.context import PipelineContext
from fabric_project.pipelines.extensions.base import DatasetExtension

logger = logging.getLogger(__name__)


class OpenAQExtension(DatasetExtension):
    name = "openaq"

    def run(self, context: PipelineContext, cfg: dict[str, Any]) -> dict[str, str | list[str]]:
        allow_cached_fallback = bool(cfg.get("allow_cached_fallback", True))
        try:
            return super().run(context, cfg)
        except Exception as exc:
            if not allow_cached_fallback:
                raise

            fallback = self._cached_artifacts(context)
            if fallback is None:
                raise

            logger.warning(
                "OpenAQ pipeline failed (%s: %s); using cached artifacts fallback: %s",
                type(exc).__name__,
                exc,
                fallback["gold"],
            )
            return {
                "dataset": self.name,
                "bronze": [str(path) for path in fallback["bronze"]],
                "silver": [str(path) for path in fallback["silver"]],
                "gold": str(fallback["gold"]),
            }

    def extract_bronze(self, context: PipelineContext, cfg: dict[str, Any]) -> Path:
        bronze_dir = context.ensure_layer_dir("bronze", self.name)
        raw_path = bronze_dir / "openaq_raw.json"

        rows = self._fetch_v3_rows(cfg)
        if not rows:
            raise ValueError("OpenAQ v3 query returned zero rows")

        with raw_path.open("w", encoding="utf-8") as fh:
            json.dump(
                {
                    "meta": {
                        "source": "openaq_v3",
                        "row_count": len(rows),
                        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    "results": rows,
                },
                fh,
                indent=2,
            )
        logger.info("Fetched %d OpenAQ rows from API v3", len(rows))

        return raw_path

    def transform_silver(
        self, context: PipelineContext, cfg: dict[str, Any], bronze_output: Path
    ) -> Path:
        silver_dir = context.ensure_layer_dir("silver", self.name)
        silver_path = silver_dir / "air_quality_measurements.parquet"

        with bronze_output.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)

        rows = payload.get("results", [])
        df = pd.DataFrame(rows)
        if df.empty:
            raise ValueError("OpenAQ extraction returned zero rows")

        df["timestamp_utc"] = pd.to_datetime(df.get("timestamp_utc"), utc=True, errors="coerce")
        if "timestamp_local" in df.columns:
            # Normalize mixed-offset local timestamps to a consistent UTC-aware dtype.
            df["timestamp_local"] = pd.to_datetime(
                df["timestamp_local"], utc=True, errors="coerce"
            )

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        for col in ["parameter", "unit", "location", "city", "country"]:
            if col in df.columns:
                df[col] = df[col].astype("string")

        df = df.dropna(subset=["timestamp_utc", "value", "parameter"]).drop_duplicates(
            subset=["timestamp_utc", "sensor_id", "parameter", "value"]
        )

        df.to_parquet(silver_path, index=False)
        return silver_path

    def publish_gold(
        self, context: PipelineContext, cfg: dict[str, Any], silver_output: Path
    ) -> Path:
        gold_dir = context.ensure_layer_dir("gold", self.name)
        gold_path = gold_dir / "fact_air_quality_daily.parquet"

        df = pd.read_parquet(silver_output)
        df["date"] = pd.to_datetime(df["timestamp_utc"]).dt.date

        fact = (
            df.groupby(["date", "city", "parameter"], as_index=False)
            .agg(
                avg_pollutant_value=("value", "mean"),
                max_pollutant_value=("value", "max"),
                measurement_count=("value", "count"),
            )
            .sort_values(["date", "city", "parameter"])
        )

        fact.to_parquet(gold_path, index=False)
        return gold_path

    def _fetch_v3_rows(self, cfg: dict[str, Any]) -> list[dict[str, Any]]:
        api_key = cfg.get("api_key") or os.getenv(cfg.get("api_key_env", "OPENAQ_API_KEY"), "")
        if not api_key:
            raise ValueError(
                "OPENAQ_API_KEY is required for OpenAQ API v3. "
                "Set OPENAQ_API_KEY in your environment or provide openaq.api_key in config."
            )

        endpoint_base = cfg.get("endpoint_base", "https://api.openaq.org/v3").rstrip("/")
        location_limit = int(cfg.get("location_limit", 5))
        location_pages = int(cfg.get("location_pages", 1))
        sensor_limit_per_location = int(cfg.get("sensor_limit_per_location", 20))
        measurement_limit = min(1000, int(cfg.get("limit", 500)))
        measurement_pages = int(cfg.get("pages", 2))

        parameter_names = {
            str(name).strip().lower()
            for name in cfg.get("parameter_names", ["pm25", "no2", "o3"])
            if str(name).strip()
        }

        headers = {"X-API-Key": api_key}
        session = build_http_session()

        latitude = float(cfg.get("latitude", 40.7128))
        longitude = float(cfg.get("longitude", -74.0060))
        radius_meters = int(cfg.get("radius_meters", 25000))

        locations: list[dict[str, Any]] = []
        for page in range(1, location_pages + 1):
            params = {
                "coordinates": f"{latitude},{longitude}",
                "radius": radius_meters,
                "limit": location_limit,
                "page": page,
            }
            payload = get_json(
                session,
                f"{endpoint_base}/locations",
                params=params,
                headers=headers,
                timeout=60,
            )
            batch = payload.get("results", [])
            locations.extend(batch)
            if len(batch) < location_limit:
                break

        if not locations:
            raise ValueError("OpenAQ v3 returned no locations")

        rows: list[dict[str, Any]] = []

        for location in locations:
            location_id = location.get("id")
            if location_id is None:
                continue

            sensor_payload = get_json(
                session,
                f"{endpoint_base}/locations/{location_id}/sensors",
                params={"limit": sensor_limit_per_location, "page": 1},
                headers=headers,
                timeout=60,
            )
            sensors = sensor_payload.get("results", [])
            if not sensors:
                continue

            location_coordinates = location.get("coordinates") or {}
            city_name = (
                location.get("locality")
                or location.get("name")
                or cfg.get("city", "New York")
            )
            country = (location.get("country") or {}).get("code")
            timezone = location.get("timezone")

            for sensor in sensors:
                sensor_id = sensor.get("id")
                parameter = ((sensor.get("parameter") or {}).get("name") or "").lower()
                if not sensor_id:
                    continue
                if parameter_names and parameter and parameter not in parameter_names:
                    continue

                unit = (sensor.get("parameter") or {}).get("units")

                for page in range(1, measurement_pages + 1):
                    measure_payload = get_json(
                        session,
                        f"{endpoint_base}/sensors/{sensor_id}/hours",
                        params={"limit": measurement_limit, "page": page},
                        headers=headers,
                        timeout=60,
                    )
                    measurements = measure_payload.get("results", [])
                    if not measurements:
                        break

                    for item in measurements:
                        period = item.get("period") or {}
                        period_from = period.get("datetimeFrom") or {}
                        period_to = period.get("datetimeTo") or {}
                        datetime_obj = item.get("datetime") or {}
                        coordinates = item.get("coordinates") or location_coordinates

                        rows.append(
                            {
                                "timestamp_utc": datetime_obj.get("utc")
                                or period_to.get("utc")
                                or period_from.get("utc"),
                                "timestamp_local": datetime_obj.get("local")
                                or period_to.get("local")
                                or period_from.get("local"),
                                "parameter": parameter or ((item.get("parameter") or {}).get("name")),
                                "value": item.get("value"),
                                "unit": unit or ((item.get("parameter") or {}).get("units")),
                                "location": location.get("name") or city_name,
                                "location_id": location_id,
                                "city": city_name,
                                "country": country,
                                "timezone": timezone,
                                "sensor_id": sensor_id,
                                "latitude": (coordinates or {}).get("latitude"),
                                "longitude": (coordinates or {}).get("longitude"),
                            }
                        )

                    if len(measurements) < measurement_limit:
                        break

        return rows

    def _cached_artifacts(self, context: PipelineContext) -> dict[str, Any] | None:
        bronze_path = context.data_lake_root / "bronze" / self.name / "openaq_raw.json"
        silver_path = (
            context.data_lake_root / "silver" / self.name / "air_quality_measurements.parquet"
        )
        gold_path = (
            context.data_lake_root / "gold" / self.name / "fact_air_quality_daily.parquet"
        )

        if not gold_path.exists():
            return None

        bronze = [bronze_path] if bronze_path.exists() else []
        silver = [silver_path] if silver_path.exists() else []
        return {"bronze": bronze, "silver": silver, "gold": gold_path}
