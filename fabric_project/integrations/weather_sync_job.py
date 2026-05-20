from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Support direct execution (e.g., `python fabric_project/integrations/weather_sync_job.py`)
# where package imports would otherwise fail without PYTHONPATH.
if __package__ in {None, ""}:
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

from fabric_project.common.logging import configure_logging
from fabric_project.common.settings import load_settings
from fabric_project.pipelines.context import PipelineContext, load_pipeline_config
from fabric_project.pipelines.extensions import EXTENSIONS

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WeatherSyncConfig:
    source_parquet_path: Path
    measurement: str
    time_column: str
    tags: list[str]
    fields: list[str]
    state_file: Path
    schedule_minutes: int
    refresh_weather_before_sync: bool
    pipeline_config_path: Path
    weather_dataset_name: str


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _clean_string_list(values: Any, fallback: list[str]) -> list[str]:
    if values is None:
        return fallback
    if not isinstance(values, list):
        raise ValueError(f"Expected list, got {type(values).__name__}")
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return cleaned or fallback


def load_sync_config(config_path: Path, default_interval_minutes: int) -> WeatherSyncConfig:
    settings = load_settings()
    resolved_config_path = settings.resolve_path(config_path)
    with resolved_config_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg, dict):
        raise ValueError(f"Weather sync config must be a YAML object: {resolved_config_path}")

    schedule_minutes = int(cfg.get("schedule_minutes", default_interval_minutes))
    if schedule_minutes <= 0:
        raise ValueError("schedule_minutes must be > 0")

    return WeatherSyncConfig(
        source_parquet_path=settings.resolve_path(cfg["source_parquet_path"]),
        measurement=str(cfg.get("measurement", "weather_enriched")),
        time_column=str(cfg.get("time_column", "timestamp")),
        tags=_clean_string_list(cfg.get("tags"), ["city", "timezone"]),
        fields=_clean_string_list(
            cfg.get("fields"),
            [
                "temperature_c",
                "apparent_temperature_c",
                "humidity_pct",
                "cloud_cover_pct",
                "precipitation_mm",
                "wind_speed_kmh",
                "comfort_index",
                "weather_code",
                "is_raining",
                "weather_label",
            ],
        ),
        state_file=settings.resolve_path(cfg.get("state_file", ".state/weather_sync_state.json")),
        schedule_minutes=schedule_minutes,
        refresh_weather_before_sync=_as_bool(cfg.get("refresh_weather_before_sync"), default=True),
        pipeline_config_path=settings.resolve_path(cfg.get("pipeline_config_path", "config/datasets.yaml")),
        weather_dataset_name=str(cfg.get("weather_dataset_name", "weather")),
    )


def _load_state(state_file: Path) -> datetime | None:
    if not state_file.exists():
        return None
    with state_file.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    value = payload.get("last_synced_utc")
    if not value:
        return None

    loaded = datetime.fromisoformat(value)
    if loaded.tzinfo is None:
        return loaded.replace(tzinfo=UTC)
    return loaded.astimezone(UTC)


def _save_state(state_file: Path, ts: datetime) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    value = ts if ts.tzinfo is not None else ts.replace(tzinfo=UTC)
    with state_file.open("w", encoding="utf-8") as fh:
        json.dump({"last_synced_utc": value.astimezone(UTC).isoformat()}, fh, indent=2)


def _cast_field(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).isoformat()
    try:
        return float(value)
    except Exception:
        return str(value)


def _refresh_weather_dataset(sync_cfg: WeatherSyncConfig) -> None:
    pipeline_cfg = load_pipeline_config(sync_cfg.pipeline_config_path)
    dataset_cfg = pipeline_cfg.get("datasets", {}).get(sync_cfg.weather_dataset_name, {})
    if dataset_cfg and not bool(dataset_cfg.get("enabled", True)):
        logger.info(
            "Weather dataset '%s' is disabled in pipeline config; skipping refresh",
            sync_cfg.weather_dataset_name,
        )
        return

    extension_cls = EXTENSIONS.get(sync_cfg.weather_dataset_name)
    if extension_cls is None:
        raise ValueError(
            f"Dataset extension '{sync_cfg.weather_dataset_name}' not found. "
            "Ensure the weather extension module is available."
        )

    settings = load_settings()
    context = PipelineContext(config=pipeline_cfg, data_lake_root=settings.data_lake_root)
    result = extension_cls().run(context, dataset_cfg)
    logger.info("Refreshed weather dataset before sync: %s", result)


def _validate_influx_settings() -> None:
    settings = load_settings()
    token = settings.influxdb_token.strip()
    if not token:
        raise ValueError(
            "INFLUXDB_TOKEN is missing. Set it in .env or environment variables before sync."
        )
    if token == "replace_with_real_token":
        logger.warning(
            "INFLUXDB_TOKEN is using the default local value. "
            "This is acceptable for local demo/dev setups, but rotate it for shared environments."
        )


def _iter_point_batches(points: list[Point], batch_size: int = 5000) -> list[list[Point]]:
    return [points[i : i + batch_size] for i in range(0, len(points), batch_size)]


def _influx_has_measurement_data(sync_cfg: WeatherSyncConfig) -> bool:
    settings = load_settings()
    query = (
        f'from(bucket: "{settings.influxdb_bucket}") '
        '|> range(start: -3650d) '
        f'|> filter(fn: (r) => r._measurement == "{sync_cfg.measurement}") '
        "|> keep(columns: [\"_time\"]) "
        "|> limit(n: 1)"
    )
    with InfluxDBClient(
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
    ) as client:
        tables = client.query_api().query(query)
    return any(table.records for table in tables)


def run_once(
    sync_cfg: WeatherSyncConfig,
    *,
    skip_refresh: bool = False,
    dry_run: bool = False,
) -> None:
    if sync_cfg.refresh_weather_before_sync and not skip_refresh:
        _refresh_weather_dataset(sync_cfg)

    settings = load_settings()
    if not sync_cfg.source_parquet_path.exists():
        raise FileNotFoundError(
            f"Weather source not found: {sync_cfg.source_parquet_path}. "
            "Run the pipeline first to materialize silver/gold weather data."
        )

    df = pd.read_parquet(sync_cfg.source_parquet_path)
    if sync_cfg.time_column not in df.columns:
        raise ValueError(f"Expected time column '{sync_cfg.time_column}' in weather dataset")

    df[sync_cfg.time_column] = pd.to_datetime(df[sync_cfg.time_column], utc=True, errors="coerce")
    df = (
        df.dropna(subset=[sync_cfg.time_column])
        .drop_duplicates(subset=[sync_cfg.time_column, *[c for c in sync_cfg.tags if c in df.columns]])
        .sort_values(sync_cfg.time_column)
    )

    last_synced = _load_state(sync_cfg.state_file)
    if last_synced is not None and not dry_run:
        try:
            has_destination_data = _influx_has_measurement_data(sync_cfg)
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            logger.warning(
                "Failed to verify destination data before incremental sync "
                "(%s: %s). Continuing with state checkpoint.",
                type(exc).__name__,
                exc,
            )
            has_destination_data = True

        # If destination bucket was recreated/emptied, ignore checkpoint and replay all rows.
        if not has_destination_data:
            logger.warning(
                "Destination bucket has no '%s' points but checkpoint exists at %s. "
                "Forcing full backfill.",
                sync_cfg.measurement,
                last_synced.isoformat(),
            )
            last_synced = None

    if last_synced is not None:
        df = df[df[sync_cfg.time_column] > last_synced]

    if df.empty:
        logger.info("No new weather rows to sync")
        return

    available_fields = [f for f in sync_cfg.fields if f in df.columns]
    available_tags = [t for t in sync_cfg.tags if t in df.columns]
    if not available_fields:
        raise ValueError(
            "None of the configured fields are present in source dataset. "
            f"Configured fields: {sync_cfg.fields}"
        )

    if dry_run:
        logger.info(
            "Dry run enabled. Would sync %d weather rows to InfluxDB (fields=%s tags=%s)",
            len(df),
            ",".join(available_fields),
            ",".join(available_tags),
        )
        return

    _validate_influx_settings()

    points: list[Point] = []
    for _, row in df.iterrows():
        ts = row[sync_cfg.time_column]
        point = Point(sync_cfg.measurement).time(ts.to_pydatetime(), WritePrecision.S)

        for tag in available_tags:
            tag_value = row.get(tag)
            if pd.notna(tag_value):
                point = point.tag(tag, str(tag_value))

        fields_written = 0
        for field in available_fields:
            field_value = _cast_field(row.get(field))
            if field_value is None:
                continue
            fields_written += 1
            if isinstance(field_value, str):
                point = point.field(field, field_value)
            elif isinstance(field_value, bool):
                point = point.field(field, field_value)
            elif isinstance(field_value, int):
                point = point.field(field, int(field_value))
            else:
                point = point.field(field, float(field_value))

        if fields_written > 0:
            points.append(point)

    if not points:
        logger.info("No points with valid fields were generated; nothing to sync")
        return

    with InfluxDBClient(
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
    ) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for batch in _iter_point_batches(points):
            write_api.write(
                bucket=settings.influxdb_bucket,
                org=settings.influxdb_org,
                record=batch,
            )

    max_synced_ts = pd.Timestamp(df[sync_cfg.time_column].max()).tz_convert("UTC")
    _save_state(sync_cfg.state_file, max_synced_ts.to_pydatetime())
    logger.info("Synced %d weather rows to InfluxDB", len(points))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync enriched weather data into a time-series database"
    )
    parser.add_argument(
        "--config", default="config/weather_sync.yaml", help="Path to sync config"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single sync cycle and exit",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip weather dataset refresh before syncing to InfluxDB",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to InfluxDB; only validate and count candidate rows",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignore incremental checkpoint by deleting weather sync state before run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    settings = load_settings()
    sync_cfg = load_sync_config(settings.resolve_path(args.config), settings.weather_sync_interval_minutes)

    if args.reset_state and sync_cfg.state_file.exists():
        sync_cfg.state_file.unlink()
        logger.info("Deleted sync state file: %s", sync_cfg.state_file)

    if args.once:
        run_once(sync_cfg, skip_refresh=args.skip_refresh, dry_run=args.dry_run)
        return

    sleep_seconds = max(60, sync_cfg.schedule_minutes * 60)
    while True:
        try:
            run_once(sync_cfg, skip_refresh=args.skip_refresh, dry_run=args.dry_run)
        except Exception:
            logger.exception("Weather sync run failed")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
