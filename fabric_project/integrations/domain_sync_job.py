from __future__ import annotations

import argparse
import logging
import numbers
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Support direct execution (e.g., `python fabric_project/integrations/domain_sync_job.py`)
if __package__ in {None, ""}:
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

from fabric_project.common.logging import configure_logging
from fabric_project.common.settings import load_settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DomainDatasetSyncSpec:
    name: str
    source_parquet_path: Path
    measurement: str
    time_column: str
    tags: list[str]
    fields: list[str]


@dataclass(slots=True)
class DomainSyncConfig:
    datasets: list[DomainDatasetSyncSpec]
    batch_size: int


def _clean_string_list(values: Any, fallback: list[str]) -> list[str]:
    if values is None:
        return fallback
    if not isinstance(values, list):
        raise ValueError(f"Expected list, got {type(values).__name__}")
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return cleaned or fallback


def _cast_field(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return str(value)


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


def load_sync_config(config_path: Path) -> DomainSyncConfig:
    settings = load_settings()
    resolved_config_path = settings.resolve_path(config_path)
    with resolved_config_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg, dict):
        raise ValueError(f"Domain sync config must be a YAML object: {resolved_config_path}")

    raw_datasets = cfg.get("datasets")
    if not isinstance(raw_datasets, dict) or not raw_datasets:
        raise ValueError("config.domain_sync.datasets must be a non-empty object")

    specs: list[DomainDatasetSyncSpec] = []
    for dataset_name, dataset_cfg in raw_datasets.items():
        if not isinstance(dataset_cfg, dict):
            raise ValueError(f"Dataset config must be an object: {dataset_name}")

        source_path = settings.resolve_path(dataset_cfg["source_parquet_path"])
        specs.append(
            DomainDatasetSyncSpec(
                name=str(dataset_name),
                source_parquet_path=source_path,
                measurement=str(dataset_cfg.get("measurement", dataset_name)),
                time_column=str(dataset_cfg.get("time_column", "timestamp")),
                tags=_clean_string_list(dataset_cfg.get("tags"), []),
                fields=_clean_string_list(dataset_cfg.get("fields"), []),
            )
        )

    batch_size = int(cfg.get("batch_size", 5000))
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    return DomainSyncConfig(datasets=specs, batch_size=batch_size)


def _iter_point_batches(points: list[Point], batch_size: int) -> list[list[Point]]:
    return [points[i : i + batch_size] for i in range(0, len(points), batch_size)]


def _sync_dataset(
    client: InfluxDBClient,
    spec: DomainDatasetSyncSpec,
    *,
    bucket: str,
    org: str,
    batch_size: int,
    dry_run: bool,
) -> None:
    if not spec.source_parquet_path.exists():
        logger.warning("Skipping %s: source file missing (%s)", spec.name, spec.source_parquet_path)
        return

    df = pd.read_parquet(spec.source_parquet_path)
    if df.empty:
        logger.warning("Skipping %s: source parquet is empty", spec.name)
        return

    if spec.time_column not in df.columns:
        raise ValueError(
            f"Dataset '{spec.name}' is missing configured time column '{spec.time_column}'"
        )

    if not spec.fields:
        raise ValueError(f"Dataset '{spec.name}' has no configured fields to sync")

    df["_sync_time_utc"] = pd.to_datetime(df[spec.time_column], utc=True, errors="coerce")
    df = df.dropna(subset=["_sync_time_utc"]).sort_values("_sync_time_utc")

    available_fields = [field for field in spec.fields if field in df.columns]
    available_tags = [tag for tag in spec.tags if tag in df.columns]
    if not available_fields:
        raise ValueError(
            f"Dataset '{spec.name}' has none of the configured fields in source parquet. "
            f"Configured={spec.fields}"
        )

    points: list[Point] = []
    for _, row in df.iterrows():
        point = Point(spec.measurement).time(row["_sync_time_utc"].to_pydatetime(), WritePrecision.S)

        for tag in available_tags:
            tag_value = row.get(tag)
            if pd.notna(tag_value):
                point = point.tag(tag, str(tag_value))

        fields_written = 0
        for field in available_fields:
            value = _cast_field(row.get(field))
            if value is None:
                continue
            fields_written += 1
            point = point.field(field, value)

        if fields_written:
            points.append(point)

    if not points:
        logger.warning("Skipping %s: no valid points generated", spec.name)
        return

    if dry_run:
        logger.info(
            "Dry run: would sync dataset=%s measurement=%s points=%d tags=%s fields=%s",
            spec.name,
            spec.measurement,
            len(points),
            ",".join(available_tags),
            ",".join(available_fields),
        )
        return

    write_api = client.write_api(write_options=SYNCHRONOUS)
    for batch in _iter_point_batches(points, batch_size):
        write_api.write(bucket=bucket, org=org, record=batch)

    logger.info(
        "Synced dataset=%s measurement=%s points=%d",
        spec.name,
        spec.measurement,
        len(points),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync cross-domain gold datasets (taxi/economy/air-quality) into InfluxDB for Grafana"
        )
    )
    parser.add_argument(
        "--config",
        default="config/domain_sync.yaml",
        help="Path to domain sync config",
    )
    parser.add_argument(
        "--datasets",
        default="",
        help="Comma-separated dataset names to sync (default: all configured datasets)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate sources and print sync counts without writing to InfluxDB",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    _validate_influx_settings()

    sync_cfg = load_sync_config(Path(args.config))
    selected_names = {
        item.strip() for item in args.datasets.split(",") if item.strip()
    } if args.datasets else set()

    settings = load_settings()
    with InfluxDBClient(
        url=settings.influxdb_url,
        token=settings.influxdb_token,
        org=settings.influxdb_org,
        timeout=60_000,
    ) as client:
        for spec in sync_cfg.datasets:
            if selected_names and spec.name not in selected_names:
                continue
            _sync_dataset(
                client,
                spec,
                bucket=settings.influxdb_bucket,
                org=settings.influxdb_org,
                batch_size=sync_cfg.batch_size,
                dry_run=args.dry_run,
            )


if __name__ == "__main__":
    main()
