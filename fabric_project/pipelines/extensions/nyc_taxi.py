from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fabric_project.common.http import build_http_session, download_to_file
from fabric_project.pipelines.context import PipelineContext
from fabric_project.pipelines.extensions.base import DatasetExtension


class NYCTaxiExtension(DatasetExtension):
    name = "nyc_taxi"

    def extract_bronze(self, context: PipelineContext, cfg: dict[str, Any]) -> list[Path]:
        bronze_dir = context.ensure_layer_dir("bronze", self.name)
        files: list[Path] = []
        source_urls = cfg.get("source_urls", [])
        redownload_existing = bool(cfg.get("redownload_existing", False))

        if not source_urls:
            raise ValueError("No NYC Taxi source URLs configured")

        session = build_http_session()
        for url in source_urls:
            filename = url.rstrip("/").split("/")[-1]
            target = bronze_dir / filename
            if target.exists() and not redownload_existing:
                files.append(target)
                continue

            download_to_file(session, url, target, timeout=180)
            files.append(target)

        return files

    def transform_silver(
        self, context: PipelineContext, cfg: dict[str, Any], bronze_output: list[Path]
    ) -> Path:
        silver_dir = context.ensure_layer_dir("silver", self.name)
        silver_path = silver_dir / "taxi_trips_clean.parquet"

        selected_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]
        max_rows = cfg.get("max_rows")

        frames = []
        for parquet_file in bronze_output:
            frame = pd.read_parquet(parquet_file)
            existing_columns = [c for c in selected_columns if c in frame.columns]
            frame = frame[existing_columns].copy()

            for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                if col in frame.columns:
                    frame[col] = pd.to_datetime(frame[col], errors="coerce", utc=True)

            if "fare_amount" in frame.columns:
                frame = frame[frame["fare_amount"].fillna(0) >= 0]

            frame = frame.drop_duplicates()
            frames.append(frame)

        merged = pd.concat(frames, ignore_index=True)
        if isinstance(max_rows, int) and max_rows > 0:
            merged = merged.head(max_rows)

        merged.to_parquet(silver_path, index=False)
        return silver_path

    def publish_gold(
        self, context: PipelineContext, cfg: dict[str, Any], silver_output: Path
    ) -> Path:
        gold_dir = context.ensure_layer_dir("gold", self.name)
        gold_path = gold_dir / "fact_taxi_daily.parquet"

        df = pd.read_parquet(silver_output)
        if "tpep_pickup_datetime" not in df.columns:
            raise ValueError("Expected tpep_pickup_datetime in silver taxi dataset")

        df["pickup_date"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.date
        if "passenger_count" not in df.columns:
            df["passenger_count"] = 0
        if "total_amount" not in df.columns:
            df["total_amount"] = 0
        if "trip_distance" not in df.columns:
            df["trip_distance"] = 0

        fact = (
            df.groupby(["pickup_date", "PULocationID"], dropna=False, as_index=False)
            .agg(
                trip_count=("tpep_pickup_datetime", "count"),
                passenger_total=("passenger_count", "sum"),
                revenue_usd=("total_amount", "sum"),
                avg_trip_distance_miles=("trip_distance", "mean"),
            )
            .sort_values(["pickup_date", "PULocationID"])
        )

        fact.to_parquet(gold_path, index=False)
        return gold_path
