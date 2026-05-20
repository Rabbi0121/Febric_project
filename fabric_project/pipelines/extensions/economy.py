from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from fabric_project.common.http import build_http_session
from fabric_project.pipelines.context import PipelineContext
from fabric_project.pipelines.extensions.base import DatasetExtension

logger = logging.getLogger(__name__)


class EconomyExtension(DatasetExtension):
    name = "economy"

    def extract_bronze(self, context: PipelineContext, cfg: dict[str, Any]) -> list[Path]:
        bronze_dir = context.ensure_layer_dir("bronze", self.name)
        gdp_path = bronze_dir / "gdp_world_bank_raw.json"
        fx_path = bronze_dir / "fx_ecb_raw.csv"

        country = cfg.get("world_bank_country", "USA")
        indicator = cfg.get("world_bank_indicator", "NY.GDP.MKTP.CD")
        world_bank_url = (
            f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json"
        )

        session = build_http_session()
        try:
            gdp_response = session.get(world_bank_url, timeout=60)
            gdp_response.raise_for_status()
            with gdp_path.open("w", encoding="utf-8") as fh:
                json.dump(gdp_response.json(), fh, indent=2)

            fx_url = cfg.get(
                "ecb_fx_url",
                "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata",
            )
            fx_response = session.get(fx_url, timeout=60)
            fx_response.raise_for_status()
            with fx_path.open("w", encoding="utf-8") as fh:
                fh.write(fx_response.text)
            return [gdp_path, fx_path]
        except Exception as exc:
            allow_cached_fallback = bool(cfg.get("allow_cached_fallback", True))
            if allow_cached_fallback and gdp_path.exists() and fx_path.exists():
                logger.warning(
                    "Economy API fetch failed (%s: %s); using cached bronze fallback files: %s, %s",
                    type(exc).__name__,
                    exc,
                    gdp_path,
                    fx_path,
                )
                return [gdp_path, fx_path]
            raise

    def transform_silver(
        self, context: PipelineContext, cfg: dict[str, Any], bronze_output: list[Path]
    ) -> list[Path]:
        silver_dir = context.ensure_layer_dir("silver", self.name)
        gdp_silver = silver_dir / "gdp.parquet"
        fx_silver = silver_dir / "fx_rates.parquet"

        gdp_path, fx_path = bronze_output

        with gdp_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)

        data_rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        gdp_rows = []
        for row in data_rows:
            gdp_value = row.get("value")
            year = row.get("date")
            if gdp_value is None or year is None:
                continue
            gdp_rows.append(
                {
                    "year": int(year),
                    "country": row.get("country", {}).get("value"),
                    "gdp_usd": float(gdp_value),
                }
            )

        gdp_df = pd.DataFrame(gdp_rows).sort_values("year")
        if gdp_df.empty:
            raise ValueError("World Bank GDP dataset produced no rows")
        gdp_df.to_parquet(gdp_silver, index=False)

        fx_df = pd.read_csv(fx_path)
        date_column = next(
            (c for c in ["TIME_PERIOD", "DATE", "date", "Date"] if c in fx_df.columns),
            None,
        )
        value_column = next(
            (c for c in ["OBS_VALUE", "value", "Value"] if c in fx_df.columns),
            None,
        )
        if not date_column or not value_column:
            raise ValueError("Unable to detect FX date/value columns in ECB dataset")

        fx_df = fx_df.rename(
            columns={date_column: "fx_date", value_column: "usd_to_eur_rate"}
        )
        fx_df = fx_df[["fx_date", "usd_to_eur_rate"]].copy()
        fx_df["fx_date"] = pd.to_datetime(fx_df["fx_date"], errors="coerce")
        fx_df["year"] = fx_df["fx_date"].dt.year
        fx_df = fx_df.dropna(subset=["fx_date", "usd_to_eur_rate", "year"])

        fx_df.to_parquet(fx_silver, index=False)
        return [gdp_silver, fx_silver]

    def publish_gold(
        self, context: PipelineContext, cfg: dict[str, Any], silver_output: list[Path]
    ) -> Path:
        gold_dir = context.ensure_layer_dir("gold", self.name)
        gold_path = gold_dir / "fact_fx_gdp_daily.parquet"

        gdp_path, fx_path = silver_output
        gdp_df = pd.read_parquet(gdp_path)
        fx_df = pd.read_parquet(fx_path)

        merged = fx_df.merge(gdp_df, on="year", how="left")
        merged["gdp_eur_approx"] = merged["gdp_usd"] * merged["usd_to_eur_rate"]

        merged.to_parquet(gold_path, index=False)
        return gold_path
