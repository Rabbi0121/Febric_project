from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fabric_project.pipelines.context import PipelineContext
from fabric_project.pipelines.extensions.economy import EconomyExtension
from fabric_project.pipelines.extensions.openaq import OpenAQExtension
from fabric_project.pipelines.extensions.weather import WeatherExtension


class _FailingSession:
    def get(self, *_args, **_kwargs):  # pragma: no cover - simple failure stub
        raise RuntimeError("network down")


class TestExtensionResilience(unittest.TestCase):
    def test_economy_uses_cached_bronze_when_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bronze_dir = root / "bronze" / "economy"
            bronze_dir.mkdir(parents=True, exist_ok=True)

            gdp = bronze_dir / "gdp_world_bank_raw.json"
            fx = bronze_dir / "fx_ecb_raw.csv"
            gdp.write_text(json.dumps([{}, []]), encoding="utf-8")
            fx.write_text("TIME_PERIOD,OBS_VALUE\n2026-01-01,0.92\n", encoding="utf-8")

            context = PipelineContext(config={}, data_lake_root=root)
            extension = EconomyExtension()

            with patch(
                "fabric_project.pipelines.extensions.economy.build_http_session",
                return_value=_FailingSession(),
            ):
                outputs = extension.extract_bronze(
                    context,
                    {
                        "allow_cached_fallback": True,
                        "world_bank_country": "USA",
                        "world_bank_indicator": "NY.GDP.MKTP.CD",
                    },
                )

            self.assertEqual(outputs, [gdp, fx])

    def test_openaq_uses_cached_artifacts_when_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bronze_dir = root / "bronze" / "openaq"
            silver_dir = root / "silver" / "openaq"
            gold_dir = root / "gold" / "openaq"
            bronze_dir.mkdir(parents=True, exist_ok=True)
            silver_dir.mkdir(parents=True, exist_ok=True)
            gold_dir.mkdir(parents=True, exist_ok=True)

            bronze = bronze_dir / "openaq_raw.json"
            silver = silver_dir / "air_quality_measurements.parquet"
            gold = gold_dir / "fact_air_quality_daily.parquet"

            bronze.write_text(json.dumps({"results": []}), encoding="utf-8")
            silver.write_text("not used in fallback test", encoding="utf-8")
            gold.write_text("not used in fallback test", encoding="utf-8")

            context = PipelineContext(config={}, data_lake_root=root)
            extension = OpenAQExtension()

            with patch.object(extension, "_fetch_v3_rows", side_effect=RuntimeError("network down")):
                result = extension.run(context, {"allow_cached_fallback": True})

            self.assertEqual(result["dataset"], "openaq")
            self.assertEqual(result["gold"], str(gold))
            self.assertIn(str(bronze), result["bronze"])
            self.assertIn(str(silver), result["silver"])

    def test_weather_uses_latest_cached_bronze_when_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bronze_dir = root / "bronze" / "weather"
            bronze_dir.mkdir(parents=True, exist_ok=True)

            cached = bronze_dir / "weather_raw_20260101T000000Z.json"
            cached.write_text(
                json.dumps({"hourly": {"time": ["2026-01-01T00:00"]}}),
                encoding="utf-8",
            )

            context = PipelineContext(config={}, data_lake_root=root)
            extension = WeatherExtension()

            with patch(
                "fabric_project.pipelines.extensions.weather.get_json",
                side_effect=RuntimeError("network down"),
            ):
                output = extension.extract_bronze(
                    context,
                    {
                        "allow_cached_fallback": True,
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                        "timezone": "America/New_York",
                    },
                )

            self.assertEqual(output, cached)


if __name__ == "__main__":
    unittest.main()
