from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from fabric_project.quality.runner import generate_quality_report


class TestQualityRunner(unittest.TestCase):
    def test_missing_column_expectation_is_reported_without_crash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            table_path = tmp_path / "sample.parquet"
            config_path = tmp_path / "quality.yaml"

            pd.DataFrame({"known_col": [1, 2, 3]}).to_parquet(table_path, index=False)
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "allow_missing_tables": False,
                        "tables": [
                            {
                                "name": "sample",
                                "path": str(table_path),
                                "expectations": [
                                    {"type": "not_null", "column": "missing_col"}
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = generate_quality_report(config_path=config_path)

            self.assertFalse(report.overall_success)
            self.assertEqual(report.table_count, 1)
            self.assertEqual(report.failed_expectation_count, 1)
            first_result = report.details[0]["expectations"][0]
            self.assertFalse(first_result["success"])
            self.assertIn("error", first_result)
            self.assertTrue(report.markdown_report_path.exists())
            self.assertTrue(report.json_report_path.exists())

    def test_optional_missing_table_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            config_path = tmp_path / "quality.yaml"

            config_path.write_text(
                yaml.safe_dump(
                    {
                        "allow_missing_tables": True,
                        "tables": [
                            {
                                "name": "missing_optional",
                                "path": str(tmp_path / "does_not_exist.parquet"),
                                "required": False,
                                "expectations": [
                                    {"type": "not_null", "column": "x"},
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = generate_quality_report(config_path=config_path)

            self.assertTrue(report.overall_success)
            self.assertEqual(report.table_count, 0)
            self.assertEqual(report.failed_table_count, 0)
            self.assertEqual(report.expectation_count, 0)
            self.assertEqual(report.missing_required_table_count, 0)
            self.assertEqual(report.details[0]["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
