from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
import yaml

# Support direct execution (e.g., `python fabric_project/quality/runner.py`)
# where package imports would otherwise fail without PYTHONPATH.
if __package__ in {None, ""}:
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

from fabric_project.common.settings import load_settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class QualityReport:
    generated_at: str
    overall_success: bool
    table_count: int
    failed_table_count: int
    expectation_count: int
    failed_expectation_count: int
    missing_required_table_count: int
    details: list[dict[str, Any]]
    markdown_report_path: Path
    json_report_path: Path


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Quality config must be a YAML object: {path}")
    return payload


def _load_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    raise ValueError(f"Unsupported table format: {path}")


_EXPECTATIONS_WITH_COLUMN = {
    "not_null",
    "between",
    "in_set",
    "unique",
    "quantile_between",
    "mean_between",
}


def _run_expectation(gdf: Any, expectation: dict[str, Any]) -> dict[str, Any]:
    exp_type = str(expectation.get("type", "")).strip()
    if not exp_type:
        return {
            "expectation": expectation,
            "success": False,
            "result": {},
            "error": "Expectation is missing required field: type",
        }

    if exp_type in _EXPECTATIONS_WITH_COLUMN:
        column = expectation.get("column")
        if not column:
            return {
                "expectation": expectation,
                "success": False,
                "result": {},
                "error": f"Expectation '{exp_type}' requires a non-empty 'column'",
            }

    try:
        if exp_type == "not_null":
            result = gdf.expect_column_values_to_not_be_null(expectation["column"])
        elif exp_type == "between":
            result = gdf.expect_column_values_to_be_between(
                expectation["column"],
                min_value=expectation.get("min"),
                max_value=expectation.get("max"),
                mostly=expectation.get("mostly", 1.0),
            )
        elif exp_type == "in_set":
            result = gdf.expect_column_values_to_be_in_set(
                expectation["column"],
                value_set=expectation.get("value_set", []),
                mostly=expectation.get("mostly", 1.0),
            )
        elif exp_type == "unique":
            result = gdf.expect_column_values_to_be_unique(
                expectation["column"],
                mostly=expectation.get("mostly", 1.0),
            )
        elif exp_type == "quantile_between":
            result = gdf.expect_column_quantile_values_to_be_between(
                expectation["column"],
                quantile_ranges={
                    "quantiles": expectation["quantiles"],
                    "value_ranges": expectation["value_ranges"],
                },
            )
        elif exp_type == "mean_between":
            result = gdf.expect_column_mean_to_be_between(
                expectation["column"],
                min_value=expectation.get("min"),
                max_value=expectation.get("max"),
            )
        else:
            raise ValueError(f"Unsupported expectation type: {exp_type}")

        result_dict = result.to_json_dict()
        return {
            "expectation": expectation,
            "success": bool(result_dict.get("success", False)),
            "result": result_dict,
            "error": "",
        }
    except Exception as exc:  # pragma: no cover - defensive guard for fragile checks
        logger.warning(
            "Expectation execution failed for %s(%s): %s",
            exp_type,
            expectation.get("column", "-"),
            exc,
        )
        return {
            "expectation": expectation,
            "success": False,
            "result": {},
            "error": f"{type(exc).__name__}: {exc}",
        }


def _resolve_default_config_path() -> Path:
    return load_settings().quality_checks_config


def _is_required_table(table_cfg: dict[str, Any], allow_missing_default: bool) -> bool:
    return bool(table_cfg.get("required", not allow_missing_default))


def _resolve_table_path(raw_path: str | Path) -> Path:
    settings = load_settings()
    return settings.resolve_path(raw_path)


def generate_quality_report(config_path: str | Path | None = None) -> QualityReport:
    settings = load_settings()
    config_path = (
        settings.resolve_path(config_path)
        if config_path is not None
        else _resolve_default_config_path()
    )
    config = _load_yaml(config_path)
    report_rows: list[dict[str, Any]] = []

    expectation_count = 0
    failed_expectation_count = 0
    missing_required_table_count = 0
    failed_table_count = 0
    checked_tables = 0

    allow_missing = bool(config.get("allow_missing_tables", False))

    for idx, table_cfg in enumerate(config.get("tables", []), start=1):
        table_name = str(table_cfg.get("name", f"table_{idx}"))
        raw_table_path = table_cfg.get("path")
        if not raw_table_path:
            failed_table_count += 1
            report_rows.append(
                {
                    "table": table_name,
                    "path": "",
                    "required": True,
                    "status": "failed_config",
                    "reason": "table path is missing",
                    "expectations": [],
                }
            )
            continue
        table_path = _resolve_table_path(raw_table_path)
        table_required = _is_required_table(table_cfg, allow_missing_default=allow_missing)

        if not table_path.exists():
            status = "failed_missing" if table_required else "skipped"
            if table_required:
                missing_required_table_count += 1
                failed_table_count += 1
            report_rows.append(
                {
                    "table": table_name,
                    "path": str(table_path),
                    "required": table_required,
                    "status": status,
                    "reason": "table_not_found",
                    "expectations": [],
                }
            )
            continue

        try:
            df = _load_table(table_path)
            gdf = gx.from_pandas(df)
        except Exception as exc:
            failed_table_count += 1
            report_rows.append(
                {
                    "table": table_name,
                    "path": str(table_path),
                    "required": table_required,
                    "status": "failed_load",
                    "reason": f"{type(exc).__name__}: {exc}",
                    "expectations": [],
                }
            )
            continue

        checked_tables += 1
        table_results = []
        for expectation in table_cfg.get("expectations", []):
            expectation_count += 1
            result = _run_expectation(gdf, expectation)
            if not result["success"]:
                failed_expectation_count += 1
            table_results.append(result)

        report_rows.append(
            {
                "table": table_name,
                "path": str(table_path),
                "required": table_required,
                "status": "checked",
                "row_count": len(df),
                "expectations": table_results,
            }
        )

    generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    overall_success = (
        failed_expectation_count == 0
        and missing_required_table_count == 0
        and failed_table_count == 0
    )

    reports_dir = settings.resolve_path("reports/quality")
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    markdown_path = reports_dir / f"quality_report_{timestamp}.md"
    json_path = reports_dir / f"quality_report_{timestamp}.json"

    markdown_path.write_text(
        _as_markdown(
            generated_at,
            overall_success,
            checked_tables,
            failed_table_count,
            expectation_count,
            failed_expectation_count,
            missing_required_table_count,
            report_rows,
        ),
        encoding="utf-8",
    )
    json_payload = {
        "generated_at": generated_at,
        "config_path": str(config_path),
        "overall_success": overall_success,
        "checked_tables": checked_tables,
        "failed_table_count": failed_table_count,
        "expectation_count": expectation_count,
        "failed_expectation_count": failed_expectation_count,
        "missing_required_table_count": missing_required_table_count,
        "tables": report_rows,
    }
    json_path.write_text(json.dumps(json_payload, indent=2, default=str), encoding="utf-8")

    return QualityReport(
        generated_at=generated_at,
        overall_success=overall_success,
        table_count=checked_tables,
        failed_table_count=failed_table_count,
        expectation_count=expectation_count,
        failed_expectation_count=failed_expectation_count,
        missing_required_table_count=missing_required_table_count,
        details=report_rows,
        markdown_report_path=markdown_path,
        json_report_path=json_path,
    )


def _as_markdown(
    generated_at: str,
    overall_success: bool,
    checked_tables: int,
    failed_table_count: int,
    expectation_count: int,
    failed_expectation_count: int,
    missing_required_table_count: int,
    report_rows: list[dict[str, Any]],
) -> str:
    lines = [
        "# Data Quality Report",
        "",
        f"- Generated at (UTC): {generated_at}",
        f"- Overall status: {'PASSED' if overall_success else 'FAILED'}",
        f"- Tables checked: {checked_tables}",
        f"- Failed tables: {failed_table_count}",
        f"- Expectations executed: {expectation_count}",
        f"- Failed expectations: {failed_expectation_count}",
        f"- Missing required tables: {missing_required_table_count}",
        "",
    ]

    for table in report_rows:
        lines.append(f"## Table: {table['table']}")
        lines.append(f"- Path: `{table['path']}`")
        lines.append(f"- Required: `{table.get('required', True)}`")
        lines.append(f"- Status: `{table['status']}`")

        if table["status"] != "checked":
            lines.append(f"- Reason: `{table.get('reason', 'n/a')}`")
            lines.append("")
            continue

        lines.append(f"- Rows: {table['row_count']}")

        for result in table["expectations"]:
            exp = result["expectation"]
            label = f"{exp.get('type', 'unknown')}({exp.get('column', '-')})"
            status = "PASS" if result["success"] else "FAIL"
            lines.append(f"- {label}: {status}")

            if not result["success"]:
                if result.get("error"):
                    lines.append(f"  - error: {result['error']}")
                details = result["result"].get("result", {})
                unexpected_pct = details.get("unexpected_percent")
                if unexpected_pct is not None:
                    lines.append(f"  - unexpected_percent: {unexpected_pct}")

        lines.append("")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Great Expectations quality checks")
    parser.add_argument(
        "--config",
        default="",
        help="Path to quality checks YAML. Defaults to QUALITY_CHECKS_CONFIG from environment.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config) if args.config else None
    report = generate_quality_report(config_path=config_path)
    print(
        f"Data quality {'PASSED' if report.overall_success else 'FAILED'} | "
        f"tables={report.table_count}, failed_tables={report.failed_table_count}, "
        f"expectations={report.expectation_count}, "
        f"failures={report.failed_expectation_count}, "
        f"missing_required_tables={report.missing_required_table_count}"
    )
    print(f"Markdown report: {report.markdown_report_path}")
    print(f"JSON report: {report.json_report_path}")
    if not report.overall_success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
