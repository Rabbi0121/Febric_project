from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import traceback

from fabric_project.common.settings import load_settings
from fabric_project.quality.runner import generate_quality_report


@dataclass(slots=True)
class BotQualityResponse:
    summary: str
    markdown_report: Path
    json_report: Path



def run_quality_report_for_bot() -> BotQualityResponse:
    settings = load_settings()
    try:
        report = generate_quality_report(config_path=settings.quality_checks_config)
    except Exception as exc:
        error_summary = "\n".join(
            [
                "Data quality run: FAILED",
                f"Reason: {type(exc).__name__}: {exc}",
                "Please inspect runner logs or run ./scripts/run_quality_checks.sh locally for stack trace.",
            ]
        )
        fallback = settings.resolve_path("reports/quality/quality_report_error.md")
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(traceback.format_exc(), encoding="utf-8")
        return BotQualityResponse(
            summary=error_summary,
            markdown_report=fallback,
            json_report=fallback,
        )

    lines = [
        f"Data quality run: {'PASSED' if report.overall_success else 'FAILED'}",
        (
            f"Checked tables: {report.table_count}, failed tables: {report.failed_table_count}, "
            f"expectations: {report.expectation_count}, failures: {report.failed_expectation_count}, "
            f"missing required tables: {report.missing_required_table_count}"
        ),
    ]

    if not report.overall_success:
        missing_required = [
            row["table"] for row in report.details if row.get("status") == "failed_missing"
        ]
        if missing_required:
            lines.append("Missing required tables:")
            for table_name in missing_required[:5]:
                lines.append(f"- {table_name}")

        lines.append("Top failed checks:")
        shown = 0
        for table in report.details:
            if table.get("status") != "checked":
                continue
            for result in table.get("expectations", []):
                if result.get("success", False):
                    continue
                expectation = result.get("expectation", {})
                lines.append(
                    f"- {table.get('table')}: {expectation.get('type')}({expectation.get('column', '-')})"
                )
                shown += 1
                if shown >= 5:
                    break
            if shown >= 5:
                break
        if shown == 0:
            lines.append("- No expectation-level failures in checked tables")

    summary = "\n".join(lines)
    return BotQualityResponse(
        summary=summary,
        markdown_report=report.markdown_report_path,
        json_report=report.json_report_path,
    )
