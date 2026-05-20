from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

# Support direct execution (e.g., `python fabric_project/pipelines/runner.py`)
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



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Fabric medallion pipelines with extension-friendly modules"
    )
    parser.add_argument(
        "--config",
        default="config/datasets.yaml",
        help="Path to dataset pipeline configuration YAML",
    )
    parser.add_argument(
        "--datasets",
        default="",
        help="Comma-separated dataset names to run. Defaults to all enabled datasets",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately if one dataset fails",
    )
    parser.add_argument(
        "--allow-partial-success",
        action="store_true",
        help="Return success exit code even if one or more datasets fail",
    )
    return parser.parse_args()



def _resolve_dataset_order(config: dict[str, Any], cli_datasets: str) -> list[str]:
    if cli_datasets.strip():
        requested = [item.strip() for item in cli_datasets.split(",") if item.strip()]
        known = set(config.get("datasets", {}).keys())
        unknown = sorted({name for name in requested if name not in known})
        if unknown:
            raise ValueError(
                f"Unknown dataset(s) in --datasets: {', '.join(unknown)}. "
                f"Known values: {', '.join(sorted(known))}"
            )
        return requested

    order = config.get("default_order", [])
    datasets_cfg = config.get("datasets", {})
    return [name for name in order if datasets_cfg.get(name, {}).get("enabled", False)]



def _is_required_dataset(dataset_cfg: dict[str, Any]) -> bool:
    return bool(dataset_cfg.get("required", True))


def main() -> None:
    args = parse_args()
    configure_logging()
    settings = load_settings()

    config_path = settings.resolve_path(args.config)
    pipeline_cfg = load_pipeline_config(config_path)
    context = PipelineContext(config=pipeline_cfg, data_lake_root=settings.data_lake_root)

    dataset_order = _resolve_dataset_order(pipeline_cfg, args.datasets)
    if not dataset_order:
        raise ValueError("No datasets selected to run")

    succeeded: list[dict[str, str | list[str]]] = []
    failed: list[str] = []
    skipped: list[str] = []

    for dataset_name in dataset_order:
        dataset_cfg = pipeline_cfg.get("datasets", {}).get(dataset_name, {})
        dataset_required = _is_required_dataset(dataset_cfg)
        extension_cls = EXTENSIONS.get(dataset_name)
        if extension_cls is None:
            if dataset_required:
                logger.error(
                    "Dataset is configured but extension module is missing: %s",
                    dataset_name,
                )
                failed.append(dataset_name)
            else:
                logger.warning(
                    "Optional dataset extension module is missing; skipping: %s",
                    dataset_name,
                )
                skipped.append(dataset_name)
            continue

        extension = extension_cls()
        logger.info("Running dataset: %s (required=%s)", dataset_name, dataset_required)

        try:
            result = extension.run(context, dataset_cfg)
            succeeded.append(result)
            logger.info("Dataset completed: %s", result)
        except Exception as exc:  # pragma: no cover - runtime safety path
            if dataset_required:
                failed.append(dataset_name)
                logger.exception("Required dataset failed: %s", dataset_name)
                if args.stop_on_error:
                    raise
            else:
                skipped.append(dataset_name)
                logger.warning(
                    "Optional dataset failed and was skipped: %s (%s: %s)",
                    dataset_name,
                    type(exc).__name__,
                    exc,
                )

    logger.info(
        "Pipeline run finished | requested=%d succeeded=%d failed=%d skipped=%d",
        len(dataset_order),
        len(succeeded),
        len(failed),
        len(skipped),
    )
    if skipped:
        logger.info("Skipped optional datasets: %s", ", ".join(skipped))

    if failed and not args.allow_partial_success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
