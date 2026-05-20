from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class PipelineContext:
    config: dict[str, Any]
    data_lake_root: Path

    def ensure_layer_dir(self, layer: str, dataset_name: str) -> Path:
        directory = self.data_lake_root / layer / dataset_name
        directory.mkdir(parents=True, exist_ok=True)
        return directory



def load_pipeline_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)

    if not isinstance(payload, dict):
        raise ValueError(f"Pipeline config must be a YAML object: {config_path}")
    payload.setdefault("default_order", [])
    payload.setdefault("datasets", {})
    if not isinstance(payload["default_order"], list):
        raise ValueError("Pipeline config 'default_order' must be a list")
    if not isinstance(payload["datasets"], dict):
        raise ValueError("Pipeline config 'datasets' must be an object")
    return payload
