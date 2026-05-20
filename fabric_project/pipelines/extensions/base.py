from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from fabric_project.pipelines.context import PipelineContext


class DatasetExtension(ABC):
    """Contract for adding new datasets without touching orchestration code."""

    name: str

    @abstractmethod
    def extract_bronze(self, context: PipelineContext, cfg: dict[str, Any]) -> Any:
        raise NotImplementedError

    @abstractmethod
    def transform_silver(
        self, context: PipelineContext, cfg: dict[str, Any], bronze_output: Any
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def publish_gold(
        self, context: PipelineContext, cfg: dict[str, Any], silver_output: Any
    ) -> Any:
        raise NotImplementedError

    def run(self, context: PipelineContext, cfg: dict[str, Any]) -> dict[str, str | list[str]]:
        bronze_output = self.extract_bronze(context, cfg)
        silver_output = self.transform_silver(context, cfg, bronze_output)
        gold_output = self.publish_gold(context, cfg, silver_output)

        return {
            "dataset": self.name,
            "bronze": _as_display(bronze_output),
            "silver": _as_display(silver_output),
            "gold": _as_display(gold_output),
        }



def _as_display(value: Any) -> str | list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return str(value)
