from __future__ import annotations

import importlib
import inspect
import pkgutil
from pathlib import Path

from fabric_project.pipelines.extensions.base import DatasetExtension


def _discover_extensions() -> dict[str, type[DatasetExtension]]:
    extensions: dict[str, type[DatasetExtension]] = {}
    package_dir = Path(__file__).resolve().parent
    package_name = __name__

    for module_info in pkgutil.iter_modules([str(package_dir)]):
        module_name = module_info.name
        if module_name == "base":
            continue

        module = importlib.import_module(f"{package_name}.{module_name}")
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if not issubclass(cls, DatasetExtension) or cls is DatasetExtension:
                continue

            dataset_name = getattr(cls, "name", "")
            if not dataset_name:
                continue
            if dataset_name in extensions:
                raise ValueError(
                    f"Duplicate dataset extension name '{dataset_name}' found in {cls.__module__}"
                )
            extensions[dataset_name] = cls

    return dict(sorted(extensions.items()))


EXTENSIONS: dict[str, type[DatasetExtension]] = _discover_extensions()
