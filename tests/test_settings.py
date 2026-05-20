from __future__ import annotations

import os
import unittest

from fabric_project.common.settings import load_settings


class TestSettings(unittest.TestCase):
    def test_relative_paths_are_resolved_from_project_root(self) -> None:
        original_data_lake_root = os.environ.get("DATA_LAKE_ROOT")
        original_quality_config = os.environ.get("QUALITY_CHECKS_CONFIG")
        try:
            os.environ["DATA_LAKE_ROOT"] = "data/lakehouse"
            os.environ["QUALITY_CHECKS_CONFIG"] = "config/quality_checks.yaml"
            settings = load_settings(reload=True)

            self.assertTrue(settings.data_lake_root.is_absolute())
            self.assertTrue(settings.quality_checks_config.is_absolute())
            self.assertTrue(str(settings.data_lake_root).endswith("/data/lakehouse"))
            self.assertTrue(str(settings.quality_checks_config).endswith("/config/quality_checks.yaml"))
        finally:
            if original_data_lake_root is None:
                os.environ.pop("DATA_LAKE_ROOT", None)
            else:
                os.environ["DATA_LAKE_ROOT"] = original_data_lake_root

            if original_quality_config is None:
                os.environ.pop("QUALITY_CHECKS_CONFIG", None)
            else:
                os.environ["QUALITY_CHECKS_CONFIG"] = original_quality_config
            load_settings(reload=True)


if __name__ == "__main__":
    unittest.main()
