from __future__ import annotations

import unittest

from fabric_project.pipelines.runner import _is_required_dataset, _resolve_dataset_order


class TestPipelineRunner(unittest.TestCase):
    def test_is_required_dataset_defaults_true(self) -> None:
        self.assertTrue(_is_required_dataset({}))

    def test_is_required_dataset_can_be_optional(self) -> None:
        self.assertFalse(_is_required_dataset({"required": False}))

    def test_resolve_dataset_order_uses_enabled_default_order(self) -> None:
        config = {
            "default_order": ["a", "b", "c"],
            "datasets": {
                "a": {"enabled": True},
                "b": {"enabled": False},
                "c": {"enabled": True},
            },
        }
        self.assertEqual(_resolve_dataset_order(config, cli_datasets=""), ["a", "c"])


if __name__ == "__main__":
    unittest.main()
