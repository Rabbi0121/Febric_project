# Extension Playbook

## Add a New Dataset in 4 Steps

1. Create `fabric_project/pipelines/extensions/<dataset_name>.py`.
2. Subclass `DatasetExtension` and implement:
   - `extract_bronze`
   - `transform_silver`
   - `publish_gold`
3. Set a unique class attribute `name = "<dataset_name>"`.
4. Add config in `config/datasets.yaml` and append the dataset name to `default_order`.

The extension registry is discovered automatically at runtime by scanning
`fabric_project/pipelines/extensions/`, so no manual registry edits are required.

## Contract Guarantees

- Each extension owns only its own Bronze/Silver/Gold folder structure.
- Extensions return materialized artifacts (paths) so downstream integrations can reuse them.
- Common orchestration, logging, and path conventions remain centralized.
- HTTP calls can reuse `fabric_project/common/http.py` for retry and timeout behavior.

## Why This Meets the “Extendability” Requirement

- New sources can be onboarded without modifying existing dataset logic.
- New extension modules are discovered automatically, minimizing orchestration churn.
- The orchestration code remains stable as datasets grow.
- Integrations (quality, sync, dashboards) can target standardized artifact paths.
