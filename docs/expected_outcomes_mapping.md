# Expected Outcomes Mapping

This file maps the assignment's expected outcomes to concrete project evidence.

## 1) Unified view of how urban mobility affects air quality

- Dashboard panel: `Trips vs PM2.5 (Unified View)` in Grafana dashboard UID `fabric-weather-overview`.
- Source measurements in InfluxDB:
  - `taxi_daily` (mobility)
  - `air_quality_daily` (OpenAQ)

## 2) Convert taxi economics into global terms (GDP, FX)

- Dashboard panels:
  - `Latest USD/EUR FX`
  - `Latest GDP USD`
  - `Revenue in EUR (Derived from FX)` (computed from taxi revenue + FX).
- Source measurements in InfluxDB:
  - `taxi_daily`
  - `economy_daily`

## 3) Demonstrate Microsoft Fabric full-stack engineering

- Lakehouse medallion implementation:
  - `fabric_project/pipelines/extensions/*.py`
  - `fabric_project/pipelines/runner.py`
- Notebook orchestration:
  - `fabric_notebooks/fabric_medallion_orchestrator.py`
- Warehouse model:
  - `sql/fabric_warehouse_star_schema.sql`
- Fabric setup guide:
  - `docs/fabric_setup.md`

## 4) Replicable framework to extend with new open datasets

- Extension contract and plugin-style registry:
  - `fabric_project/pipelines/extensions/base.py`
  - `fabric_project/pipelines/extensions/__init__.py`
- Example extended dataset (weather) and external integrations:
  - `fabric_project/pipelines/extensions/weather.py`
  - `fabric_project/integrations/weather_sync_job.py`
  - `fabric_project/integrations/domain_sync_job.py`
