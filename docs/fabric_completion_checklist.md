# Fabric Completion Checklist

This checklist maps the assignment instructions to project deliverables in this repository and the remaining click-ops in Microsoft Fabric.

## 1) Scope Coverage Matrix

| Requirement | Status | Evidence |
|---|---|---|
| Medallion architecture (Bronze/Silver/Gold) | Completed | `fabric_project/pipelines/extensions/*.py`, `fabric_project/pipelines/runner.py` |
| NYC Taxi ingestion (Parquet) | Completed | `fabric_project/pipelines/extensions/nyc_taxi.py` |
| OpenAQ ingestion (API v3) | Completed | `fabric_project/pipelines/extensions/openaq.py`, `OPENAQ_API_KEY` in `.env` |
| Economy ingestion (World Bank + ECB) | Completed | `fabric_project/pipelines/extensions/economy.py` |
| Weather integration (extended requirement) | Completed | `fabric_project/pipelines/extensions/weather.py` |
| External periodic sync to time-series DB | Completed | `fabric_project/integrations/weather_sync_job.py`, `config/weather_sync.yaml` |
| Dashboard for time-series analysis (Grafana) | Completed | `dashboards/grafana/**`, `docker-compose.yml` |
| Data quality checks (Great Expectations) | Completed | `fabric_project/quality/runner.py`, `config/quality_checks.yaml` |
| User-friendly trigger via bot | Completed (Telegram only) | `fabric_project/integrations/telegram_bot.py` |
| Warehouse star schema | Completed | `sql/fabric_warehouse_star_schema.sql` |
| Documentation (dictionary, lineage, governance) | Completed | `docs/data_dictionary.md`, `docs/lineage.md`, `docs/governance_policies.md` |
| Extendability (last point) | Completed | `docs/extensibility.md`, extension contract in `extensions/base.py` |

## 2) Fabric Portal Finalization (Manual Click Steps)

Run these once in your Fabric workspace (`Fabric-Course-Project`) to finish cloud wiring:

1. Create/confirm items: `Lakehouse`, `Dataflow Gen2`, `Pipeline`, `Notebook`, `Warehouse`.
2. Dataflow Gen2:
   - Build API entities for OpenAQ, World Bank GDP, and ECB FX.
   - Output to Lakehouse Bronze tables/files.
3. Pipeline:
   - Add activities in order: Copy NYC Taxi -> Dataflow Gen2 -> Notebook -> Warehouse SQL.
   - Set schedule (hourly/daily).
4. Notebook:
   - Use `fabric_notebooks/fabric_medallion_orchestrator.py`.
   - Ensure it reads Bronze and writes Gold managed tables.
5. Warehouse:
   - Run `sql/fabric_warehouse_star_schema.sql`.
6. Secrets:
   - Configure `OPENAQ_API_KEY` and any connection secrets in Fabric.
7. Validate:
   - Confirm Bronze/Silver/Gold outputs exist and pipeline run is green.

## 3) Local Validation Commands

```bash
./scripts/run_pipeline.sh
./scripts/run_quality_checks.sh
./scripts/run_weather_sync.sh --once --dry-run
python -m unittest discover -s tests -p 'test_*.py'
```

## 4) Latest Verification Snapshot (2026-05-13)

### Local execution

- Pipeline runner: `requested=4 succeeded=4 failed=0 skipped=0`
- Quality checks: `PASSED` (`tables=5`, `expectations=19`, `failures=0`)
- Unit tests: `Ran 9 tests` -> `OK`
- Weather sync:
  - `--once --dry-run`: no failures
  - `--once`: no failures (`No new weather rows to sync` when checkpoint is current)

### Dashboard/runtime validation

- Docker runtime confirmed:
  - Grafana service running on `:3000`
  - InfluxDB service running on `:8086`
- InfluxDB query smoke test returned weather rows successfully.
- Grafana datasource query smoke test succeeded (`200`, no Flux error).
- Dashboard variable query was hardened to avoid `$` interpolation compile errors by using a fixed bucket in city variable lookup:
  - `dashboards/grafana/dashboards/weather-overview.json`

### Fabric portal status observed

- Workspace artifacts exist in `Fabric-Course-Project`:
  - `Dataflow_Main`
  - `Lakehouse_Main`
  - `Notebook_Main`
  - `Pipeline_Main`
  - `Warehouse_Main`
- Recent activity visible in portal quick access confirms active usage of key items.
