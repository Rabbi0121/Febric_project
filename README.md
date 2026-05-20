# Microsoft Fabric Data Engineering Project (Extendable)

This repository implements the Fabric learning project scope and adds the requested integrations:

1. Weather data as an additional dataset in the same medallion pattern.
2. External periodic sync of enriched weather data into a separate time-series DB (InfluxDB).
3. Grafana dashboard for cross-domain analysis (weather + taxi + economy + air quality).
4. Great Expectations quality checks with user-friendly trigger through Telegram bot.

## What Is Implemented

- Extension-based medallion pipeline framework (`Bronze -> Silver -> Gold`).
- Dataset extensions:
  - `nyc_taxi`
  - `openaq`
  - `economy` (World Bank GDP + ECB FX)
  - `weather` (Open-Meteo)
- External sync jobs from Gold artifacts to InfluxDB:
  - weather (`weather_enriched`)
  - taxi/economy/air-quality domain facts
- Grafana provisioning and starter dashboard.
- Great Expectations checks with markdown/json report output.
- Bot commands to run checks on demand.
- Data dictionary, lineage, and governance policy docs.
- Production hardening: path-safe config resolution, `.env` auto-loading, and resilient error handling.

## Structure

- `fabric_project/pipelines/` -> ingestion/transform/model pipeline code
- `fabric_project/integrations/` -> weather sync + bot trigger integrations
- `fabric_project/quality/` -> Great Expectations runner and report generation
- `config/` -> dataset, weather sync, and quality configs
- `dashboards/grafana/` -> Grafana datasource/dashboard provisioning
- `sql/` -> Fabric Warehouse star schema DDL
- `docs/` -> architecture, operations, and extension playbook

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# required for real-time OpenAQ API v3 ingestion
./scripts/run_pipeline.sh
./scripts/run_quality_checks.sh
python -m unittest discover -s tests -p 'test_*.py'
```

Optional stack for weather analytics:

```bash
docker compose up -d influxdb grafana
./scripts/run_weather_sync.sh --once
./scripts/run_domain_sync.sh
```

Notes:
- Runner scripts auto-use `./.venv/bin/python` when available.
- `.env` values are loaded automatically by application settings.
- `run_weather_sync` refreshes weather data before syncing by default (`--skip-refresh` to disable).
- `datasets.openaq.required: true` enforces the assignment requirement that OpenAQ stays part of the main pipeline.
- `allow_cached_fallback: true` for openaq/economy/weather keeps pipelines resilient during temporary API outages by reusing previously fetched artifacts.
- Quality checks can mark some datasets optional with `required: false` in `config/quality_checks.yaml`.

## Requirement Mapping

- Main Fabric scope: covered by medallion pipelines + warehouse schema docs.
- Extendability (last point): implemented through extension contract + registry.
- External integration 1: weather + periodic sync + Grafana.
- External integration 2: Great Expectations + Telegram trigger.

For details, see:
- `docs/architecture.md`
- `docs/extensibility.md`
- `docs/fabric_completion_checklist.md`
- `docs/operations.md`
- `docs/expected_outcomes_mapping.md`
- `docs/data_dictionary.md`
- `docs/lineage.md`
- `docs/governance_policies.md`
