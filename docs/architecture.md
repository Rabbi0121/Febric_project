# Microsoft Fabric Data Engineering Project Architecture

## 1) Core Fabric Requirements Coverage

This implementation follows the requested medallion architecture and keeps it extendable.

- Bronze: raw ingestion of NYC Taxi (Parquet), OpenAQ (JSON API), GDP+FX (JSON/CSV), and Weather (JSON API).
- Silver: schema standardization, type normalization, deduplication, and enrichment.
- Gold: analytics-ready fact tables for taxi, air quality, economy, and weather.
- Warehouse: star-schema DDL is included in `sql/fabric_warehouse_star_schema.sql`.
- Monitoring/Governance hooks:
  - automated quality checks via Great Expectations
  - external observability via time-series + Grafana

## 2) Extendability Design (Key Requirement)

The pipeline is extension-based.

- Dataset contract: `fabric_project/pipelines/extensions/base.py`
- Registry: `fabric_project/pipelines/extensions/__init__.py`
- Orchestrator: `fabric_project/pipelines/runner.py`
- Shared HTTP client/retries: `fabric_project/common/http.py`

To add a new dataset, implement one class (`extract_bronze`, `transform_silver`, `publish_gold`) with a unique `name`. The registry auto-discovers extension modules, so no orchestrator rewrite is required.

## 3) External Integration #1: Weather + Time-Series + Dashboard

- Weather dataset extension: `fabric_project/pipelines/extensions/weather.py`
- External periodic sync job: `fabric_project/integrations/weather_sync_job.py`
- Time-series DB: InfluxDB (Docker)
- Dashboard: Grafana with pre-provisioned data source and dashboard JSON

Flow:
1. Weather enters Bronze/Silver/Gold with enrichment (comfort index, labels, rain flag).
2. External job can refresh weather data and then copy new enriched rows into InfluxDB on a schedule.
3. Grafana reads InfluxDB and renders weather analytics panels.

## 4) External Integration #2: Great Expectations + Bot Trigger

- Quality config: `config/quality_checks.yaml`
- Quality runner: `fabric_project/quality/runner.py`
- Telegram bot trigger: `fabric_project/integrations/telegram_bot.py`
- Data dictionary and governance docs:
  - `docs/data_dictionary.md`
  - `docs/lineage.md`
  - `docs/governance_policies.md`

A user sends `/dqreport` to the Telegram bot, then the bot:
1. runs Great Expectations checks,
2. generates markdown + JSON quality reports,
3. responds with summary + top failures and report attachment.

## 5) OpenAQ API Compatibility

- OpenAQ v2 is deprecated (returns HTTP 410).
- This project uses OpenAQ API v3 with `X-API-Key` auth.
- Config:
  - `datasets.openaq.endpoint_base`
  - `OPENAQ_API_KEY` environment variable
- Ingestion is real-data mode with OpenAQ required by default (`datasets.openaq.required: true`).
- OpenAQ, economy, and weather support cached fallback for transient upstream API outages (`allow_cached_fallback: true`).
