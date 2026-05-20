# Operations Guide

## 1) Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set required secrets in `.env`:

```bash
# recommended for OpenAQ v3
OPENAQ_API_KEY=your_key_here
INFLUXDB_TOKEN=your_real_token
```

## 2) Run Medallion Pipelines

```bash
./scripts/run_pipeline.sh
```

OpenAQ ingestion is real-data only and required by default (`required: true`).
Set `OPENAQ_API_KEY` in `.env` to fetch fresh air-quality data from OpenAQ API v3.
OpenAQ, economy, and weather support cached fallback (`allow_cached_fallback: true`) when upstream APIs are temporarily unavailable.

By default, pipeline exits with non-zero code if any dataset fails. If you intentionally want partial-success behavior:

```bash
./scripts/run_pipeline.sh --allow-partial-success
```

Run selected datasets only:

```bash
./scripts/run_pipeline.sh --datasets weather,economy
```

## 3) Start InfluxDB + Grafana + Sync Job

```bash
docker compose up -d influxdb grafana
./scripts/run_weather_sync.sh --once
./scripts/run_domain_sync.sh
```

If Grafana shows `connection refused` to `localhost:8086`, set
`GRAFANA_INFLUXDB_URL=http://influxdb:8086` in `.env` and recreate Grafana.

Long-running sync loop:

```bash
./scripts/run_weather_sync.sh
```

By default, weather sync refreshes the weather dataset first, then syncs newly enriched rows to InfluxDB.

To sync domain metrics used by cross-domain dashboard panels (taxi trips/revenue, GDP/FX, air quality):

```bash
./scripts/run_domain_sync.sh
```

Sync selected datasets only:

```bash
./scripts/run_domain_sync.sh --datasets taxi,economy
```

Skip refresh if you want to sync only existing Gold weather artifacts:

```bash
./scripts/run_weather_sync.sh --once --skip-refresh
```

Preview candidate rows without writing to InfluxDB:

```bash
./scripts/run_weather_sync.sh --once --dry-run
```

Grafana URL: `http://localhost:3000` (admin/admin12345 by default in compose).

## 4) Run Great Expectations Checks

```bash
./scripts/run_quality_checks.sh
```

Output reports are generated under `reports/quality/`.
If a table is configured as optional (`required: false`) and missing, the report includes it as skipped instead of crashing.
If a check itself is malformed (for example, missing column), it is reported as a failed expectation instead of aborting the entire run.

Run tests:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

## 5) Run Bot Triggers

Telegram:

```bash
./scripts/run_telegram_bot.sh
```

Trigger commands:

- Telegram: `/dqreport`

Optional channel/chat controls:
- Telegram: `TELEGRAM_ALLOWED_CHAT_IDS=123456789,987654321`

## 6) Documentation Deliverables

- Data dictionary: `docs/data_dictionary.md`
- Lineage diagram: `docs/lineage.md`
- Governance policy: `docs/governance_policies.md`

## 7) Troubleshooting

Grafana error: `invalid: compilation failed ... $`
- Cause: unresolved dashboard variable interpolation in Flux query.
- Fix: use the latest `dashboards/grafana/dashboards/weather-overview.json` (city variable query now uses a fixed bucket string).
- Then refresh/reload Grafana dashboard provisioning.

Grafana panel shows `No data` for current temperature
- Run:
  - `./scripts/run_pipeline.sh --datasets weather`
  - `./scripts/run_weather_sync.sh --once --reset-state`
- Ensure InfluxDB is reachable at `INFLUXDB_URL` and that weather rows exist in `weather_timeseries`.
- Why `--reset-state` helps:
  - If InfluxDB was recreated but `.state/weather_sync_state.json` still exists, incremental sync may skip all rows.
  - Resetting state forces a replay/backfill into the bucket.

Docker compose conflict with old local containers
- If an older local environment already used fixed container names, stop/remove stale containers before starting a new stack copy.

Fabric Pipeline `Dataflow1` error `20302` (`InvalidQueriesMetadata`, `QueriesMetadata must not be empty`)
- Cause: the referenced Dataflow Gen2 was published without at least one valid output query metadata entry (for example, no load-enabled query or broken/missing data destination metadata).
- Fix in Fabric UI:
  - Open the dataflow item used by `Dataflow1` (for example `Dataflow_Main`) and select **Edit**.
  - Ensure at least one real table query exists (not only parameters/helper queries).
  - For that query, enable load and configure a valid destination (Lakehouse/Warehouse).
  - If destination metadata looks stale, remove the destination and add it again from **Add data destination**.
  - **Publish** the dataflow and wait for publish completion.
  - Open `Pipeline_Main`, edit `Dataflow1`, reselect/refresh the dataflow reference, save, and run again.
- Verification:
  - Run the dataflow directly once from its own item page; confirm success.
  - Then rerun the pipeline and confirm `Dataflow1` turns green.
