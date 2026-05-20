# Governance Policies

## Data Quality
- Run Great Expectations checks at least daily and on-demand via bot trigger.
- Treat any failed critical checks (null key columns, invalid ranges) as a release blocker for downstream analytics.
- Keep expectation thresholds versioned in `config/quality_checks.yaml`.

## Security and Secrets
- Never commit secrets; use environment variables (`OPENAQ_API_KEY`, `INFLUXDB_TOKEN`, `TELEGRAM_BOT_TOKEN`).
- Rotate API keys and bot tokens periodically.
- Restrict bot usage to known Telegram chat IDs where possible.

## Access Controls
- Implement RLS in Power BI semantic model for role-based views.
- Restrict write access to Gold/Warehouse layers to pipeline service principals.
- Grant read-only access to analysts and dashboard consumers.

## Lineage and Auditability
- Keep Bronze raw payloads immutable per run where possible.
- Maintain run logs for extraction, transformation, quality, and sync jobs.
- Enable Fabric lineage/Purview integration in workspace deployments.

## Refresh and Incident Policy
- Primary pipeline refresh: daily/hourly as required by business SLA.
- External weather sync: every 30 minutes (configurable).
- On ingestion failure:
  - preserve last successful Gold snapshot,
  - alert engineering owner,
  - attach latest quality report and pipeline logs.
