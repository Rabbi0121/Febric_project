# Fabric Setup Notes

## 1) Prerequisites

- Microsoft account with Power BI/Fabric trial or paid capacity access.
- Region supported by Fabric.
- Permissions to create workspace items.

## 2) Create Workspace

1. Open [https://app.fabric.microsoft.com](https://app.fabric.microsoft.com).
2. Select **Workspaces** -> **New workspace**.
3. Name: `Fabric-Course-Project` (or your preferred name).
4. Description: short project description.
5. Workspace type:
   - Choose `Fabric` if enabled.
   - Choose `Fabric Trial` if enabled and you are evaluating.
   - If both are disabled, see section 3 below.
6. Select **Apply**.

## 3) If `Fabric` / `Fabric Trial` Is Disabled

This usually means no Fabric capacity is assigned to your tenant/user.

- Ask tenant admin to enable Fabric and assign capacity.
- Or start Fabric trial from your profile if your tenant allows self-service trial.
- After enablement, re-open workspace settings and confirm Fabric items are available.

## 4) Create Required Items In Workspace

Create these items in the workspace:

- Lakehouse
- Data Factory pipeline(s)
- Dataflow Gen2 (API ingestion)
- Notebook (PySpark orchestration/transform)
- Warehouse
- Optional: Power BI semantic model + report

## 5) Map This Repository To Fabric Items

- Lakehouse folder structure mirrors this project:
  - `data/lakehouse/bronze`
  - `data/lakehouse/silver`
  - `data/lakehouse/gold`
- Warehouse schema SQL: `sql/fabric_warehouse_star_schema.sql`
- Notebook starter orchestration: `fabric_notebooks/fabric_medallion_orchestrator.py`

## 6) Configure Secrets and Runtime

- Store secrets in Fabric (or Key Vault) and map to pipeline/notebook runtime:
  - `OPENAQ_API_KEY`
  - `INFLUXDB_TOKEN` (for external weather sync)
- Keep config files in source control (`config/*.yaml`) and secrets outside source control.

## 7) Scheduling

- Fabric medallion pipeline refresh: hourly/daily as needed.
- External weather sync job: every 30 minutes (or your SLA).
- Quality checks:
  - On-demand from Telegram bot command (`/dqreport`)
  - Optional scheduled run with report distribution.

## 8) Governance

- Enable lineage in Fabric/Purview.
- Apply workspace access roles (`Admin`, `Member`, `Viewer`) by least privilege.
- Apply RLS in semantic model if report audience requires row-level isolation.

## 9) Validation Checklist

- Lakehouse Bronze/Silver/Gold tables/files are populated.
- Warehouse fact/dimension tables created successfully.
- Grafana displays weather time-series from InfluxDB.
- Great Expectations report generates and flags failures correctly.
- Telegram command trigger works end-to-end.
