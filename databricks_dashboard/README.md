# SFDC Account Consumption & Forecast Dashboard

Streamlit dashboard for SFDC account consumption and forecast scenario with year-bucketed organic growth.

**Source**: `main.fin_live_gold.paid_usage_metering`

## Parameters (sidebar)

| Parameter | Description |
|-----------|-------------|
| **SFDC Account ID** | Filter all queries (required). |
| **Historical Start / End** | Date range for the consumption report. |
| **Forecast Month Start / End** | Date range for forecast months (categorized as Yr1, Yr2, Yr3…). |
| **Yr1 / Yr2 / Yr3 growth %** | Organic growth % for forecast scenario (Yr1 = first 12 months from forecast start, Yr2 = next 12, etc.). |
| **Max rows** | Limit for historical consumption result. |
| **Catalog / Schema** | Default: `main`, `fin_live_gold`. |

## Year buckets

- **Yr1**: First 12 months from Forecast Month Start.
- **Yr2**: Next 12 months (months 13–24).
- **Yr3**: Next 12 months (months 25–36).

Forecast scenario applies the corresponding organic growth % per year (compound YoY).

## Sections

1. **Historical consumption report**  
   Query on `paid_usage_metering`: `sfdc_account_id`, `sfdc_workspace_id`, `sfdc_workspace_name`, `sku_name`, `month`, `usage_quantity`, `usage_dollars`, `usage_dollars_at_list` for the account and historical date range. Download as XLS.

2. **Forecast scenario**  
   Table of forecast months with `month`, `year_bucket` (Yr1/Yr2/Yr3), `organic_growth_pct`, `projected_usage_dollars`. Baseline = last month’s total `usage_dollars` from historical; projected = baseline × compound growth by year. Download as XLS.

## Deploy

Same as parent app: upload this folder to the workspace, create an App, add a SQL warehouse resource (or enter HTTP path in sidebar), deploy. Ensure the app has **SELECT** on `main.fin_live_gold.paid_usage_metering`.

## Files

- `app.py` — Streamlit entry point
- `app.yaml` — Run command and env
- `requirements.txt` — Dependencies
