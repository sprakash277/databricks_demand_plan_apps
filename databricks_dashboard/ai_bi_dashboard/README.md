# Databricks AI/BI Dashboard — SFDC Consumption & Forecast

This folder contains **parameterized SQL datasets** and setup steps to build a **Databricks AI/BI dashboard** (Lakeview) that mirrors the logic of the Streamlit dashboard: SFDC account consumption report and forecast scenario with Yr1/Yr2/Yr3 organic growth.

**Source table**: `main.fin_live_gold.paid_usage_metering`

---

## Logic (same as Streamlit dashboard)

- **Historical consumption**: Filter by `sfdc_account_id`, `historical_start`, `historical_end`; show `sfdc_workspace_id`, `sfdc_workspace_name`, `sku_name`, `month`, `usage_quantity`, `usage_dollars`, `usage_dollars_at_list`.
- **Forecast scenario**: Months from `forecast_start` to `forecast_end`, categorized as **Yr1** (first 12 months), **Yr2** (next 12), **Yr3** (next 12). User sets **organic growth %** for Yr1, Yr2, Yr3. **Baseline** = last month’s total `usage_dollars` from historical; **projected_usage_dollars** = baseline × compound growth (Yr1: ×(1+g1), Yr2: ×(1+g1)(1+g2), Yr3: ×(1+g1)(1+g2)(1+g3)).

---

## Files

| File | Purpose |
|------|---------|
| `01_historical_consumption.sql` | Historical consumption report dataset (params: sfdc_account_id, historical_start, historical_end). |
| `02_baseline_last_month.sql` | Optional: baseline total for last month (params: sfdc_account_id, historical_end). |
| `03_forecast_scenario.sql` | Forecast months with year_bucket, organic_growth_pct, projected_usage_dollars (params: forecast_start, forecast_end, organic_growth_yr1/yr2/yr3, sfdc_account_id, historical_end). |

---

## Create the dashboard in Databricks

### 1. Create a new AI/BI dashboard

1. In Databricks: **SQL** → **Dashboards** (or **AI/BI**), then **Create** → **Dashboard**.
2. Name it (e.g. **SFDC Consumption & Forecast**).

### 2. Add datasets and parameters

For each dataset below, open the **Data** tab, **Add dataset**, paste the SQL, then add parameters as listed (click **Add parameter** or type `:parameter_name` in the query).

**Dataset 1: Historical consumption**

- Paste the contents of **`01_historical_consumption.sql`**.
- Add parameters (gear icon or type in query):
  - `sfdc_account_id` — **String**, display name e.g. "SFDC Account ID".
  - `historical_start` — **Date**, display name "Historical Start".
  - `historical_end` — **Date**, display name "Historical End".
- Set default values (e.g. a valid account ID and date range), run the query to confirm it works.

**Dataset 2: Forecast scenario**

- Add a second dataset; paste the contents of **`03_forecast_scenario.sql`**.
- Add parameters:
  - `forecast_start` — **Date**, "Forecast Month Start".
  - `forecast_end` — **Date**, "Forecast Month End".
  - `organic_growth_yr1` — **Numeric** (Decimal), "Yr1 growth %".
  - `organic_growth_yr2` — **Numeric**, "Yr2 growth %".
  - `organic_growth_yr3` — **Numeric**, "Yr3 growth %".
  - `sfdc_account_id` — **String**, "SFDC Account ID".
  - `historical_end` — **Date**, "Historical End" (for baseline).
- Set defaults (e.g. 5 for growth %, same account and historical_end as dataset 1), run to confirm.

**Note:** If your warehouse does not support `SEQUENCE`/`EXPLODE` for generating months, use the **Streamlit app** in `../app.py` for the forecast scenario, or adapt the SQL to your dialect (e.g. a calendar table).

### 3. Add filter widgets

1. On the dashboard canvas, click **Add a filter (field/parameter)**.
2. Under **Parameters**, add widgets for:
   - **SFDC Account ID** (parameter: `sfdc_account_id`) — so one widget can drive both datasets if they share this parameter.
   - **Historical Start** / **Historical End** (parameters: `historical_start`, `historical_end`), or a **Date range** parameter if you prefer.
   - **Forecast Month Start** / **Forecast Month End** (`forecast_start`, `forecast_end`).
   - **Yr1 growth %**, **Yr2 growth %**, **Yr3 growth %** (`organic_growth_yr1`, `organic_growth_yr2`, `organic_growth_yr3`).

### 4. Add visualizations

1. **Historical consumption**: Add a visualization widget, choose the historical consumption dataset; e.g. table or bar chart (e.g. `usage_dollars` by `month` or by `sku_name`).
2. **Forecast scenario**: Add another widget, choose the forecast scenario dataset; e.g. line chart of `projected_usage_dollars` by `month`, or table with `month`, `year_bucket`, `organic_growth_pct`, `projected_usage_dollars`.

### 5. Publish (optional)

Use **Publish** to create a published view; optionally embed credentials and set the SQL warehouse. Viewers can then use the dashboard with the same parameters (SFDC Account ID, historical and forecast dates, Yr1/Yr2/Yr3 growth %).

---

## Parameter summary

| Parameter | Type | Used in | Description |
|-----------|------|---------|-------------|
| `sfdc_account_id` | String | 01, 03 | SFDC Account ID for paid_usage_metering. |
| `historical_start` | Date | 01 | Start of consumption report period. |
| `historical_end` | Date | 01, 03 | End of consumption report; also used for baseline in 03. |
| `forecast_start` | Date | 03 | First month of forecast (Yr1 start). |
| `forecast_end` | Date | 03 | Last month of forecast. |
| `organic_growth_yr1` | Numeric | 03 | Organic growth % for Yr1 (e.g. 5 for 5%). |
| `organic_growth_yr2` | Numeric | 03 | Organic growth % for Yr2. |
| `organic_growth_yr3` | Numeric | 03 | Organic growth % for Yr3. |

---

## Import/export dashboard (optional)

- **Export**: Use Workspace API `GET /api/2.0/workspace/export` with path ending in `.lvdash.json` to export the dashboard as JSON.
- **Import**: Use `POST /api/2.0/workspace/import` with the same path and `format: "AUTO"` to import into another workspace.

See [Manage dashboards with Workspace APIs](https://docs.databricks.com/aws/en/dashboards/tutorials/workspace-dashboard-api).
