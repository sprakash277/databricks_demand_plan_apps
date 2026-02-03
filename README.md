# C360 Consumption Analytics

Query account-level consumption with historical MoM growth, organic baseline, and forecast comparison.

**Source table**: `{catalog}.{schema}.c360_consumption_account_monthly` (default: `main.gtm_data`).

---

## Databricks App (Streamlit)

A [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps) runs as a web app on the serverless platform with Streamlit.

### App parameters (sidebar)

| Parameter | Description | Example |
|-----------|-------------|---------|
| **Account Name (ILIKE)** | Filter on `account_name`; use `%` for all | `%Kroger%`, `%Databricks%` |
| **Historical Start / End** | `usage_date` range for consumption history | 2024-01-01 … 2026-01-01 |
| **Forecast Start / End** | Date range for forecast-period view | 2025-01-01 … 2026-06-01 |
| **Min MoM Growth %** | Optional filter: rows where MoM growth ≥ this value | `5` or empty |
| **Max rows** | Result limit (1–10000) | 500 |
| **Catalog / Schema** | Unity Catalog and schema of the consumption table | main, gtm_data |

### Deploy the app

1. **Upload app files**  
   Upload this `databricks_demand_plan_apps/` folder to your Databricks workspace (e.g. **Workspace** → **Import** → folder or sync via CLI).

2. **Create and configure the app**  
   - Go to **Compute** → **Apps** → **Create app**.  
   - Choose the folder where you uploaded the app (e.g. `/Workspace/Users/<you>/databricks_demand_plan_apps`).  
   - Add a **SQL warehouse** resource so the app can run queries (recommended). In **Configure** → **App resources** → **+ Add resource** → **SQL warehouse**, grant **Can use**, and set the resource key (e.g. `sql_warehouse`).  
   - In the app’s `app.yaml`, add under `env`:  
     `- name: DATABRICKS_WAREHOUSE_ID`  
     `  valueFrom: sql_warehouse`  
     (Replace `sql_warehouse` with your resource key.)  
   - If you don’t add a warehouse resource, the app will show a sidebar text input for the SQL warehouse HTTP path.

3. **Deploy**  
   Click **Deploy** and select the same folder. After deployment, open the app URL to use the UI.

4. **CLI deploy (optional)**  
   ```bash
   databricks sync ./databricks_demand_plan_apps /Workspace/Users/<you>/databricks_demand_plan_apps
   databricks apps deploy consumption-analytics --source-code-path /Workspace/Users/<you>/databricks_demand_plan_apps
   ```

### App files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit entry point; parameters and SQL execution |
| `app.yaml` | Run command (`streamlit run app.py`) and optional env |
| `requirements.txt` | databricks-sdk, databricks-sql-connector, streamlit, pandas |

### Permissions

The app’s service principal (or the user, if using user auth) needs **SELECT** on `main.gtm_data.c360_consumption_account_monthly` and **Can use** on the SQL warehouse. See [Configure authorization in a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth).

---

## Notebook (widgets only)

For a notebook-based workflow without Streamlit, import **`consumption_app_notebook.py`** from this folder as a Databricks notebook. Run it and set the widgets (Account Name, Historical/Forecast dates, etc.) for historical consumption and forecast-period views.

---

## Key calculations

- **MoM Growth %**: `(current_month_dbu - prev_month_dbu) / prev_month_dbu * 100`, via `LAG()` over `account_id` ordered by `usage_date`.
- **Organic growth**: Uses `organic_growth_baseline` from the table.
- **Forecast comparison**: Outputs `dbu_dollars`, `forecast_consumption_ds`, and `submitted_ae_forecast`.

## Implementation notes

- **Dates**: `usage_date` is the first day of each month.  
- **Growth**: Window is `PARTITION BY account_id ORDER BY usage_date`.  
- **Zeros**: Rows with `dbu_dollars <= 0` are excluded in the historical query.  
- **Forecast**: Future months can have forecast fields set and zero actual consumption; use Forecast Start/End to scope the forecast view.
