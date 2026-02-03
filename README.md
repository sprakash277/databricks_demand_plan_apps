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

### Other-workspace: use a SQL warehouse in a different workspace

The app can connect to a SQL warehouse in **another** Databricks workspace (not the one where the app runs). You configure this with environment variables; the app does **not** use the built-in SQL warehouse resource for the remote warehouse.

**If you see:** *"Other-workspace mode is on but REMOTE_DATABRICKS_TOKEN is not set"* — do this:

1. **Add a secret resource:** App → **Configure** → **App resources** → **+ Add resource** → **Secret**. Choose the scope/key where the other workspace's PAT is stored; set the **resource key** to `remote_databricks_token`. Save.
2. **Set the env in `app.yaml`:** In the app folder, under `env`, uncomment the `REMOTE_DATABRICKS_TOKEN` block so it reads: `- name: REMOTE_DATABRICKS_TOKEN` and `valueFrom: remote_databricks_token`.
3. **Redeploy** the app.

**Steps (full first-time setup):**

1. **Create a secret for the other workspace’s token**  
   In the workspace where the app runs: **Settings** → **Secrets** (or use a secret scope). Create a secret that holds a [personal access token (PAT)](https://docs.databricks.com/aws/en/dev-tools/auth.html#pat) or OAuth credentials for the **other** workspace (the one that has the SQL warehouse and the data).  
   Example scope: `app-secrets`, key: `remote-databricks-token`.

2. **Get the other workspace’s connection details**  
   In the **other** workspace: open the target SQL warehouse → **Connection details**. Note:
   - **Server hostname** (e.g. `adb-1234567890123456.7.azuredatabricks.net` or `other-workspace.cloud.databricks.com`)
   - **HTTP path** (e.g. `/sql/1.0/warehouses/abc123def456`)

3. **Configure the app’s `app.yaml`**  
   In the app folder, under `env`, add (or uncomment and set):

   ```yaml
   env:
     - name: REMOTE_WORKSPACE_HOST
       value: "other-workspace.cloud.databricks.com"   # hostname from step 2
     - name: REMOTE_HTTP_PATH
       value: "/sql/1.0/warehouses/abc123def456"     # HTTP path from step 2
     - name: REMOTE_DATABRICKS_TOKEN
       valueFrom: remote_databricks_token            # secret scope + key, e.g. app-secrets/remote-databricks-token
   ```

   For `valueFrom`, use the resource key you gave the secret when adding it as an app resource (e.g. `remote_databricks_token`), or the scope/key format your workspace uses for env injection.

4. **Add the secret as an app resource (recommended)**  
   When editing the app: **Configure** → **App resources** → **+ Add resource** → **Secret**. Grant the app access to the secret (e.g. **Can read**), set the resource key to match `valueFrom` (e.g. `remote_databricks_token`). This keeps the token out of `app.yaml` and uses secure injection.

5. **Redeploy the app**  
   Deploy the app so the new env and secret are applied. On load, the app will see `REMOTE_WORKSPACE_HOST` and `REMOTE_HTTP_PATH`, and will use `REMOTE_DATABRICKS_TOKEN` to connect to that warehouse instead of the local one.

**Behavior:**

- If **all three** of `REMOTE_WORKSPACE_HOST`, `REMOTE_HTTP_PATH`, and `REMOTE_DATABRICKS_TOKEN` are set, the app connects to the **other** workspace’s SQL warehouse using that host, path, and token.
- If none of the remote env vars are set, the app uses the **same-workspace** behavior (SQL warehouse resource or sidebar HTTP path).

**Security:**

- Never put the token in `app.yaml` as plain text. Always use a secret and `valueFrom`.
- The token must have access to the SQL warehouse and the tables (e.g. `main.gtm_data.c360_consumption_account_monthly`) in the **other** workspace.
- Prefer a service principal or a PAT with minimal scope and rotation.

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
