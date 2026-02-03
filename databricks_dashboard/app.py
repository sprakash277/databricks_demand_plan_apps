"""
SFDC Account Consumption & Forecast Dashboard — Databricks App (Streamlit).

Parameters: sfdc_account_id, historical start/end, forecast month start/end.
Forecast months are categorized as Yr1 (first 12), Yr2 (next 12), etc.
User can set organic growth % for Yr1, Yr2, Yr3 for forecast scenario.

Source: main.fin_live_gold.paid_usage_metering
"""

import os
from datetime import date
from io import BytesIO
from dateutil.relativedelta import relativedelta

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
    buffer.seek(0)
    return buffer.getvalue()


@st.cache_resource
def get_connection(http_path: str):
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate(),
    )


def get_http_path() -> str | None:
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        return f"/sql/1.0/warehouses/{warehouse_id}"
    return os.getenv("DATABRICKS_HTTP_PATH") or None


def year_bucket(forecast_start: date, month_date: date) -> str:
    """Categorize forecast month as Yr1 (first 12), Yr2 (next 12), etc."""
    if month_date < forecast_start:
        return "Historical"
    delta_months = (month_date.year - forecast_start.year) * 12 + (month_date.month - forecast_start.month)
    year_num = (delta_months // 12) + 1
    return f"Yr{year_num}"


# -----------------------------------------------------------------------------
# Page config and connection
# -----------------------------------------------------------------------------

st.set_page_config(page_title="SFDC Consumption & Forecast", layout="wide")
st.title("SFDC Account Consumption & Forecast Dashboard")

http_path = get_http_path()
if not http_path:
    http_path = st.sidebar.text_input(
        "SQL Warehouse HTTP Path",
        value="",
        placeholder="/sql/1.0/warehouses/xxxxxxxx",
    )
    if not http_path:
        st.info("Configure a SQL warehouse resource or enter the HTTP path in the sidebar.")
        st.stop()

# -----------------------------------------------------------------------------
# Parameters (sidebar)
# -----------------------------------------------------------------------------

with st.sidebar:
    st.header("Parameters")
    sfdc_account_id = st.text_input(
        "SFDC Account ID",
        value="",
        placeholder="e.g. 001xxxxxxxxxxxx",
        help="Required. Used to filter paid_usage_metering.",
    )
    st.subheader("Consumption report (historical)")
    historical_start = st.date_input("Historical Start", value=None)
    historical_end = st.date_input("Historical End", value=None)
    st.subheader("Forecast period")
    forecast_start = st.date_input("Forecast Month Start", value=None)
    forecast_end = st.date_input("Forecast Month End", value=None)
    st.subheader("Organic growth % (forecast scenario)")
    organic_growth_yr1 = st.number_input("Yr1 growth %", min_value=-100.0, max_value=500.0, value=5.0, step=0.5)
    organic_growth_yr2 = st.number_input("Yr2 growth %", min_value=-100.0, max_value=500.0, value=5.0, step=0.5)
    organic_growth_yr3 = st.number_input("Yr3 growth %", min_value=-100.0, max_value=500.0, value=5.0, step=0.5)
    result_limit = st.number_input("Max rows (consumption)", min_value=100, max_value=10000, value=1000)
    catalog = st.text_input("Catalog", value="main")
    schema_fin = st.text_input("Schema (fin_live_gold)", value="fin_live_gold")

# Defaults
if historical_start is None:
    historical_start = date.today() - relativedelta(months=12)
if historical_end is None:
    historical_end = date.today()
if forecast_start is None:
    forecast_start = date.today() + relativedelta(months=1)
if forecast_end is None:
    forecast_end = forecast_start + relativedelta(months=35)  # ~3 years

if not sfdc_account_id or not sfdc_account_id.strip():
    st.warning("Enter an SFDC Account ID in the sidebar.")
    st.stop()

account_id_escaped = sfdc_account_id.strip().replace("'", "''")
table = f"`{catalog}`.`{schema_fin}`.`paid_usage_metering`"
hist_start_s = historical_start.isoformat()
hist_end_s = historical_end.isoformat()

# -----------------------------------------------------------------------------
# Historical consumption query
# -----------------------------------------------------------------------------

historical_sql = f"""
SELECT
    sfdc_account_id,
    sfdc_workspace_id,
    sfdc_workspace_name,
    sku_name,
    DATE_TRUNC('MONTH', date) AS month,
    usage_quantity,
    usage_dollars,
    usage_dollars_at_list
FROM {table}
WHERE sfdc_account_id = '{account_id_escaped}'
    AND date >= '{hist_start_s}'
    AND date <= '{hist_end_s}'
ORDER BY month DESC, usage_dollars DESC
LIMIT {int(result_limit)}
"""

try:
    conn = get_connection(http_path)
except Exception as e:
    st.error(f"Could not connect to SQL warehouse: {e}")
    st.stop()


def run_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

# -----------------------------------------------------------------------------
# Historical consumption report
# -----------------------------------------------------------------------------

st.subheader("Historical consumption report")
with st.spinner("Loading consumption…"):
    try:
        df_hist = run_query(conn, historical_sql)
        if df_hist.empty:
            st.warning("No rows returned. Check SFDC Account ID and date range.")
        else:
            # Ensure month is date-like for display
            if "month" in df_hist.columns and hasattr(df_hist["month"].iloc[0], "date"):
                df_hist["month"] = pd.to_datetime(df_hist["month"]).dt.date
            st.dataframe(df_hist, use_container_width=True)
            xlsx_hist = dataframe_to_xlsx_bytes(df_hist)
            st.download_button(
                label="Download as XLS",
                data=xlsx_hist,
                file_name="consumption_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_hist",
            )
    except Exception as e:
        st.error(f"Query failed: {e}")
        df_hist = pd.DataFrame()

# -----------------------------------------------------------------------------
# Forecast scenario: months + Yr1/Yr2/Yr3 + organic growth
# -----------------------------------------------------------------------------

st.subheader("Forecast scenario (by year bucket)")
st.caption("Forecast months from Forecast Month Start to End, categorized as Yr1 (first 12 months), Yr2 (next 12), Yr3 (next 12). Projected usage applies organic growth per year.")

# Build list of forecast months
forecast_months = []
d = date(forecast_start.year, forecast_start.month, 1)
end = date(forecast_end.year, forecast_end.month, 1)
while d <= end:
    forecast_months.append(d)
    d = d + relativedelta(months=1)

# Baseline: last month's total usage_dollars from historical (if we have it)
baseline_dollars = 0.0
if not df_hist.empty and "usage_dollars" in df_hist.columns and "month" in df_hist.columns:
    df_hist_copy = df_hist.copy()
    df_hist_copy["month"] = pd.to_datetime(df_hist_copy["month"])
    last_month = df_hist_copy["month"].max()
    baseline_dollars = float(df_hist_copy[df_hist_copy["month"] == last_month]["usage_dollars"].sum())
if baseline_dollars == 0:
    baseline_dollars = 1.0  # placeholder so we still show growth

# Apply organic growth by year: Yr1 = baseline * (1+g1), Yr2 = baseline * (1+g1)*(1+g2), etc.
growth_by_yr = {1: organic_growth_yr1 / 100.0, 2: organic_growth_yr2 / 100.0, 3: organic_growth_yr3 / 100.0}
compound = 1.0
rows = []
for i, month_start in enumerate(forecast_months):
    bucket = year_bucket(forecast_start, month_start)
    if bucket == "Historical":
        continue
    yr_num = int(bucket.replace("Yr", ""))
    growth_pct = growth_by_yr.get(yr_num, growth_by_yr.get(3, 0.05))
    # Start of new year: compound previous years' growth
    if i % 12 == 0 and yr_num > 1:
        compound *= 1.0 + growth_by_yr.get(yr_num - 1, 0.05)
    projected = baseline_dollars * compound * (1.0 + growth_pct)
    rows.append({
        "month": month_start,
        "year_bucket": bucket,
        "organic_growth_pct": round(growth_pct * 100, 2),
        "projected_usage_dollars": round(projected, 2),
    })

if rows:
    df_forecast = pd.DataFrame(rows)
    st.dataframe(df_forecast, use_container_width=True)
    xlsx_fc = dataframe_to_xlsx_bytes(df_forecast)
    st.download_button(
        label="Download forecast as XLS",
        data=xlsx_fc,
        file_name="forecast_scenario.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_forecast",
    )
else:
    st.info("No forecast months in range. Adjust Forecast Month Start/End.")
