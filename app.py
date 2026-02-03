"""
C360 Consumption Analytics — Databricks App (Streamlit).

Query account-level consumption with historical MoM growth, organic baseline,
and forecast comparison. Source: main.gtm_data.c360_consumption_account_monthly

Deploy as a Databricks App: https://docs.databricks.com/aws/en/dev-tools/databricks-apps
"""

import os
from datetime import date
from io import BytesIO
from typing import Optional

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Write DataFrame to Excel (.xlsx) and return bytes for download."""
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
    buffer.seek(0)
    return buffer.getvalue()

# -----------------------------------------------------------------------------
# Connection (same-workspace or other-workspace SQL warehouse)
# Uses: from databricks import sql; sql.connect(server_hostname=..., http_path=..., access_token=...)
# -----------------------------------------------------------------------------

def _is_remote_workspace() -> bool:
    """True if REMOTE_WORKSPACE_HOST and REMOTE_HTTP_PATH are set (other-workspace mode)."""
    return bool(os.getenv("REMOTE_WORKSPACE_HOST") and os.getenv("REMOTE_HTTP_PATH"))


def _get_access_token(use_remote: bool) -> Optional[str]:
    """Resolve access_token for sql.connect. Remote: REMOTE_DATABRICKS_TOKEN; same-workspace: Config or DATABRICKS_TOKEN. Returns None if same-workspace and no token (caller may use credentials_provider)."""
    if use_remote:
        token = (os.getenv("REMOTE_DATABRICKS_TOKEN") or "").strip()
        if not token:
            raise ValueError("REMOTE_DATABRICKS_TOKEN is required for other-workspace connection.")
        return token
    cfg = Config()
    auth = cfg.authenticate() if hasattr(cfg, "authenticate") and callable(cfg.authenticate) else None
    if isinstance(auth, dict) and auth.get("access_token"):
        return auth["access_token"]
    token = getattr(cfg, "token", None) or os.getenv("DATABRICKS_TOKEN", "").strip()
    return token if token else None


@st.cache_resource
def get_connection(server_hostname: str, http_path: str, use_remote_token: bool = False):
    """Connect to a SQL warehouse using databricks.sql.connect(server_hostname, http_path, access_token)."""
    access_token = _get_access_token(use_remote_token)
    if access_token is not None:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )
    else:
        # Same-workspace fallback when no token (e.g. app uses SQL warehouse resource only)
        cfg = Config()
        connection = sql.connect(
            server_hostname=cfg.host,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate(),
        )
    return connection


def get_connection_params() -> tuple[str, str, bool]:
    """
    Return (server_hostname, http_path, use_remote_token).
    - Other-workspace: REMOTE_WORKSPACE_HOST, REMOTE_HTTP_PATH, REMOTE_DATABRICKS_TOKEN (or valueFrom secret).
    - Same-workspace: app host, DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH.
    """
    if _is_remote_workspace():
        host = os.getenv("REMOTE_WORKSPACE_HOST", "").strip()
        path = os.getenv("REMOTE_HTTP_PATH", "").strip()
        return host, path, True
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else (os.getenv("DATABRICKS_HTTP_PATH") or "")
    cfg = Config()
    return cfg.host, http_path, False


# -----------------------------------------------------------------------------
# Parameters (sidebar)
# -----------------------------------------------------------------------------

st.set_page_config(page_title="C360 Consumption Analytics", layout="wide")
st.title("C360 Consumption Analytics")

server_hostname, http_path, use_remote = get_connection_params()

if not http_path:
    if _is_remote_workspace():
        st.warning("REMOTE_WORKSPACE_HOST and REMOTE_HTTP_PATH are set but REMOTE_HTTP_PATH is empty. Set REMOTE_HTTP_PATH (e.g. /sql/1.0/warehouses/<id>) in app env.")
        st.stop()
    http_path = st.sidebar.text_input(
        "SQL Warehouse HTTP Path",
        value="",
        placeholder="/sql/1.0/warehouses/xxxxxxxx",
        help="Required if no SQL warehouse resource is configured for this app.",
    )
    if not http_path:
        st.info("Configure a SQL warehouse resource for this app, or enter the HTTP path in the sidebar.")
        st.stop()
    server_hostname = Config().host
elif use_remote and not server_hostname:
    st.warning("REMOTE_HTTP_PATH is set but REMOTE_WORKSPACE_HOST is empty. Set both in app env.")
    st.stop()

if use_remote and not (os.getenv("REMOTE_DATABRICKS_TOKEN") or "").strip():
    st.warning("Other-workspace mode is on but REMOTE_DATABRICKS_TOKEN is not set. Add a secret resource and set env REMOTE_DATABRICKS_TOKEN with valueFrom to that secret.")
    st.stop()

with st.sidebar:
    if use_remote:
        st.caption("Using SQL warehouse in another workspace (REMOTE_* env).")
    st.header("Parameters")
    account_name = st.text_input(
        "Account Name (ILIKE)",
        value="%",
        help="e.g. %Kroger%, %Databricks%. Use % for all.",
    )
    col1, col2 = st.columns(2)
    with col1:
        historical_start = st.date_input("Historical Start", value=None)
        forecast_start = st.date_input("Forecast Start", value=None)
    with col2:
        historical_end = st.date_input("Historical End", value=None)
        forecast_end = st.date_input("Forecast End", value=None)
    organic_mom_min = st.text_input(
        "Min MoM Growth % (optional)",
        value="",
        help="Filter rows where MoM growth >= this value. Leave empty for no filter.",
    )
    result_limit = st.number_input("Max rows", min_value=1, max_value=10000, value=500)
    catalog = st.text_input("Catalog", value="main")
    schema = st.text_input("Schema", value="gtm_data")

# Default dates if not set
if historical_start is None:
    historical_start = date(2024, 1, 1)
if historical_end is None:
    historical_end = date(2026, 1, 1)
if forecast_start is None:
    forecast_start = date(2025, 1, 1)
if forecast_end is None:
    forecast_end = date(2026, 6, 1)

historical_start_s = historical_start.isoformat()
historical_end_s = historical_end.isoformat()
forecast_start_s = forecast_start.isoformat()
forecast_end_s = forecast_end.isoformat()

organic_mom_min_f = None
if organic_mom_min and organic_mom_min.strip():
    try:
        organic_mom_min_f = float(organic_mom_min.strip())
    except ValueError:
        pass

account_name_escaped = (account_name or "%").strip().replace("'", "''")
table = f"`{catalog}`.`{schema}`.`c360_consumption_account_monthly`"

# -----------------------------------------------------------------------------
# Historical consumption query
# -----------------------------------------------------------------------------

historical_sql = f"""
WITH historical_consumption AS (
  SELECT
    `account_id`,
    `account_name`,
    `usage_date`,
    `dbu_dollars`,
    `organic_growth_baseline`,
    `forecast_consumption_ds`,
    `submitted_ae_forecast`,
    `business_unit`,
    `BU1`,
    LAG(`dbu_dollars`, 1) OVER (
        PARTITION BY `account_id`
        ORDER BY `usage_date`
      ) AS prev_month_dbu_dollars,
    CASE
      WHEN LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`) > 0
      THEN (
          (`dbu_dollars` - LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`))
          / LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`)
        ) * 100
      ELSE NULL
    END AS mom_growth_pct
  FROM {table}
  WHERE `account_name` ILIKE '{account_name_escaped}'
    AND `usage_date` >= '{historical_start_s}'
    AND `usage_date` <= '{historical_end_s}'
)
SELECT
  `account_name`,
  `usage_date`,
  ROUND(`dbu_dollars`, 2) AS dbu_dollars,
  ROUND(`prev_month_dbu_dollars`, 2) AS prev_month_dbu_dollars,
  ROUND(`mom_growth_pct`, 2) AS mom_growth_pct,
  ROUND(`organic_growth_baseline`, 2) AS organic_growth_baseline,
  ROUND(`forecast_consumption_ds`, 2) AS forecast_consumption_ds,
  ROUND(`submitted_ae_forecast`, 2) AS submitted_ae_forecast,
  `business_unit`,
  `BU1`
FROM historical_consumption
WHERE `dbu_dollars` > 0
"""
if organic_mom_min_f is not None:
    historical_sql += f"\n  AND (`mom_growth_pct` IS NULL OR `mom_growth_pct` >= {organic_mom_min_f})"
historical_sql += f"""
ORDER BY `account_name`, `usage_date` DESC
LIMIT {int(result_limit)}
"""

# -----------------------------------------------------------------------------
# Forecast period query
# -----------------------------------------------------------------------------

forecast_sql = f"""
SELECT
  `account_name`,
  `usage_date`,
  ROUND(`dbu_dollars`, 2) AS dbu_dollars,
  ROUND(`organic_growth_baseline`, 2) AS organic_growth_baseline,
  ROUND(`forecast_consumption_ds`, 2) AS forecast_consumption_ds,
  ROUND(`submitted_ae_forecast`, 2) AS submitted_ae_forecast,
  `business_unit`,
  `BU1`
FROM {table}
WHERE `account_name` ILIKE '{account_name_escaped}'
  AND `usage_date` >= '{forecast_start_s}'
  AND `usage_date` <= '{forecast_end_s}'
ORDER BY `account_name`, `usage_date`
LIMIT {int(result_limit)}
"""

# -----------------------------------------------------------------------------
# Run and display
# -----------------------------------------------------------------------------

try:
    conn = get_connection(server_hostname, http_path, use_remote)
except Exception as e:
    st.error(f"Could not connect to SQL warehouse: {e}")
    st.stop()

def run_query(connection, query, label: str):
    """Execute SQL and return a DataFrame. Uses cursor.execute / fetchall_arrow pattern."""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()
    finally:
        cursor.close()

st.subheader("Historical consumption (MoM growth)")
with st.spinner("Running historical consumption query…"):
    try:
        df_hist = run_query(conn, historical_sql, "historical")
        st.dataframe(df_hist, use_container_width=True)
        if not df_hist.empty:
            xlsx_hist = dataframe_to_xlsx_bytes(df_hist)
            st.download_button(
                label="Download as XLS",
                data=xlsx_hist,
                file_name="historical_consumption.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_historical",
            )
    except Exception as e:
        st.error(f"Query failed: {e}")
        raise

st.subheader("Forecast period (actual vs forecast)")
with st.spinner("Running forecast-period query…"):
    try:
        df_forecast = run_query(conn, forecast_sql, "forecast")
        st.dataframe(df_forecast, use_container_width=True)
        if not df_forecast.empty:
            xlsx_forecast = dataframe_to_xlsx_bytes(df_forecast)
            st.download_button(
                label="Download as XLS",
                data=xlsx_forecast,
                file_name="forecast_period.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_forecast",
            )
    except Exception as e:
        st.error(f"Query failed: {e}")
