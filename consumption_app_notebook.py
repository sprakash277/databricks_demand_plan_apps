# Databricks notebook source
# MAGIC %md
# MAGIC # C360 Consumption Analytics App
# MAGIC
# MAGIC Query account-level consumption with historical MoM growth, organic baseline, and forecast comparison.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - **Account Name**: Filter by account (ILIKE; e.g. `%Kroger%`, `%Databricks%`)
# MAGIC - **Historical Period**: Start/end dates for `usage_date` (consumption history)
# MAGIC - **Forecast Period**: Start/end months for forecast comparison
# MAGIC - **Organic MoM Growth %** (optional): Minimum MoM growth filter; leave empty for no filter
# MAGIC
# MAGIC **Source**: `{catalog}.{schema}.c360_consumption_account_monthly`

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Parameters (Widgets)

# COMMAND ----------

# Drop widgets if re-running to avoid duplicates
for w in ["account_name", "historical_start", "historical_end", "forecast_start", "forecast_end",
          "organic_mom_growth_min", "result_limit", "catalog", "schema"]:
    try:
        dbutils.widgets.drop(w)
    except Exception:
        pass

dbutils.widgets.text("account_name", "%", "Account Name (ILIKE; e.g. %Kroger% or %Databricks%)")
dbutils.widgets.text("historical_start", "2024-01-01", "Historical Period Start (usage_date >=)")
dbutils.widgets.text("historical_end", "2026-01-01", "Historical Period End (usage_date <=)")
dbutils.widgets.text("forecast_start", "2025-01-01", "Forecast Start Month")
dbutils.widgets.text("forecast_end", "2026-06-01", "Forecast End Month")
dbutils.widgets.text("organic_mom_growth_min", "", "Min Organic MoM Growth % (optional; leave empty for no filter)")
dbutils.widgets.text("result_limit", "500", "Max rows to return")
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "gtm_data", "Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters and Validate

# COMMAND ----------

account_name = dbutils.widgets.get("account_name").strip() or "%"
historical_start = dbutils.widgets.get("historical_start").strip()
historical_end = dbutils.widgets.get("historical_end").strip()
forecast_start = dbutils.widgets.get("forecast_start").strip()
forecast_end = dbutils.widgets.get("forecast_end").strip()
organic_mom_growth_min_str = dbutils.widgets.get("organic_mom_growth_min").strip()
result_limit_str = dbutils.widgets.get("result_limit").strip()
catalog = dbutils.widgets.get("catalog").strip() or "main"
schema = dbutils.widgets.get("schema").strip() or "gtm_data"

result_limit = 500
try:
    result_limit = max(1, min(10000, int(result_limit_str)))
except ValueError:
    result_limit = 500

organic_mom_growth_min = None
if organic_mom_growth_min_str:
    try:
        organic_mom_growth_min = float(organic_mom_growth_min_str)
    except ValueError:
        pass

# Escape single quotes in account_name for SQL
account_name_escaped = account_name.replace("'", "''")
table = f"`{catalog}`.`{schema}`.`c360_consumption_account_monthly`"

print(f"Account filter: {account_name}")
print(f"Historical: {historical_start} to {historical_end}")
print(f"Forecast period: {forecast_start} to {forecast_end}")
print(f"Min MoM growth %: {organic_mom_growth_min if organic_mom_growth_min is not None else 'none'}")
print(f"Table: {table}")
print(f"Limit: {result_limit}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Consumption Query

# COMMAND ----------

# Parameterized SQL: historical consumption with LAG for MoM growth
# We use Python f-strings only for identifiers/dates that we control (widgets); account_name is escaped above.
sql = f"""
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
      WHEN
        LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`) > 0
      THEN
        (
          (
            `dbu_dollars`
            - LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`)
          )
          / LAG(`dbu_dollars`, 1) OVER (PARTITION BY `account_id` ORDER BY `usage_date`)
        )
        * 100
      ELSE NULL
    END AS mom_growth_pct
  FROM
    {table}
  WHERE
    `account_name` ILIKE '{account_name_escaped}'
    AND `usage_date` >= '{historical_start}'
    AND `usage_date` <= '{historical_end}'
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
FROM
  historical_consumption
WHERE
  `dbu_dollars` > 0
"""

# Optional: filter by minimum MoM growth %
if organic_mom_growth_min is not None:
    sql += f"""
  AND (`mom_growth_pct` IS NULL OR `mom_growth_pct` >= {organic_mom_growth_min})
"""

sql += f"""
ORDER BY
  `account_name`,
  `usage_date` DESC
LIMIT {result_limit}
"""

# COMMAND ----------

df = spark.sql(sql)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast Period View (actual vs forecast)
# MAGIC
# MAGIC Months in the forecast range: compare actual consumption to organic baseline and AE forecast.

# COMMAND ----------

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
FROM
  {table}
WHERE
  `account_name` ILIKE '{account_name_escaped}'
  AND `usage_date` >= '{forecast_start}'
  AND `usage_date` <= '{forecast_end}'
ORDER BY
  `account_name`,
  `usage_date`
LIMIT {result_limit}
"""
display(spark.sql(forecast_sql))
