-- Dataset: Forecast scenario by month with Yr1/Yr2/Yr3 and organic growth
-- Parameters: :forecast_start (Date), :forecast_end (Date), :organic_growth_yr1, :organic_growth_yr2, :organic_growth_yr3 (Numeric),
--             :sfdc_account_id (String), :historical_end (Date) for baseline
-- Year buckets: Yr1 = first 12 months from forecast_start, Yr2 = next 12, Yr3 = next 12.
-- Projected = baseline (last month total) × compound growth by year.

WITH baseline AS (
    SELECT COALESCE(SUM(usage_dollars), 1) AS total
    FROM main.fin_live_gold.paid_usage_metering
    WHERE sfdc_account_id = :sfdc_account_id
      AND date >= ADD_MONTHS(DATE_TRUNC('MONTH', :historical_end), -1)
      AND date <= :historical_end
),
forecast_months AS (
    SELECT EXPLODE(
        SEQUENCE(
            DATE_TRUNC('MONTH', :forecast_start),
            DATE_TRUNC('MONTH', :forecast_end),
            INTERVAL 1 MONTH
        )
    ) AS month
),
with_yr AS (
    SELECT
        fm.month,
        CAST(FLOOR(MONTHS_BETWEEN(fm.month, DATE_TRUNC('MONTH', :forecast_start)) / 12) + 1 AS INT) AS yr_num
    FROM forecast_months fm
),
with_growth AS (
    SELECT
        month,
        yr_num,
        CONCAT('Yr', yr_num) AS year_bucket,
        CASE yr_num
            WHEN 1 THEN :organic_growth_yr1
            WHEN 2 THEN :organic_growth_yr2
            ELSE :organic_growth_yr3
        END AS organic_growth_pct,
        b.total AS baseline_dollars
    FROM with_yr
    CROSS JOIN baseline b
    WHERE yr_num >= 1
)
SELECT
    month,
    year_bucket,
    ROUND(organic_growth_pct, 2) AS organic_growth_pct,
    ROUND(
        CASE yr_num
            WHEN 1 THEN baseline_dollars * (1 + :organic_growth_yr1 / 100)
            WHEN 2 THEN baseline_dollars * (1 + :organic_growth_yr1 / 100) * (1 + :organic_growth_yr2 / 100)
            ELSE baseline_dollars * (1 + :organic_growth_yr1 / 100) * (1 + :organic_growth_yr2 / 100) * (1 + :organic_growth_yr3 / 100)
        END,
        2
    ) AS projected_usage_dollars
FROM with_growth
ORDER BY month
