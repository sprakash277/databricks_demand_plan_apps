-- Dataset: Baseline (last month total usage_dollars) for forecast scenario
-- Parameters: :sfdc_account_id (String), :historical_end (Date)
-- Used to derive projected_usage_dollars in forecast scenario.

SELECT
    COALESCE(SUM(usage_dollars), 0) AS baseline_usage_dollars
FROM main.fin_live_gold.paid_usage_metering
WHERE sfdc_account_id = :sfdc_account_id
    AND date >= ADD_MONTHS(DATE_TRUNC('MONTH', :historical_end), -1)
    AND date <= :historical_end
