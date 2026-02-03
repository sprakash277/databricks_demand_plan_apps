-- Dataset: Historical consumption report
-- Use in Databricks AI/BI Dashboard as a dataset with parameters.
-- Parameters to add in the Data tab: :sfdc_account_id (String), :historical_start (Date), :historical_end (Date)
-- Source: main.fin_live_gold.paid_usage_metering (adjust catalog/schema if needed)

SELECT
    sfdc_account_id,
    sfdc_workspace_id,
    sfdc_workspace_name,
    sku_name,
    DATE_TRUNC('MONTH', date) AS month,
    usage_quantity,
    usage_dollars,
    usage_dollars_at_list
FROM main.fin_live_gold.paid_usage_metering
WHERE sfdc_account_id = :sfdc_account_id
    AND date >= :historical_start
    AND date <= :historical_end
ORDER BY month DESC, usage_dollars DESC
LIMIT 5000
