-- @param customerId STRING = 'cust_000000'

SELECT
    coalesce(max(CASE WHEN table_name = 'silver.customers' THEN row_count ELSE 0 END), 0)
        AS silver_customer_rows,
    coalesce(max(CASE WHEN table_name = 'silver.orders' THEN row_count ELSE 0 END), 0)
        AS silver_order_rows,
    coalesce(max(CASE WHEN table_name = 'silver.payments' THEN row_count ELSE 0 END), 0)
        AS silver_payment_rows,
    coalesce(max(CASE WHEN table_name = 'silver.orders_quarantine' THEN row_count ELSE 0 END), 0)
        AS silver_order_quarantine_rows,
    coalesce(max(CASE WHEN table_name = 'silver.payments_quarantine' THEN row_count ELSE 0 END), 0)
        AS silver_payment_quarantine_rows,
    coalesce(max(CASE WHEN table_name = 'gold.customer_360' THEN row_count ELSE 0 END), 0)
        AS gold_customer_rows,
    coalesce(max(CASE WHEN table_name = 'gold.order_facts' THEN row_count ELSE 0 END), 0)
        AS gold_order_rows,
    coalesce(max(CASE WHEN table_name = 'gold.fraud_summary' THEN row_count ELSE 0 END), 0)
        AS fraud_summary_rows,
    EXISTS (
        SELECT 1
        FROM eurostream.governance.suppression_registry
        WHERE customer_id = :customerId
    ) AS already_suppressed
FROM eurostream.governance.erasure_impact_counts
WHERE customer_id = :customerId;
