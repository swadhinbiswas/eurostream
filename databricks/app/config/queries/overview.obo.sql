WITH customer_summary AS (
    SELECT
        count(*) AS customer_count,
        sum(CASE WHEN consents_marketing THEN 1 ELSE 0 END) AS consented_customers,
        coalesce(sum(lifetime_spend_eur), 0.0) AS lifetime_spend_eur,
        max(last_order_at) AS latest_order_at
    FROM eurostream.gold.customer_360
),
fraud_summary AS (
    SELECT
        coalesce(sum(velocity_alerts), 0) AS velocity_alerts,
        coalesce(sum(zscore_alerts), 0) AS zscore_alerts,
        coalesce(sum(geo_alerts), 0) AS geo_alerts,
        max(last_alert_at) AS latest_alert_at
    FROM eurostream.gold.fraud_summary
),
quality_summary AS (
    SELECT
        avg(CASE WHEN passed THEN 1.0 ELSE 0.0 END) AS quality_pass_rate,
        max(checked_at) AS last_quality_check_at
    FROM eurostream.governance.quality_results
    WHERE checked_at >= current_timestamp() - INTERVAL 24 HOURS
),
erasure_summary AS (
    SELECT
        count(*) AS erasure_requests,
        sum(CASE WHEN lower(status) = 'completed' THEN 1 ELSE 0 END) AS completed_erasures,
        sum(CASE WHEN lower(status) = 'failed' THEN 1 ELSE 0 END) AS failed_erasures,
        avg(latency_seconds) AS average_latency_seconds
    FROM eurostream.governance.erasure_command_state
    WHERE requested_at >= current_timestamp() - INTERVAL 30 DAYS
),
suppression_summary AS (
    SELECT count(DISTINCT customer_id) AS suppressed_customers
    FROM eurostream.governance.suppression_registry
)
SELECT
    current_timestamp() AS generated_at,
    c.customer_count,
    c.consented_customers,
    c.lifetime_spend_eur,
    c.latest_order_at,
    f.velocity_alerts,
    f.zscore_alerts,
    f.geo_alerts,
    f.velocity_alerts + f.zscore_alerts + f.geo_alerts AS fraud_alert_count,
    f.latest_alert_at,
    q.quality_pass_rate,
    q.last_quality_check_at,
    e.erasure_requests,
    e.completed_erasures,
    e.failed_erasures,
    e.average_latency_seconds,
    s.suppressed_customers
FROM customer_summary c
CROSS JOIN fraud_summary f
CROSS JOIN quality_summary q
CROSS JOIN erasure_summary e
CROSS JOIN suppression_summary s;
