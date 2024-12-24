SELECT
    customer_id,
    coalesce(velocity_alerts, 0) AS velocity_alerts,
    coalesce(zscore_alerts, 0) AS zscore_alerts,
    coalesce(geo_alerts, 0) AS geo_alerts,
    coalesce(velocity_alerts, 0) + coalesce(zscore_alerts, 0) + coalesce(geo_alerts, 0) AS total_alerts,
    max_score,
    last_alert_at
FROM eurostream.gold.fraud_summary
WHERE coalesce(velocity_alerts, 0) + coalesce(zscore_alerts, 0) + coalesce(geo_alerts, 0) > 0
ORDER BY total_alerts DESC, last_alert_at DESC, customer_id
LIMIT 100;
