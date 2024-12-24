-- @param searchTerm STRING = ''
-- @param country STRING = ''
-- @param consentFilter STRING = 'ALL'
-- @param limit INT = 50
-- @param offset INT = 0

SELECT
    c.customer_id,
    c.country,
    coalesce(c.order_count, 0) AS order_count,
    coalesce(c.lifetime_spend_eur, 0.0) AS lifetime_spend_eur,
    c.lifetime_spend_eur / nullif(c.order_count, 0) AS average_order_value_eur,
    c.consents_marketing AS marketing_consent,
    coalesce(f.velocity_alerts, 0) AS velocity_alerts,
    coalesce(f.zscore_alerts, 0) AS zscore_alerts,
    coalesce(f.geo_alerts, 0) AS geo_alerts,
    coalesce(f.velocity_alerts, 0) + coalesce(f.zscore_alerts, 0) + coalesce(f.geo_alerts, 0) AS fraud_alert_count,
    f.last_alert_at,
    s.customer_id IS NOT NULL AS suppressed,
    c.last_order_at
FROM eurostream.gold.customer_360 c
LEFT JOIN eurostream.gold.fraud_summary f
    ON c.customer_id = f.customer_id
LEFT JOIN eurostream.governance.suppression_registry s
    ON c.customer_id = s.customer_id
WHERE (:searchTerm = '' OR contains(lower(c.customer_id), lower(:searchTerm)))
  AND (:country = '' OR c.country = :country)
  AND (
      :consentFilter = 'ALL'
      OR (:consentFilter = 'OPTED_IN' AND c.consents_marketing = true)
      OR (:consentFilter = 'OPTED_OUT' AND c.consents_marketing = false)
  )
ORDER BY c.customer_id
LIMIT :limit OFFSET :offset;
