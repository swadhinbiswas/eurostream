SELECT
    country,
    count(*) AS customer_count,
    sum(CASE WHEN consents_marketing THEN 1 ELSE 0 END) AS opted_in_count,
    sum(CASE WHEN NOT consents_marketing THEN 1 ELSE 0 END) AS opted_out_count,
    round(100.0 * sum(CASE WHEN consents_marketing THEN 1 ELSE 0 END) / nullif(count(*), 0), 1) AS consent_rate_pct
FROM eurostream.gold.customer_360
GROUP BY country
ORDER BY customer_count DESC, country;
