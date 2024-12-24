WITH ranked AS (
    SELECT
        check_name,
        passed,
        detail,
        try_cast(checked_at AS TIMESTAMP) AS checked_at,
        row_number() OVER (PARTITION BY check_name ORDER BY checked_at DESC) AS recency_rank
    FROM eurostream.governance.quality_results
)
SELECT check_name, passed, detail, checked_at
FROM ranked
WHERE recency_rank = 1
ORDER BY check_name;
