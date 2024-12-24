-- @param limit INT = 25

SELECT
    request_id,
    customer_id,
    status,
    actor AS requested_by,
    ticket_id,
    requested_at,
    completed_at,
    latency_seconds,
    logical_verified,
    physical_verified,
    export_verified,
    CASE
        WHEN physical_verified = TRUE THEN 'FRAUD_SINK_PURGED'
        WHEN status IN ('failed', 'running', 'queued') THEN status
        ELSE 'PENDING'
    END AS physical_purge_status,
    blocked_layer,
    next_action,
    confirmation_hash,
    failed_layer,
    failure_message AS error_detail
FROM eurostream.governance.erasure_command_state
ORDER BY requested_at DESC
LIMIT :limit;
