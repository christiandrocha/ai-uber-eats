{{
  config(
    materialized         = 'incremental',
    unique_key           = 'payment_date',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns',
    merge_exclude_columns = ['_computed_at']
  )
}}

/*
  Daily payment funnel metrics.

  Recomputes the last 3 days on each incremental run to absorb late-arriving
  events (e.g., a Bronze file for day D-1 processed on day D).
*/

SELECT
    DATE(created_at)                                                               AS payment_date,
    COUNT(payment_id)                                                              AS total_payments,
    COUNT_IF(payment_status = 'captured')                                          AS captured_payments,
    COUNT_IF(payment_status = 'authorized')                                        AS authorized_payments,
    COUNT_IF(payment_status = 'created')                                           AS pending_payments,
    ROUND(COUNT_IF(payment_status = 'captured') * 100.0 / COUNT(payment_id), 2)   AS capture_rate_pct,
    ROUND(AVG(auth_time_seconds),              2)                                  AS avg_auth_time_seconds,
    ROUND(AVG(capture_time_seconds),           2)                                  AS avg_capture_time_seconds,
    ROUND(AVG(total_processing_time_seconds),  2)                                  AS avg_total_time_seconds,
    current_timestamp()                                                            AS _computed_at

FROM {{ ref('gold_payment_summary') }}

WHERE created_at IS NOT NULL

{% if is_incremental() %}
  AND DATE(created_at) >= (
      SELECT MAX(payment_date) - INTERVAL 3 DAYS
      FROM {{ this }}
  )
{% endif %}

GROUP BY DATE(created_at)
