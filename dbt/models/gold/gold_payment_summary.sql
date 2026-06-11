{{
  config(
    materialized         = 'incremental',
    unique_key           = 'payment_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns',
    merge_exclude_columns = ['_computed_at']
  )
}}

/*
  Gold layer — one row per payment_id.

  Pivots Silver events into lifecycle columns using conditional aggregation,
  derives payment_status (captured > authorized > created), and computes
  three timing metrics.

  Incremental strategy: on reruns, only recomputes payment_ids that had new
  Silver activity since the last Gold computation, avoiding a full table scan.
*/

SELECT
    payment_id,

    -- Lifecycle timestamps (one per event_name via conditional aggregation)
    MAX(CASE WHEN event_name = 'created'    THEN event_timestamp END) AS created_at,
    MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END) AS authorized_at,
    MAX(CASE WHEN event_name = 'captured'   THEN event_timestamp END) AS captured_at,
    MAX(CASE WHEN event_name = 'closed'     THEN event_timestamp END) AS closed_at,
    MAX(CASE WHEN event_name = 'succeeded'  THEN event_timestamp END) AS succeeded_at,
    MAX(CASE WHEN event_name = 'settled'    THEN event_timestamp END) AS settled_at,
    MAX(CASE WHEN event_name = 'refunded'   THEN event_timestamp END) AS refunded_at,

    COUNT(event_id)                                                    AS event_count,

    -- Payment status: most advanced lifecycle state wins
    -- Success path:  created → authorized → captured → closed → succeeded → settled
    -- Refund path:   … → refunded (terminal, separate from settled)
    CASE
        WHEN MAX(CASE WHEN event_name = 'settled'    THEN event_timestamp END) IS NOT NULL THEN 'settled'
        WHEN MAX(CASE WHEN event_name = 'refunded'   THEN event_timestamp END) IS NOT NULL THEN 'refunded'
        WHEN MAX(CASE WHEN event_name = 'succeeded'  THEN event_timestamp END) IS NOT NULL THEN 'succeeded'
        WHEN MAX(CASE WHEN event_name = 'closed'     THEN event_timestamp END) IS NOT NULL THEN 'closed'
        WHEN MAX(CASE WHEN event_name = 'captured'   THEN event_timestamp END) IS NOT NULL THEN 'captured'
        WHEN MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END) IS NOT NULL THEN 'authorized'
        ELSE 'created'
    END AS payment_status,

    -- Time from creation to authorization (null if not yet authorized)
    CASE
        WHEN MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END) IS NOT NULL
         AND MAX(CASE WHEN event_name = 'created'    THEN event_timestamp END) IS NOT NULL
        THEN unix_timestamp(MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END))
           - unix_timestamp(MAX(CASE WHEN event_name = 'created'    THEN event_timestamp END))
    END AS auth_time_seconds,

    -- Time from authorization to capture (null if not yet captured)
    CASE
        WHEN MAX(CASE WHEN event_name = 'captured'   THEN event_timestamp END) IS NOT NULL
         AND MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END) IS NOT NULL
        THEN unix_timestamp(MAX(CASE WHEN event_name = 'captured'   THEN event_timestamp END))
           - unix_timestamp(MAX(CASE WHEN event_name = 'authorized' THEN event_timestamp END))
    END AS capture_time_seconds,

    -- End-to-end processing time: created → settled (or captured if not yet settled)
    CASE
        WHEN MAX(CASE WHEN event_name = 'settled'  THEN event_timestamp END) IS NOT NULL
         AND MAX(CASE WHEN event_name = 'created'  THEN event_timestamp END) IS NOT NULL
        THEN unix_timestamp(MAX(CASE WHEN event_name = 'settled' THEN event_timestamp END))
           - unix_timestamp(MAX(CASE WHEN event_name = 'created' THEN event_timestamp END))
        WHEN MAX(CASE WHEN event_name = 'captured' THEN event_timestamp END) IS NOT NULL
         AND MAX(CASE WHEN event_name = 'created'  THEN event_timestamp END) IS NOT NULL
        THEN unix_timestamp(MAX(CASE WHEN event_name = 'captured' THEN event_timestamp END))
           - unix_timestamp(MAX(CASE WHEN event_name = 'created'  THEN event_timestamp END))
    END AS total_processing_time_seconds,

    current_timestamp() AS _computed_at

FROM {{ source('payments', 'silver_payment_events') }}

{% if is_incremental() %}
-- Incremental: recompute only payment_ids with Silver activity since last run
WHERE payment_id IN (
    SELECT DISTINCT payment_id
    FROM {{ source('payments', 'silver_payment_events') }}
    WHERE _silver_processed_at > (
        SELECT COALESCE(MAX(_computed_at), CAST('1900-01-01' AS TIMESTAMP))
        FROM {{ this }}
    )
)
{% endif %}

GROUP BY payment_id
