/*
  Custom test: time metrics must never be negative.

  A negative value means captured_at < created_at (or authorized_at < created_at),
  which indicates out-of-order event delivery or a source data issue.
  Returns rows that fail the assertion — dbt marks the test as failed if any rows are returned.
*/

SELECT
    payment_id,
    auth_time_seconds,
    capture_time_seconds,
    total_processing_time_seconds
FROM {{ ref('gold_payment_summary') }}
WHERE
       (auth_time_seconds              IS NOT NULL AND auth_time_seconds              < 0)
    OR (capture_time_seconds           IS NOT NULL AND capture_time_seconds           < 0)
    OR (total_processing_time_seconds  IS NOT NULL AND total_processing_time_seconds  < 0)
