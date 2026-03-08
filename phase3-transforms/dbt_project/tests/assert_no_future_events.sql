-- Fail if any transaction has event_at more than 1 hour in the future.
-- Catches clock skew on producers or corrupted event_time values.

select
    transaction_id,
    event_at,
    current_timestamp as checked_at
from {{ ref('stg_transactions') }}
where event_at > current_timestamp + interval '1' hour
