-- Gold: composite risk score per user, combining velocity and merchant signals.
-- Incremental to avoid S3A directory-rename limitation with table materialization.



with features as (
    select * from "delta"."bronze_marts"."mart_fraud_features"
),

risk as (
    select
        user_id,
        tx_count_30d,
        avg_amount_30d,
        max_amount_30d,
        unique_devices_30d,
        unique_countries_30d,
        intl_tx_count_30d,
        high_amount_count_30d,
        last_tx_at,
        feature_updated_at,
        -- Simple rule-based risk tier before ML model is available
        case
            when unique_countries_30d > 3
              or unique_devices_30d > 5
              or (intl_tx_count_30d / nullif(tx_count_30d, 0)) > 0.5
              or high_amount_count_30d > 5  then 'high'
            when unique_devices_30d > 2
              or (intl_tx_count_30d / nullif(tx_count_30d, 0)) > 0.2
              or high_amount_count_30d > 2  then 'medium'
            else                                 'low'
        end                             as risk_tier
    from features
)

select * from risk