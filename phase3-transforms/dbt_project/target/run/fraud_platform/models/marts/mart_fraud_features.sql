insert into "delta"."gold"."mart_fraud_features" ("user_id", "tx_count_30d", "avg_amount_30d", "stddev_amount_30d", "max_amount_30d", "unique_devices_30d", "unique_countries_30d", "intl_tx_count_30d", "high_amount_count_30d", "last_tx_at", "feature_updated_at")
    (
        select "user_id", "tx_count_30d", "avg_amount_30d", "stddev_amount_30d", "max_amount_30d", "unique_devices_30d", "unique_countries_30d", "intl_tx_count_30d", "high_amount_count_30d", "last_tx_at", "feature_updated_at"
        from "delta"."gold"."mart_fraud_features__dbt_tmp"
    )

