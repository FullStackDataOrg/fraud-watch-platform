select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Fail if any hour has fraud_rate > 50%: signals bad labels or pipeline error.
-- Returns rows on failure (dbt test passes when 0 rows returned).

select
    hour_bucket,
    fraud_rate
from "delta"."bronze_marts"."mart_hourly_metrics"
where fraud_rate > 0.50
      
    ) dbt_internal_test