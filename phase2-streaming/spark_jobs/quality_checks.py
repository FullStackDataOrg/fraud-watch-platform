"""Quality gate: validate transaction fields, return (valid_df, invalid_df)."""
from typing import Callable, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.column import Column

# Lambdas defer Column creation until a SparkSession is active.
RULES: Dict[str, Callable[[], Column]] = {
    "amount_positive":  lambda: col("amount") > 0,
    "user_id_not_null": lambda: col("user_id").isNotNull(),
    "tx_id_not_null":   lambda: col("transaction_id").isNotNull(),
    "valid_event_time": lambda: col("event_time").isNotNull() & (col("event_time") > 0),
}


def split_on_quality(df: DataFrame) -> "tuple[DataFrame, DataFrame]":
    """Returns (valid_df, invalid_df)."""
    combined_check = None
    for rule_fn in RULES.values():
        rule = rule_fn()
        combined_check = rule if combined_check is None else combined_check & rule
    return df.filter(combined_check), df.filter(~combined_check)
