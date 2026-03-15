"""
Label generation and evaluation helpers for training.
In production, labels come from a confirmed fraud feedback table.
Here we use a rule-based heuristic to simulate labels for the portfolio.
"""
import pandas as pd
import numpy as np


def build_label_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates synthetic fraud labels based on risk signal combinations.
    Used only when no real label table exists (portfolio / demo mode).
    High-risk heuristic: many devices + high international ratio + high amounts.
    """
    rng = np.random.default_rng(42)

    risk_score = (
        (df["unique_devices_30d"] > 3).astype(float) * 0.3
        + (df["intl_tx_count_30d"] / df["tx_count_30d"].clip(lower=1) > 0.5).astype(float) * 0.4
        + (df["high_amount_count_30d"] / df["tx_count_30d"].clip(lower=1) > 0.3).astype(float) * 0.3
    )

    fraud_prob  = (risk_score * 0.4).clip(0, 1)
    df["label"] = (rng.random(len(df)) < fraud_prob).astype(int)
    return df
