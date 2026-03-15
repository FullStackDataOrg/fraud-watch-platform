"""
Shared feature engineering — imported by both training and serving.
Never duplicate this logic. Any change here affects both contexts.
"""
from dataclasses import dataclass, asdict
import numpy as np


@dataclass
class UserFeatures:
    tx_count_30d: float
    avg_amount_30d: float
    stddev_amount_30d: float
    max_amount_30d: float
    unique_devices_30d: float
    unique_countries_30d: float
    intl_tx_count_30d: float
    high_amount_count_30d: float

    def to_array(self) -> np.ndarray:
        return np.array([
            self.tx_count_30d,
            self.avg_amount_30d,
            self.stddev_amount_30d or 0.0,
            self.max_amount_30d,
            self.unique_devices_30d,
            self.unique_countries_30d,
            self.intl_tx_count_30d / max(self.tx_count_30d, 1),
            self.high_amount_count_30d / max(self.tx_count_30d, 1),
        ]).reshape(1, -1)

    def to_dict(self) -> dict:
        return asdict(self)


FEATURE_NAMES = [
    "tx_count_30d",
    "avg_amount_30d",
    "stddev_amount_30d",
    "max_amount_30d",
    "unique_devices_30d",
    "unique_countries_30d",
    "intl_tx_ratio_30d",
    "high_amount_ratio_30d",
]


def from_dict(d: dict) -> UserFeatures:
    return UserFeatures(
        tx_count_30d=float(d.get("tx_count_30d") or 0),
        avg_amount_30d=float(d.get("avg_amount_30d") or 0),
        stddev_amount_30d=float(d.get("stddev_amount_30d") or 0),
        max_amount_30d=float(d.get("max_amount_30d") or 0),
        unique_devices_30d=float(d.get("unique_devices_30d") or 0),
        unique_countries_30d=float(d.get("unique_countries_30d") or 0),
        intl_tx_count_30d=float(d.get("intl_tx_count_30d") or 0),
        high_amount_count_30d=float(d.get("high_amount_count_30d") or 0),
    )


DEFAULT_FEATURES = UserFeatures(
    tx_count_30d=0, avg_amount_30d=0, stddev_amount_30d=0,
    max_amount_30d=0, unique_devices_30d=1, unique_countries_30d=1,
    intl_tx_count_30d=0, high_amount_count_30d=0,
)
