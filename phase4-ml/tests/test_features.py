import numpy as np
import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from features import UserFeatures, from_dict, DEFAULT_FEATURES, FEATURE_NAMES


def make_features(**overrides) -> UserFeatures:
    defaults = dict(
        tx_count_30d=10, avg_amount_30d=200, stddev_amount_30d=50,
        max_amount_30d=500, unique_devices_30d=2, unique_countries_30d=1,
        intl_tx_count_30d=1, high_amount_count_30d=1,
    )
    defaults.update(overrides)
    return UserFeatures(**defaults)


def test_to_array_shape():
    arr = make_features().to_array()
    assert arr.shape == (1, len(FEATURE_NAMES))


def test_intl_ratio_computed():
    f = make_features(tx_count_30d=10, intl_tx_count_30d=4)
    arr = f.to_array().flatten()
    # intl_tx_ratio_30d is index 6
    assert abs(arr[6] - 0.4) < 1e-9


def test_high_amount_ratio_computed():
    f = make_features(tx_count_30d=10, high_amount_count_30d=3)
    arr = f.to_array().flatten()
    assert abs(arr[7] - 0.3) < 1e-9


def test_zero_tx_count_no_division_error():
    f = make_features(tx_count_30d=0, intl_tx_count_30d=0, high_amount_count_30d=0)
    arr = f.to_array()
    assert not np.isnan(arr).any()


def test_null_stddev_becomes_zero():
    f = make_features(stddev_amount_30d=None)
    arr = f.to_array().flatten()
    assert arr[2] == 0.0


def test_from_dict_roundtrip():
    original = make_features()
    restored = from_dict(original.to_dict())
    assert original == restored


def test_from_dict_handles_none_values():
    d = {"tx_count_30d": None, "avg_amount_30d": None, "stddev_amount_30d": None,
         "max_amount_30d": None, "unique_devices_30d": None, "unique_countries_30d": None,
         "intl_tx_count_30d": None, "high_amount_count_30d": None}
    f = from_dict(d)
    assert f.tx_count_30d == 0.0


def test_default_features_no_error():
    arr = DEFAULT_FEATURES.to_array()
    assert arr.shape == (1, len(FEATURE_NAMES))
    assert not np.isnan(arr).any()
