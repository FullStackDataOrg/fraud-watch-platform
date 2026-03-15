"""
Tests for feature_client.py — focuses on Redis failure handling.
All Redis calls are mocked; no live Redis required.
"""
import json
import pytest
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "serving"))

import redis
from features import UserFeatures, DEFAULT_FEATURES
import feature_client as fc


def _make_features() -> UserFeatures:
    return UserFeatures(
        tx_count_30d=5, avg_amount_30d=150, stddev_amount_30d=30,
        max_amount_30d=300, unique_devices_30d=1, unique_countries_30d=1,
        intl_tx_count_30d=0, high_amount_count_30d=0,
    )


@patch("feature_client._get_client")
def test_get_returns_features_when_cache_hit(mock_client):
    features = _make_features()
    mock_redis = MagicMock()
    mock_redis.get.return_value = json.dumps(features.to_dict())
    mock_client.return_value = mock_redis

    result = fc.get("U1234")
    assert result == features


@patch("feature_client._get_client")
def test_get_returns_none_on_cache_miss(mock_client):
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    mock_client.return_value = mock_redis

    result = fc.get("U9999")
    assert result is None


@patch("feature_client._get_client")
def test_get_returns_none_on_redis_connection_error(mock_client):
    mock_redis = MagicMock()
    mock_redis.get.side_effect = redis.ConnectionError("refused")
    mock_client.return_value = mock_redis

    result = fc.get("U1234")
    assert result is None


@patch("feature_client._get_client")
def test_get_returns_none_on_redis_timeout(mock_client):
    mock_redis = MagicMock()
    mock_redis.get.side_effect = redis.TimeoutError("timeout")
    mock_client.return_value = mock_redis

    result = fc.get("U1234")
    assert result is None


@patch("feature_client._get_client")
def test_get_with_fallback_returns_cache_when_available(mock_client):
    features = _make_features()
    mock_redis = MagicMock()
    mock_redis.get.return_value = json.dumps(features.to_dict())
    mock_client.return_value = mock_redis

    result, from_cache = fc.get_with_fallback("U1234")
    assert result == features
    assert from_cache is True


@patch("feature_client._get_client")
def test_get_with_fallback_uses_defaults_on_redis_failure(mock_client):
    mock_redis = MagicMock()
    mock_redis.get.side_effect = redis.ConnectionError("refused")
    mock_client.return_value = mock_redis

    result, from_cache = fc.get_with_fallback("U1234")
    assert result == DEFAULT_FEATURES
    assert from_cache is False


@patch("feature_client._get_client")
def test_set_features_silently_fails_on_redis_error(mock_client):
    mock_redis = MagicMock()
    mock_redis.setex.side_effect = redis.ConnectionError("refused")
    mock_client.return_value = mock_redis

    # Must not raise
    fc.set_features("U1234", _make_features())


@patch("feature_client._get_client")
def test_set_features_writes_correct_key(mock_client):
    mock_redis = MagicMock()
    mock_client.return_value = mock_redis

    fc.set_features("U1234", _make_features(), ttl=3600)
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == "user_features:U1234"
    assert call_args[0][1] == 3600
