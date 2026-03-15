"""
Redis feature store client.
Redis down → returns None → caller falls back to DEFAULT_FEATURES.
Never raises — Redis failure is non-fatal.
"""
import os
import json
import logging
import redis

from features import UserFeatures, DEFAULT_FEATURES, from_dict

log = logging.getLogger(__name__)

KEY_PREFIX = "user_features:"

_client: redis.Redis | None = None


def _get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_PORT"]),
            decode_responses=True,
            socket_timeout=0.5,
        )
    return _client


def get(user_id: str) -> UserFeatures | None:
    try:
        raw = _get_client().get(f"{KEY_PREFIX}{user_id}")
        if raw:
            return from_dict(json.loads(raw))
    except redis.RedisError as e:
        log.warning({"event": "redis_get_failed", "user_id": user_id, "error": str(e)})
    return None


def set_features(user_id: str, features: UserFeatures, ttl: int = 86400) -> None:
    try:
        _get_client().setex(
            f"{KEY_PREFIX}{user_id}", ttl, json.dumps(features.to_dict())
        )
    except redis.RedisError as e:
        log.warning({"event": "redis_set_failed", "user_id": user_id, "error": str(e)})


def get_with_fallback(user_id: str) -> tuple[UserFeatures, bool]:
    """Returns (features, from_cache). from_cache=False means fallback was used."""
    features = get(user_id)
    if features is not None:
        return features, True
    return DEFAULT_FEATURES, False
