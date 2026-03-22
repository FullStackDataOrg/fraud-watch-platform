"""
Redis feature writer.
Reads mart_fraud_features from Trino (Gold), pushes each user's features
to Redis. Run by Airflow after dbt Gold models complete.
"""
import os
import json
import logging
import redis
from trino.dbapi import connect

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KEY_PREFIX = "user_features:"

FEATURE_COLUMNS = [
    "tx_count_30d", "avg_amount_30d", "stddev_amount_30d", "max_amount_30d",
    "unique_devices_30d", "unique_countries_30d", "intl_tx_count_30d", "high_amount_count_30d",
]


def fetch_features() -> list[dict]:
    trino_host = os.environ.get("TRINO_HOST", "trino")
    conn = connect(host=trino_host, port=8080, user="feature-writer", catalog="delta", schema="gold")
    cur  = conn.cursor()
    cur.execute(f"SELECT user_id, {', '.join(FEATURE_COLUMNS)} FROM mart_fraud_features")
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def push_to_redis(rows: list[dict]) -> int:
    r = redis.Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        decode_responses=True,
    )
    ttl  = int(os.environ.get("REDIS_FEATURE_TTL", 86400))
    pipe = r.pipeline(transaction=False)

    for row in rows:
        user_id = row.pop("user_id")
        payload = {k: (float(v) if v is not None else 0.0) for k, v in row.items()}
        pipe.setex(f"{KEY_PREFIX}{user_id}", ttl, json.dumps(payload))

    pipe.execute()
    return len(rows)


def main() -> None:
    log.info({"event": "feature_writer_start"})
    rows    = fetch_features()
    written = push_to_redis(rows)
    log.info({"event": "feature_writer_done", "users_written": written})


if __name__ == "__main__":
    main()
