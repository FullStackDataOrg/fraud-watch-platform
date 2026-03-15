"""
MLflow model loader and scorer.
Loads Production model on startup, exposes score() and log_prediction().
Predictions are logged to Postgres for auditability and retraining.
"""
import os
import logging
import psycopg2
import mlflow.sklearn
from datetime import datetime, timezone

from features import UserFeatures

log = logging.getLogger(__name__)

TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]
MODEL_NAME   = os.environ["MODEL_NAME"]
MODEL_STAGE  = os.environ.get("MODEL_STAGE", "Production")

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASS = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DB   = os.environ["POSTGRES_DB"]

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id  TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    fraud_probability FLOAT NOT NULL,
    model_version   TEXT NOT NULL,
    latency_ms      FLOAT NOT NULL,
    predicted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class Predictor:
    def __init__(self) -> None:
        mlflow.set_tracking_uri(TRACKING_URI)
        model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
        self.model = mlflow.sklearn.load_model(model_uri)
        self.model_version = self._resolve_version(model_uri)
        self._ensure_predictions_table()
        log.info({"event": "model_loaded", "version": self.model_version, "stage": MODEL_STAGE})

    def score(self, features: UserFeatures) -> float:
        return float(self.model.predict_proba(features.to_array())[0, 1])

    def log_prediction(
        self, transaction_id: str, user_id: str, prob: float, latency_ms: float
    ) -> None:
        try:
            with self._db_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO predictions
                        (transaction_id, user_id, fraud_probability, model_version, latency_ms)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    (transaction_id, user_id, prob, self.model_version, latency_ms),
                )
        except Exception as e:
            log.error({"event": "prediction_log_failed", "error": str(e), "tx_id": transaction_id})

    def _db_conn(self):
        return psycopg2.connect(
            host=POSTGRES_HOST, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASS,
        )

    def _ensure_predictions_table(self) -> None:
        try:
            with self._db_conn() as conn, conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
        except Exception as e:
            log.error({"event": "predictions_table_init_failed", "error": str(e)})

    def _resolve_version(self, model_uri: str) -> str:
        try:
            client = mlflow.tracking.MlflowClient()
            versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
            return versions[0].version if versions else "unknown"
        except Exception:
            return "unknown"
