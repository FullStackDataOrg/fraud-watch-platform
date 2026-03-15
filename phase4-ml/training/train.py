"""
Model training pipeline.
Reads from Gold mart_fraud_features via Trino, trains RandomForest,
logs to MLflow. Promotes to Staging if AUC >= threshold.
"""
import os
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score

from features import FEATURE_NAMES
from evaluate import build_label_column

TRACKING_URI = os.environ["MLFLOW_TRACKING_URI"]
EXPERIMENT   = os.environ["MLFLOW_EXPERIMENT_NAME"]
MODEL_NAME   = os.environ["MODEL_NAME"]
MIN_AUC      = float(os.environ.get("MIN_AUC_FOR_PROMOTION", 0.90))
TRINO_HOST   = os.environ.get("TRINO_HOST", "trino")


def load_training_data() -> pd.DataFrame:
    from trino.dbapi import connect
    conn = connect(host=TRINO_HOST, port=8080, user="training", catalog="delta", schema="gold")
    cur = conn.cursor()
    cur.execute("""
        SELECT
            user_id,
            tx_count_30d,
            avg_amount_30d,
            stddev_amount_30d,
            max_amount_30d,
            unique_devices_30d,
            unique_countries_30d,
            intl_tx_count_30d,
            high_amount_count_30d
        FROM mart_fraud_features
    """)
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def train() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT)

    df = load_training_data()
    if df.empty:
        raise RuntimeError("No training data found in mart_fraud_features")

    df = build_label_column(df)

    # Compute ratio features that match FEATURE_NAMES
    df["intl_tx_ratio_30d"] = df["intl_tx_count_30d"] / df["tx_count_30d"].clip(lower=1)
    df["high_amount_ratio_30d"] = df["high_amount_count_30d"] / df["tx_count_30d"].clip(lower=1)

    X = df[FEATURE_NAMES].fillna(0)
    y = df["label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    params = {
        "n_estimators": 200,
        "max_depth": 8,
        "class_weight": "balanced",
        "random_state": 42,
    }

    with mlflow.start_run() as run:
        model = RandomForestClassifier(**params)
        model.fit(X_train, y_train)

        y_prob = model.predict_proba(X_test)[:, 1]
        auc    = roc_auc_score(y_test, y_prob)
        preds  = (y_prob > 0.5).astype(int)

        mlflow.log_params(params)
        mlflow.log_metrics({
            "auc_roc":    auc,
            "precision":  precision_score(y_test, preds, zero_division=0),
            "recall":     recall_score(y_test, preds, zero_division=0),
            "train_size": len(X_train),
            "test_size":  len(X_test),
        })
        mlflow.sklearn.log_model(
            model, "model", registered_model_name=MODEL_NAME
        )

        print(f"Run {run.info.run_id}: AUC={auc:.4f}")

        if auc >= MIN_AUC:
            _promote_to_staging(run.info.run_id)
            print(f"Promoted to Staging (AUC={auc:.4f} >= {MIN_AUC})")
        else:
            print(f"Not promoted (AUC={auc:.4f} < threshold={MIN_AUC})")


def _promote_to_staging(run_id: str) -> None:
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}' and run_id='{run_id}'")
    if versions:
        client.transition_model_version_stage(
            MODEL_NAME, versions[0].version, "Staging"
        )


if __name__ == "__main__":
    train()
