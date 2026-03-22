"""FastAPI fraud prediction service."""
import logging
import time

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from predictor import Predictor
from feature_client import get_with_fallback

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Fraud Detection API", version="1.0")

# ── Prometheus metrics ────────────────────────────────────
PREDICTIONS_TOTAL = Counter(
    "fraud_predictions_total",
    "Total predictions served",
    ["risk_tier", "feature_source"],
)
PREDICTION_LATENCY = Histogram(
    "fraud_prediction_latency_seconds",
    "Prediction latency in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
FRAUD_PROBABILITY = Histogram(
    "fraud_probability_distribution",
    "Distribution of fraud probability scores",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
MODEL_VERSION = Gauge("fraud_model_version", "Currently loaded model version")
FEATURE_CACHE_HITS = Counter("fraud_feature_cache_hits_total", "Redis cache hits")
FEATURE_CACHE_MISSES = Counter("fraud_feature_cache_misses_total", "Redis cache misses (fallback)")

predictor: Predictor | None = None


@app.on_event("startup")
def startup() -> None:
    global predictor
    predictor = Predictor()
    try:
        MODEL_VERSION.set(float(predictor.model_version))
    except (ValueError, TypeError):
        MODEL_VERSION.set(0)


@app.get("/health")
def health():
    return {"status": "ok", "model_version": predictor.model_version if predictor else "loading"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


class PredictRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    merchant_id: str
    is_international: bool


class PredictResponse(BaseModel):
    transaction_id: str
    fraud_probability: float
    risk_tier: str
    model_version: str
    latency_ms: float
    feature_source: str


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    start = time.monotonic()

    features, from_cache = get_with_fallback(req.user_id)
    prob      = predictor.score(features)
    risk_tier = "high" if prob > 0.7 else "medium" if prob > 0.4 else "low"
    latency   = time.monotonic() - start

    predictor.log_prediction(req.transaction_id, req.user_id, prob, latency * 1000)

    PREDICTIONS_TOTAL.labels(risk_tier=risk_tier, feature_source="cache" if from_cache else "fallback").inc()
    PREDICTION_LATENCY.observe(latency)
    FRAUD_PROBABILITY.observe(prob)
    if from_cache:
        FEATURE_CACHE_HITS.inc()
    else:
        FEATURE_CACHE_MISSES.inc()

    log.info({
        "event": "prediction",
        "tx_id": req.transaction_id,
        "user_id": req.user_id,
        "fraud_probability": round(prob, 4),
        "risk_tier": risk_tier,
        "feature_source": "cache" if from_cache else "fallback",
        "latency_ms": round(latency * 1000, 2),
    })

    return PredictResponse(
        transaction_id=req.transaction_id,
        fraud_probability=round(prob, 4),
        risk_tier=risk_tier,
        model_version=predictor.model_version,
        latency_ms=round(latency * 1000, 2),
        feature_source="cache" if from_cache else "fallback",
    )
