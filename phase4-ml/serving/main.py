"""FastAPI fraud prediction service."""
import logging
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from predictor import Predictor
from feature_client import get_with_fallback

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Fraud Detection API", version="1.0")

predictor: Predictor | None = None


@app.on_event("startup")
def startup() -> None:
    global predictor
    predictor = Predictor()


@app.get("/health")
def health():
    return {"status": "ok", "model_version": predictor.model_version if predictor else "loading"}


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
    latency   = (time.monotonic() - start) * 1000

    predictor.log_prediction(req.transaction_id, req.user_id, prob, latency)

    log.info({
        "event": "prediction",
        "tx_id": req.transaction_id,
        "user_id": req.user_id,
        "fraud_probability": round(prob, 4),
        "risk_tier": risk_tier,
        "feature_source": "cache" if from_cache else "fallback",
        "latency_ms": round(latency, 2),
    })

    return PredictResponse(
        transaction_id=req.transaction_id,
        fraud_probability=round(prob, 4),
        risk_tier=risk_tier,
        model_version=predictor.model_version,
        latency_ms=round(latency, 2),
        feature_source="cache" if from_cache else "fallback",
    )
