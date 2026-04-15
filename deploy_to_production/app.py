"""
To run locally (outside Docker):
    ARTIFACTS_DIR=./artifacts/current uvicorn app:app --reload --port 8000

Run via Docker:
    docker compose -f deploy_to_production/compose.yaml up -d
"""
from __future__ import annotations
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from sqlalchemy import create_engine
import json


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "/app/artifacts/current"))

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Customer Churn Prediction API",
    version="2.0.0",
    description=(
        "Serves churn predictions using the current MLflow champion model. "
        "The champion is a full sklearn Pipeline (preprocessor + XGBClassifier) "
        "loaded from a locally synced artifact directory. "
        "Run `sync_champion_from_mlflow.py` to update the champion, "
        "then restart this container."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------------------------------------------------------------
# Global state — loaded once at startup
# ---------------------------------------------------------------------------

_pipeline = None          # full sklearn Pipeline: predict_proba() ready
_feature_cols: list = []  # ordered list of required input features
_metadata: dict = {}      # champion provenance (version, period, threshold …)
_threshold: float = 0.57  # decision threshold — overridden from metadata.json


@app.on_event("startup")
def _startup() -> None:
    """
    Load champion artifacts if they exist.  If any required file is missing
    (e.g. after an MLflow reset before the first champion is promoted), the app
    starts anyway and returns 503 on all prediction endpoints until a champion
    is synced and the container is restarted.
    """
    global _pipeline, _feature_cols, _metadata, _threshold

    model_pkl = ARTIFACTS_DIR / "model" / "model.pkl"
    feature_list_path = ARTIFACTS_DIR / "feature_lists.json"
    metadata_path = ARTIFACTS_DIR / "metadata.json"

    # --- model -----------------------------------------------------------
    if not model_pkl.exists():
        print(
            f"[startup] WARNING: No champion model found at {model_pkl}. "
            "Serving will return 503 until a champion is synced. "
            "Run: python deploy_to_production/sync_champion_from_mlflow.py"
        )
        return  # leave _pipeline = None; routes handle this gracefully

    try:
        _pipeline = joblib.load(model_pkl)
    except Exception as exc:
        print(f"[startup] ERROR: Failed to load model: {exc}. Serving will return 503.")
        return

    # --- feature list ----------------------------------------------------
    if not feature_list_path.exists():
        print(f"[startup] ERROR: feature_lists.json not found at {feature_list_path}. Serving will return 503.")
        _pipeline = None
        return

    with open(feature_list_path) as f:
        fl = json.load(f)
    _feature_cols = fl["all"] if isinstance(fl, dict) and "all" in fl else (
        fl if isinstance(fl, list) else []
    )
    if not _feature_cols:
        print("[startup] ERROR: feature_lists.json has an empty feature list. Serving will return 503.")
        _pipeline = None
        return

    # --- metadata --------------------------------------------------------
    if not metadata_path.exists():
        print(f"[startup] WARNING: metadata.json not found at {metadata_path}. Using defaults.")
    else:
        with open(metadata_path) as f:
            _metadata = json.load(f)

    raw_threshold = _metadata.get("decision_threshold")
    _threshold = float(raw_threshold) if raw_threshold is not None else 0.57

    print(
        f"[startup] Champion loaded — "
        f"version={_metadata.get('champion_version')} | "
        f"period={_metadata.get('trained_up_to_period')} | "
        f"threshold={_threshold} | "
        f"features={len(_feature_cols)}"
    )


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class CustomerFeatures(BaseModel):
    """
    Feature values for a single customer.

    Fields reflect the champion model's selected feature set as defined in
    feature_lists.json.  All fields are required; extra fields are silently
    ignored during scoring.
    """

    # numeric
    customer_satisfaction: float = Field(..., ge=0, example=7.0,
        description="Customer satisfaction score")
    num_complaints: int = Field(..., ge=0, example=1,
        description="Number of complaints raised")
    num_service_calls: int = Field(..., ge=0, example=2,
        description="Number of inbound service calls")
    late_payments: int = Field(..., ge=0, example=0,
        description="Number of late payment occurrences")

    # binary flags (0 / 1)
    has_phone_service: int = Field(..., ge=0, le=1, example=1)
    has_internet_service: int = Field(..., ge=0, le=1, example=1)
    has_tech_support: int = Field(..., ge=0, le=1, example=0)
    has_streaming_tv: int = Field(..., ge=0, le=1, example=1)
    has_streaming_movies: int = Field(..., ge=0, le=1, example=1)
    high_risk_flag: int = Field(..., ge=0, le=1, example=0)
    is_auto_pay: int = Field(..., ge=0, le=1, example=1)

    # ordinal / categorical
    tenure_segment: Literal['Infant', 'Stable', 'Loyal', 'Veteran'] = Field(
        ..., 
        description="Tenure category from dim_customer"
    )
    contract: Literal['month_to_month', 'one_year', 'two_year'] = Field(...)

    class Config:
        extra = "allow"  # extra keys are accepted but ignored


class PredictionResponse(BaseModel):
    registered_model_name: str
    champion_version: str | None
    decision_threshold: float
    churn_probability: float
    churn_prediction: int
    risk_tier: str


class BatchPredictionResponse(BaseModel):
    registered_model_name: str
    champion_version: str | None
    decision_threshold: float
    count: int
    predictions: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Scoring helper
# ---------------------------------------------------------------------------

def _score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run df through the champion Pipeline and return a results DataFrame.
    Raises ValueError if any required feature column is missing.
    """
    missing = [c for c in _feature_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Input is missing required feature columns: {missing}. "
            f"Expected columns: {_feature_cols}"
        )

    X = df[_feature_cols]
    probs = _pipeline.predict_proba(X)[:, 1]
    preds = (probs >= _threshold).astype(int)

    risk_tier = pd.cut(
        probs,
        bins=[-0.001, 0.3, 0.6, 1.001],
        labels=["low", "medium", "high"],
    )

    return pd.DataFrame(
        {
            "churn_probability": probs.round(4),
            "churn_prediction": preds,
            "risk_tier": risk_tier.astype(str),
        },
        index=df.index,
    )


def _champion_meta() -> dict:
    return {
        "registered_model_name": _metadata.get("registered_model_name", "unknown"),
        "champion_version": _metadata.get("champion_version"),
        "decision_threshold": _threshold,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", summary="Service info")
def root():
    """Returns basic service information and champion provenance."""
    return {
        "service": "Customer Churn Prediction API",
        "docs": "/docs",
        "health": "/health",
        "registered_model_name": _metadata.get("registered_model_name"),
        "champion_version": _metadata.get("champion_version"),
        "trained_up_to_period": _metadata.get("trained_up_to_period"),
        "synced_at": _metadata.get("synced_at"),
    }


@app.get("/health", summary="Health check")
def health():
    """
    Returns 200 if the champion model is loaded and ready to serve.
    Returns 503 with a clear message if no champion has been synced yet
    (e.g. after an MLflow reset, before the first champion is promoted).
    """
    if _pipeline is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "No champion model loaded. "
                "Wait for the retraining pipeline to promote a champion, "
                "then run: python deploy_to_production/sync_champion_from_mlflow.py"
            ),
        )
    return {
        "status": "ok",
        "registered_model_name": _metadata.get("registered_model_name"),
        "champion_version": _metadata.get("champion_version"),
        "trained_up_to_period": _metadata.get("trained_up_to_period"),
        "decision_threshold": _threshold,
        "feature_count": len(_feature_cols),
        "features": _feature_cols,
        "synced_at": _metadata.get("synced_at"),
    }


@app.post(
    "/predict",
    response_model=PredictionResponse,
    summary="Score a single customer",
)
def predict_one(payload: CustomerFeatures):
    """
    Score a single customer record.

    Returns:
    - **churn_probability**: raw model probability (0–1)
    - **churn_prediction**: 0 or 1 based on the champion's decision threshold
    - **risk_tier**: `low` (< 0.3), `medium` (0.3–0.6), `high` (≥ 0.6)
    """
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    try:
        df = pd.DataFrame([payload.model_dump()])
        row = _score(df).iloc[0]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Scoring error: {exc}")

    return {
        **_champion_meta(),
        "churn_probability": float(row["churn_probability"]),
        "churn_prediction": int(row["churn_prediction"]),
        "risk_tier": str(row["risk_tier"]),
    }


@app.post(
    "/predict-batch",
    response_model=BatchPredictionResponse,
    summary="Score a batch of customers",
)
def predict_batch(payloads: List[CustomerFeatures]):
    """
    Score a list of customer records in one call.

    Each element in `predictions` contains:
    - **churn_probability**, **churn_prediction**, **risk_tier**

    Returns 400 on empty list, 422 on missing columns, 500 on unexpected errors.
    """
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")
    if not payloads:
        raise HTTPException(status_code=400, detail="Payload list must not be empty.")

    try:
        df = pd.DataFrame([p.model_dump() for p in payloads])
        results = _score(df)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Scoring error: {exc}")

    return {
        **_champion_meta(),
        "count": len(results),
        "predictions": results.to_dict(orient="records"),
    }

@app.get("/fetch-latest-data")
def fetch_latest_from_db():
    USER, PASSWORD, HOST, DB = 'bt4301', 'password', 'localhost', 'customer_churn'
    engine = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}')
    query = """
    SELECT 
        t.customer_id,
        t.customer_satisfaction, t.num_complaints, t.num_service_calls, 
        t.late_payments, t.has_phone_service, t.has_internet_service, 
        t.has_tech_support, t.has_streaming_tv, t.has_streaming_movies, 
        t.high_risk_flag, t.is_auto_pay, t.tenure_segment, t.contract
    FROM train_churn_model t
    JOIN dim_customer c ON t.customer_id = c.customer_id
    WHERE c.signup_date >= (
        SELECT DATE_SUB(MAX(signup_date), INTERVAL 2 DAY) 
        FROM dim_customer
    )
    """
    df = pd.read_sql(query, engine)
    df = df.fillna({
        'contract': 'month_to_month',
        'customer_satisfaction': 0.0
    }).fillna(0)
    
    return df.to_dict(orient="records")
