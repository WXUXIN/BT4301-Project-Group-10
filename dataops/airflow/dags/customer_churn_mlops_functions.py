"""
customer_churn_mlops_functions.py
──────────────────────────────────
MLOps helper functions for the customer churn prediction pipeline.

Responsibilities
────────────────
1. Build a reproducible sklearn Pipeline (OrdinalEncoder → XGBClassifier).
2. Evaluate the pipeline on a held-out test set.
3. Log parameters, metrics, and the model artifact to MLflow.
4. Compare the new model against the current champion and promote if better.
5. Persist a training-run audit log to the MySQL data warehouse.

Retraining schedule (controlled by the caller)
───────────────────────────────────────────────
  Period 1  → initial train on first month of ingested data
  Period 4  → retrain on all 4 months ingested so far
  Period 7  → retrain on all 7 months …
  …and so on every 3 periods: should_retrain(period) is True when
  (period - 1) % 3 == 0.

The `train_churn_model` table in MySQL always contains the full
denormalised feature set for ALL periods ingested so far (it is
rebuilt by the ETL DAG after every period load).  Training therefore
always uses the latest available data snapshot.
"""

from __future__ import annotations

from datetime import datetime

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sqlalchemy import text
from xgboost import XGBClassifier


# CONFIGURATION

DATAWAREHOUSE_DB = "mysql://bt4301:password@localhost:3306/customer_churn"

# MLflow tracking server (started separately with:
#   python -m mlflow server --host 127.0.0.1 --port 9080)
MLFLOW_TRACKING_URI = "http://127.0.0.1:9080"

EXPERIMENT_NAME = "Customer Churn MLOps"

# Name used in the MLflow Model Registry
REGISTERED_MODEL_NAME = "customer_churn_xgboost"

# The alias given to the best-performing registered model version.
# Load for inference with:
#   model = mlflow.pyfunc.load_model(f"models:/{REGISTERED_MODEL_NAME}@{CHAMPION_ALIAS}")
CHAMPION_ALIAS = "champion"

# Best hyperparameters discovered during experimentation 
# These are fixed; every retraining run uses the same architecture so that
# performance differences across runs are attributable to new data, not
# different hyperparameters.
BEST_XGB_PARAMS: dict = {
    "n_estimators": 373,
    "max_depth": 3,
    "learning_rate": 0.05623620173745784,
    "subsample": 0.782678789545537,
    "colsample_bytree": 0.8111691627386142,
    "min_child_weight": 48,
    "gamma": 3.0897457799952854,
    "reg_alpha": 1.6575862542719726,
    "reg_lambda": 3.220311781475361,
    # Reproducibility / runtime
    "random_state": 42,
    "eval_metric": "logloss",
    "n_jobs": -1,
}

# Classification threshold tuned to balance precision/recall on validation data.
# Predictions are positive (churn=1) when predicted probability >= THRESHOLD.
THRESHOLD: float = 0.57

# Feature columns (must match train_churn_model schema) 
# Categoricals: need OrdinalEncoding before XGBoost
CATEGORICAL_FEATURES: list[str] = ["contract", "age_group", "tenure_segment"]

# Numericals: passed through as-is
NUMERICAL_FEATURES: list[str] = [
    "annual_income",
    "customer_satisfaction",
    "num_complaints",
    "num_service_calls",
    "late_payments",
]

# Binaries: already 0/1 integers — passed through as-is
BINARY_FEATURES: list[str] = [
    "has_phone_service",
    "has_internet_service",
    "has_tech_support",
    "has_streaming_tv",
    "has_streaming_movies",
    "high_risk_flag",
    "is_auto_pay",
]

ALL_FEATURES: list[str] = CATEGORICAL_FEATURES + NUMERICAL_FEATURES + BINARY_FEATURES
TARGET: str = "churn"

# Ordinal categories.
# The ordering encodes natural risk direction (lower value = lower churn risk).
CAT_CATEGORIES: list[list[str]] = [
    # contract: month-to-month customers churn most; two-year least
    ["month_to_month", "one_year", "two_year"],
    # age_group: youngest to oldest
    ["0-18", "18-29", "30-39", "40-49", "50-59", "60-69", "70+"],
    # tenure_segment: newest to most tenured
    ["Infant", "Stable", "Loyal", "Veteran"],
]

# Name of the MySQL audit table where each training run is recorded
MLOPS_LOG_TABLE = "mlops_training_log"


# SCHEDULE HELPER

def should_retrain(period: int) -> bool:
    """
    Return True if the model should be retrained at this period.

    Retraining schedule: periods 1, 4, 7, 10, …
    That is, the first ingestion triggers training, and then every
    3 additional periods thereafter.

    Examples
    ────────
    should_retrain(1)  → True   (initial training)
    should_retrain(2)  → False
    should_retrain(3)  → False
    should_retrain(4)  → True   (3 new periods since last training)
    should_retrain(5)  → False
    should_retrain(7)  → True
    """
    return (period - 1) % 3 == 0


def next_retrain_period(period: int) -> int:
    """Return the next period at which retraining will occur."""
    remainder = (period - 1) % 3
    return period + (3 - remainder) if remainder != 0 else period + 3


# DATA LOADING

def load_training_data(engine) -> pd.DataFrame:
    """
    Read the denormalised `train_churn_model` table from the data warehouse.

    This table is rebuilt (replace) by the ETL DAG after every period
    ingestion, so it always contains the full history of loaded data.
    """
    df = pd.read_sql("SELECT * FROM train_churn_model", engine)
    print(f"[MLOps] Loaded {len(df):,} rows from train_churn_model.")
    return df


# PIPELINE CONSTRUCTION

def build_pipeline() -> Pipeline:
    """
    Return a fresh, untrained sklearn Pipeline.

    Pipeline structure
    Step 1 - ColumnTransformer:
      • OrdinalEncoder applied to the three categorical columns.
        The category ordering encodes natural churn-risk direction so
        that XGBoost's tree splits have meaningful thresholds.
        unknown_value=-1 handles any unseen category gracefully.
      • remainder='passthrough' leaves numerical/binary columns unchanged.
      • verbose_feature_names_out=False keeps the original column names
        instead of prefixing them with the transformer name.

    Step 2 - XGBClassifier:
      • Uses the pre-tuned hyperparameters from BEST_XGB_PARAMS.
      • random_state=42 ensures reproducibility across identical datasets.

    Storing both steps inside a Pipeline means the complete transform +
    predict logic is serialised together by MLflow — the same object
    can be loaded and called with raw (pre-encoded) feature DataFrames
    at inference time.
    """
    ordinal_enc = OrdinalEncoder(
        categories=CAT_CATEGORIES,
        handle_unknown="use_encoded_value",
        unknown_value=-1,
        dtype=np.float64,
    )

    preprocessor = ColumnTransformer(
        transformers=[("cat", ordinal_enc, CATEGORICAL_FEATURES)],
        remainder="passthrough",
        verbose_feature_names_out=False,
    )

    classifier = XGBClassifier(**BEST_XGB_PARAMS)

    return Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", classifier),
    ])


# EVALUATION

def evaluate(pipeline: Pipeline, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """
    Compute classification metrics using the tuned decision threshold.

    Using predict_proba rather than predict lets us apply our custom
    THRESHOLD, which was tuned on the validation set to balance
    precision and recall.  Default threshold (0.5) is not used.

    Returns a dict with keys: roc_auc, f1_score, precision, recall, accuracy.
    """
    y_prob = pipeline.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= THRESHOLD).astype(int)

    return {
        "roc_auc":   float(roc_auc_score(y_test, y_prob)),
        "f1_score":  float(f1_score(y_test, y_pred, zero_division=0)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall":    float(recall_score(y_test, y_pred, zero_division=0)),
        "accuracy":  float(accuracy_score(y_test, y_pred)),
    }


# MODEL REGISTRY HELPERS

def _get_champion_roc_auc(client: MlflowClient) -> float | None:
    """
    Retrieve the ROC AUC of the current champion model from the registry.

    Returns None if no champion alias has been set yet (first run).
    The champion alias is set on the model version that achieved the
    highest ROC AUC on the held-out test set.
    """
    try:
        version_info = client.get_model_version_by_alias(
            REGISTERED_MODEL_NAME, CHAMPION_ALIAS
        )
        run = client.get_run(version_info.run_id)
        return run.data.metrics.get("roc_auc")
    except Exception:
        # Covers: model not yet registered, alias not yet set, network issues
        return None


def _get_latest_version(client: MlflowClient) -> str:
    """Return the version number (as str) of the most recently registered model."""
    versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
    return str(max(int(v.version) for v in versions))


# DATABASE AUDIT LOG

def _ensure_mlops_log_table(engine) -> None:
    """
    Create mlops_training_log if it does not already exist.

    This table forms part of the MLOps lineage — it links each training
    run back to the DataOps period that triggered it, and records the
    model's performance metrics so they can be queried directly from MySQL
    without going through the MLflow UI.
    """
    create_sql = text(f"""
    CREATE TABLE IF NOT EXISTS {MLOPS_LOG_TABLE} (
        log_id               INT AUTO_INCREMENT PRIMARY KEY,
        mlflow_run_id        VARCHAR(100)   NOT NULL,
        mlflow_model_version VARCHAR(20),
        period               INT            NOT NULL,
        training_samples     INT,
        test_samples         INT,
        roc_auc              FLOAT,
        f1_score             FLOAT,
        precision_score      FLOAT,
        recall_score         FLOAT,
        accuracy             FLOAT,
        promoted_to_champion TINYINT(1)     DEFAULT 0,
        trained_at           DATETIME       NOT NULL
    )
    """)
    with engine.begin() as conn:
        conn.execute(create_sql)


def log_training_to_db(
    engine,
    period: int,
    run_id: str,
    model_version: str,
    metrics: dict,
    n_train: int,
    n_test: int,
    promoted: bool,
) -> None:
    """
    Insert one audit record into mlops_training_log.

    This provides an SQL-queryable audit trail of every training run,
    making it easy to answer questions like:
    "Which model version was champion after period 7?" or
    "How did ROC AUC evolve as more data was ingested?"
    """
    _ensure_mlops_log_table(engine)

    insert_sql = text(f"""
    INSERT INTO {MLOPS_LOG_TABLE}
        (mlflow_run_id, mlflow_model_version, period,
         training_samples, test_samples,
         roc_auc, f1_score, precision_score, recall_score, accuracy,
         promoted_to_champion, trained_at)
    VALUES
        (:run_id, :version, :period,
         :n_train, :n_test,
         :roc_auc, :f1_score, :precision, :recall, :accuracy,
         :promoted, :trained_at)
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "run_id":    run_id,
            "version":   model_version,
            "period":    period,
            "n_train":   n_train,
            "n_test":    n_test,
            "roc_auc":   metrics["roc_auc"],
            "f1_score":  metrics["f1_score"],
            "precision": metrics["precision"],
            "recall":    metrics["recall"],
            "accuracy":  metrics["accuracy"],
            "promoted":  int(promoted),
            "trained_at": datetime.now(),
        })


# MAIN PIPELINE ENTRY POINT

def run_mlops_pipeline(period: int, engine) -> tuple[str, dict]:
    """
    Execute the full MLOps pipeline for the given DataOps period.

    Steps
    1. Set MLflow tracking URI and create/select the experiment.
    2. Load all ingested data from train_churn_model.
    3. Stratified 80/20 train-test split (fixed seed → comparable across runs).
    4. Build and fit the sklearn Pipeline.
    5. Evaluate on the held-out test set.
    6. Log parameters, metrics, and the model artifact to MLflow.
       The model is simultaneously registered in the Model Registry under
       REGISTERED_MODEL_NAME.
    7. Compare the new model's ROC AUC against the current champion:
       - If better (or no champion yet) → set champion alias.
       - Otherwise → log as a challenger; existing champion is retained.
    8. Write an audit record to mlops_training_log in MySQL.

    Returns
    (mlflow_run_id, metrics_dict)
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    # 1. Ensure the MLflow experiment exists 
    if mlflow.get_experiment_by_name(EXPERIMENT_NAME) is None:
        mlflow.create_experiment(EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # 2. Load data 
    df = load_training_data(engine)
    df[TARGET] = df[TARGET].astype(int)

    X = df[ALL_FEATURES].copy()
    y = df[TARGET].copy()

    # 3. Train / test split 
    # Using a fixed random_state=42 ensures the same rows are always in the
    # test set regardless of which period triggered the run, making ROC AUC
    # comparisons across runs fair (same test distribution).
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # 4. Train 
    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    # 5. Evaluate 
    metrics = evaluate(pipeline, X_test, y_test)

    # 6. Log to MLflow 
    run_name = f"xgboost_period_{period}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        # --- Tags: high-level metadata visible in the MLflow Experiments UI
        mlflow.set_tag("model_type", "XGBoost")
        mlflow.set_tag("pipeline_version", "sklearn_pipeline_v1")

        # --- Parameters: everything needed to reproduce the run
        mlflow.log_param("period_trained_up_to", period)
        mlflow.log_param("decision_threshold", THRESHOLD)
        mlflow.log_param("training_samples", len(X_train))
        mlflow.log_param("test_samples", len(X_test))
        mlflow.log_param("train_churn_rate", round(float(y_train.mean()), 4))
        mlflow.log_param("test_churn_rate", round(float(y_test.mean()), 4))
        mlflow.log_params(BEST_XGB_PARAMS)

        # --- Metrics: model performance on the held-out test set
        mlflow.log_metrics(metrics)

        # --- Model artifact + registration in the Model Registry
        # Passing registered_model_name causes MLflow to:
        #   (a) save the model artifact under this run, and
        #   (b) create a new version in the Model Registry automatically.
        # input_example infers the model signature (input schema) so that
        # the REST serving endpoint knows what JSON fields to expect.
        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
            input_example=X_train.iloc[:5].reset_index(drop=True),
            registered_model_name=REGISTERED_MODEL_NAME,
        )

    print(f"[MLOps] Run '{run_name}' logged → run_id={run_id}")
    print(f"[MLOps] Metrics: {metrics}")

    # 7. Model governance: compare with champion
    new_version = _get_latest_version(client)
    champion_roc_auc = _get_champion_roc_auc(client)
    promoted = False

    if champion_roc_auc is None:
        # No champion exists yet — this is the very first model
        client.set_registered_model_alias(
            REGISTERED_MODEL_NAME, CHAMPION_ALIAS, new_version
        )
        promoted = True
        print(
            f"[MLOps] Version {new_version} set as champion "
            f"(first model — ROC AUC={metrics['roc_auc']:.4f})"
        )
    elif metrics["roc_auc"] > champion_roc_auc:
        # New model outperforms the existing champion — promote it
        client.set_registered_model_alias(
            REGISTERED_MODEL_NAME, CHAMPION_ALIAS, new_version
        )
        promoted = True
        print(
            f"[MLOps] Version {new_version} promoted to champion "
            f"(new ROC AUC={metrics['roc_auc']:.4f} > "
            f"champion ROC AUC={champion_roc_auc:.4f})"
        )
    else:
        # New model does not beat the champion — retain existing champion
        print(
            f"[MLOps] Version {new_version} registered as challenger. "
            f"Champion ROC AUC={champion_roc_auc:.4f}, "
            f"new ROC AUC={metrics['roc_auc']:.4f} — champion unchanged."
        )

    # 8. Write audit log to MySQL
    log_training_to_db(
        engine=engine,
        period=period,
        run_id=run_id,
        model_version=new_version,
        metrics=metrics,
        n_train=len(X_train),
        n_test=len(X_test),
        promoted=promoted,
    )

    return run_id, metrics
