import os
import json

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT", "/root/bt4301_group_project/BT4301-Project-Group-10"
)

DATAWAREHOUSE_DB = os.getenv(
    "DATAWAREHOUSE_DB", "mysql+pymysql://bt4301:password@localhost:3306/customer_churn"
)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:9080")

MLFLOW_EXPERIMENT_NAME = "customer_churn_retraining"
REGISTERED_MODEL_NAME = "customer_churn_xgb"

TRAIN_TABLE = "train_churn_model"
LINEAGE_TABLE = "data_lineage_log"
MONITOR_TABLE = "mlops_monitor_log"

RETRAIN_EVERY_N_PERIODS = 3

# promotion thresholds
MIN_ACCEPTABLE_ROC_AUC = 0.68
MIN_ACCEPTABLE_PR_AUC = 0.20

ARTIFACT_DIR = os.path.join(PROJECT_ROOT, "dataops", "airflow", "mlops", "artifacts")
BEST_PARAMS_PATH = os.path.join(ARTIFACT_DIR, "best_params.json")
FEATURE_LIST_PATH = os.path.join(ARTIFACT_DIR, "feature_lists.json")

# default feature typing for current train_churn_model design
FEATURE_TYPE_MAP = {
    "annual_income": "numeric",
    "contract": "nominal",
    "has_phone_service": "binary",
    "has_internet_service": "binary",
    "has_tech_support": "binary",
    "has_streaming_tv": "binary",
    "has_streaming_movies": "binary",
    "customer_satisfaction": "numeric",
    "num_complaints": "numeric",
    "num_service_calls": "numeric",
    "late_payments": "numeric",
    "high_risk_flag": "binary",
    "age_group": "ordinal",
    "tenure_segment": "ordinal",
    "is_auto_pay": "binary",
}

EXCLUDED_COLS = {"customer_id", "signup_date", "ingestion_period", "churn"}


def load_selected_features(fallback_cols=None):
    """
    Prefer feature_lists.json if it exists.
    If not, fall back to all current train_churn_model feature columns.
    """
    if os.path.exists(FEATURE_LIST_PATH):
        with open(FEATURE_LIST_PATH, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            if "all" in data:
                return data["all"]
            # if teammate stored grouped lists, flatten them
            out = []
            for _, vals in data.items():
                if isinstance(vals, list):
                    out.extend(vals)
            return list(dict.fromkeys(out))

        if isinstance(data, list):
            return data

    return [c for c in (fallback_cols or []) if c not in EXCLUDED_COLS]
