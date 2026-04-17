import time
import json
import pandas as pd
import mlflow
import mlflow.sklearn

from mlflow import MlflowClient
from sqlalchemy import create_engine, text
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    f1_score,
    recall_score,
    precision_score,
)
from xgboost import XGBClassifier

from dataops.airflow.mlops.mlops_config import (
    DATAWAREHOUSE_DB,
    MLFLOW_TRACKING_URI,
    MLFLOW_EXPERIMENT_NAME,
    REGISTERED_MODEL_NAME,
    TRAIN_TABLE,
    LINEAGE_TABLE,
    RETRAIN_EVERY_N_PERIODS,
    MIN_ACCEPTABLE_ROC_AUC,
    MIN_ACCEPTABLE_PR_AUC,
    BEST_PARAMS_PATH,
    FEATURE_TYPE_MAP,
    load_selected_features,
)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


def get_latest_ingested_period():
    """
    Reads the latest completed ETL period from data lineage.
    """
    engine = create_engine(DATAWAREHOUSE_DB, echo=False)
    query = text(f"""
        SELECT MAX(period) AS latest_period
        FROM {LINEAGE_TABLE}
        WHERE target_table = 'fact_customer_churn'
          AND status = 'success'
    """)
    df = pd.read_sql(query, con=engine)
    value = df["latest_period"].iloc[0]
    return int(value) if pd.notna(value) else 0


def get_last_trained_period():
    """
    Reads the highest trained_up_to_period tag from MLflow registered model versions.
    Asks MLFLow, Up to which period have I already trained my model before? and returns the period number
    """
    client = MlflowClient()
    try:
        versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
    except Exception:
        return 0

    periods = []
    for v in versions:
        tag_value = v.tags.get("trained_up_to_period")
        if tag_value is not None and str(tag_value).isdigit():
            periods.append(int(tag_value))

    return max(periods) if periods else 0


def should_retrain():
    latest_ingested = get_latest_ingested_period()
    last_trained = get_last_trained_period()
    return (
        (latest_ingested - last_trained) >= RETRAIN_EVERY_N_PERIODS,
        latest_ingested,
        last_trained,
    )


def load_best_params():
    try:
        with open(BEST_PARAMS_PATH, "r") as f:
            data = json.load(f)

            params = data.get("params", {})
            params["threshold"] = data.get("threshold", 0.57)

            return params

    except Exception:
        return {
            "threshold": 0.57,
            "max_depth": 3,
            "learning_rate": 0.056,
            "n_estimators": 373,
            "min_child_weight": 48,
            "gamma": 3.09,
            "reg_alpha": 1.66,
            "reg_lambda": 3.22,
        }


def load_training_data(latest_period: int):
    engine = create_engine(DATAWAREHOUSE_DB, echo=False)
    query = text(f"""
        SELECT *
        FROM {TRAIN_TABLE}
        WHERE ingestion_period <= :latest_period
        ORDER BY ingestion_period, customer_id
    """)
    return pd.read_sql(query, con=engine, params={"latest_period": latest_period})


def split_by_period(df: pd.DataFrame):
    """
    Time-based split:
    - train: all periods except last 2
    - val:   second last period
    - test:  latest period

    Minimum meaningful run is when at least 3 periods exist.
    """
    periods = sorted(df["ingestion_period"].unique())
    if len(periods) < 3:
        raise ValueError(
            "Need at least 3 ingested periods before the first retraining run."
        )

    test_period = periods[-1]
    val_period = periods[-2]
    train_periods = periods[:-2]

    train_df = df[df["ingestion_period"].isin(train_periods)].copy()
    val_df = df[df["ingestion_period"] == val_period].copy()
    test_df = df[df["ingestion_period"] == test_period].copy()

    return train_df, val_df, test_df


def build_preprocessor(feature_cols):
    """
    Builds the preprocessing logic based on current feature types.
    Take all my features and apply the correct transformation based on their type.
    """
    numeric_cols = [c for c in feature_cols if FEATURE_TYPE_MAP.get(c) == "numeric"]
    binary_cols = [c for c in feature_cols if FEATURE_TYPE_MAP.get(c) == "binary"]
    ordinal_cols = [c for c in feature_cols if FEATURE_TYPE_MAP.get(c) == "ordinal"]
    nominal_cols = [c for c in feature_cols if FEATURE_TYPE_MAP.get(c) == "nominal"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_cols),
            ("bin", "passthrough", binary_cols),
            (
                "ord",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                ordinal_cols,
            ),
            ("nom", OneHotEncoder(drop="first", handle_unknown="ignore"), nominal_cols),
        ]
    )
    return preprocessor


def get_current_champion_metrics():
    """
    Returns current champion version + ROC-AUC if a champion exists in MLflow.
    """
    client = MlflowClient()
    try:
        champion = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "champion")
    except Exception:
        return None

    run = client.get_run(champion.run_id)
    metrics = run.data.metrics
    return {
        "version": champion.version,
        "roc_auc": metrics.get("roc_auc"),
        "pr_auc": metrics.get("pr_auc"),
    }


def wait_for_model_version(model_name, version, timeout=30):
    """
    It waits until your MLflow model version is fully ready before continuing.
    """
    client = MlflowClient()
    start = time.time()
    while time.time() - start < timeout:
        mv = client.get_model_version(model_name, version)
        if mv.status == "READY":
            return
        time.sleep(1)
    raise TimeoutError(
        f"Model version {version} for {model_name} not READY within timeout."
    )


def retrain_and_register():
    """
    Main function:
    - checks retraining rule
    - trains XGBoost candidate on latest accumulated data
    - evaluates candidate
    - compares candidate against current champion
    - if better and above threshold, registers and promotes new champion
    """
    retrain, latest_ingested, last_trained = should_retrain()

    if not retrain:
        return {
            "action": "skip",
            "latest_ingested_period": latest_ingested,
            "last_trained_period": last_trained,
            "reason": f"Need {RETRAIN_EVERY_N_PERIODS} new periods before retraining.",
        }

    df = load_training_data(latest_ingested)
    selected_features = load_selected_features(df.columns.tolist())

    # only keep features that actually exist in train_churn_model
    feature_cols = [
        c
        for c in selected_features
        if c in df.columns
        and c not in {"customer_id", "signup_date", "ingestion_period", "churn"}
    ]

    train_df, val_df, test_df = split_by_period(df)

    X_train = train_df[feature_cols]
    y_train = train_df["churn"]

    X_val = val_df[feature_cols]
    y_val = val_df["churn"]

    X_test = test_df[feature_cols]
    y_test = test_df["churn"]

    params = load_best_params()
    print("Loaded params:", params)

    # class weighting from current train split
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    scale_pos_weight = float(neg / pos) if pos > 0 else 1.0

    preprocessor = build_preprocessor(feature_cols)

    xgb = XGBClassifier(
        random_state=42,
        eval_metric="logloss",
        max_depth=params.get("max_depth", 3),
        learning_rate=params.get("learning_rate", 0.056),
        n_estimators=params.get("n_estimators", 373),
        min_child_weight=params.get("min_child_weight", 48),
        gamma=params.get("gamma", 3.09),
        reg_alpha=params.get("reg_alpha", 1.66),
        reg_lambda=params.get("reg_lambda", 3.22),
        scale_pos_weight=params.get("scale_pos_weight", scale_pos_weight),
    )

    # candidate for evaluation
    candidate_pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("model", xgb),
        ]
    )

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run(
        run_name=f"retrain_xgb_up_to_period_{latest_ingested}"
    ) as run:
        candidate_pipeline.fit(X_train, y_train)

        y_prob = candidate_pipeline.predict_proba(X_test)[:, 1]
        threshold = float(params.get("threshold", 0.57))
        y_pred = (y_prob >= threshold).astype(int)

        metrics = {
            "roc_auc": float(roc_auc_score(y_test, y_prob)),
            "pr_auc": float(average_precision_score(y_test, y_prob)),
            "f1": float(f1_score(y_test, y_pred)),
            "recall": float(recall_score(y_test, y_pred)),
            "precision": float(precision_score(y_test, y_pred)),
        }

        mlflow.log_params(
            {
                "model_family": "XGBoost",
                "trained_up_to_period": latest_ingested,
                "threshold": threshold,
                "feature_count": len(feature_cols),
                "train_rows": len(train_df),
                "val_rows": len(val_df),
                "test_rows": len(test_df),
                "max_depth": params.get("max_depth", 3),
                "learning_rate": params.get("learning_rate", 0.056),
                "n_estimators": params.get("n_estimators", 373),
            }
        )

        mlflow.log_metrics(metrics)
        mlflow.set_tags(
            {
                "trained_up_to_period": str(latest_ingested),
                "candidate_model": "true",
                "model_family": "XGBoost",
            }
        )

        champion = get_current_champion_metrics()

        is_good_enough = (
            metrics["roc_auc"] >= MIN_ACCEPTABLE_ROC_AUC
            and metrics["pr_auc"] >= MIN_ACCEPTABLE_PR_AUC
        )

        beats_champion = champion is None or metrics["roc_auc"] > (
            champion["roc_auc"] or 0.0
        )

        promote = is_good_enough and beats_champion

        mlflow.log_dict({"selected_features": feature_cols}, "selected_features.json")
        mlflow.log_dict(metrics, "evaluation_metrics.json")

        result = {
            "candidate_metrics": metrics,
            "latest_ingested_period": latest_ingested,
            "last_trained_period": last_trained,
            "promote": promote,
            "champion_before": champion,
        }

        if promote:
            # IMPORTANT:
            # retrain final champion on ALL available data up to latest period
            X_all = df[feature_cols]
            y_all = df["churn"]

            final_pipeline = Pipeline(
                [
                    ("preprocessor", build_preprocessor(feature_cols)),
                    (
                        "model",
                        XGBClassifier(
                            random_state=42,
                            eval_metric="logloss",
                            max_depth=params.get("max_depth", 3),
                            learning_rate=params.get("learning_rate", 0.056),
                            n_estimators=params.get("n_estimators", 373),
                            min_child_weight=params.get("min_child_weight", 48),
                            gamma=params.get("gamma", 3.09),
                            reg_alpha=params.get("reg_alpha", 1.66),
                            reg_lambda=params.get("reg_lambda", 3.22),
                            subsample=params.get("subsample", 0.782678789545537),
                            colsample_bytree=params.get(
                                "colsample_bytree", 0.8111691627386142
                            ),
                            scale_pos_weight=params.get(
                                "scale_pos_weight", scale_pos_weight
                            ),
                        ),
                    ),
                ]
            )

            final_pipeline.fit(X_all, y_all)

            mlflow.sklearn.log_model(
                sk_model=final_pipeline,
                artifact_path="model",
                input_example=X_all.head(3),
            )

            run_id = run.info.run_id
            client = MlflowClient()
            registered = mlflow.register_model(
                model_uri=f"runs:/{run_id}/model", name=REGISTERED_MODEL_NAME
            )

            wait_for_model_version(REGISTERED_MODEL_NAME, registered.version)

            client.set_model_version_tag(
                REGISTERED_MODEL_NAME,
                registered.version,
                "trained_up_to_period",
                str(latest_ingested),
            )
            client.set_model_version_tag(
                REGISTERED_MODEL_NAME,
                registered.version,
                "decision_threshold",
                str(threshold),
            )
            client.set_model_version_tag(
                REGISTERED_MODEL_NAME,
                registered.version,
                "roc_auc",
                str(metrics["roc_auc"]),
            )
            client.set_registered_model_alias(
                REGISTERED_MODEL_NAME, "champion", registered.version
            )

            result["new_champion_version"] = registered.version
        else:
            result["new_champion_version"] = None

        return result


if __name__ == "__main__":
    print(retrain_and_register())
