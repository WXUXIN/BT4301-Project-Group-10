from datetime import datetime
import mlflow
import pandas as pd
from sqlalchemy import create_engine, text
from mlflow import MlflowClient

from dataops.airflow.mlops.mlops_config import (
    DATAWAREHOUSE_DB,
    MLFLOW_TRACKING_URI,
    TRAIN_TABLE,
    LINEAGE_TABLE,
    MONITOR_TABLE,
    REGISTERED_MODEL_NAME,
)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


def ensure_monitor_table():
    engine = create_engine(DATAWAREHOUSE_DB, echo=False)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {MONITOR_TABLE} (
        monitor_id INT AUTO_INCREMENT PRIMARY KEY,
        checked_at DATETIME,
        latest_period INT,
        total_rows INT,
        churn_rate FLOAT,
        avg_num_complaints FLOAT,
        avg_customer_satisfaction FLOAT,
        champion_version VARCHAR(50)
    )
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def log_monitor_snapshot():
    """
    It logs a snapshot of your data and current champion model into a database so you can monitor changes over time.
    """
    ensure_monitor_table()
    engine = create_engine(DATAWAREHOUSE_DB, echo=False)
    df = pd.read_sql(text(f"SELECT * FROM {TRAIN_TABLE}"), con=engine)

    latest_period_df = pd.read_sql(
        text(f"""
        SELECT MAX(period) AS latest_period
        FROM {LINEAGE_TABLE}
        WHERE target_table = 'fact_customer_churn'
          AND status = 'success'
    """),
        con=engine,
    )

    latest_period = latest_period_df["latest_period"].iloc[0]
    latest_period = int(latest_period) if pd.notna(latest_period) else 0

    client = MlflowClient()
    try:
        champion = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "champion")
        champion_version = champion.version
    except Exception:
        champion_version = None

    payload = {
        "checked_at": datetime.now(),
        "latest_period": latest_period,
        "total_rows": int(len(df)),
        "churn_rate": float(df["churn"].mean()),
        "avg_num_complaints": float(df["num_complaints"].mean()),
        "avg_customer_satisfaction": float(df["customer_satisfaction"].mean()),
        "champion_version": champion_version,
    }

    insert_sql = text(f"""
    INSERT INTO {MONITOR_TABLE} (
        checked_at, latest_period, total_rows, churn_rate,
        avg_num_complaints, avg_customer_satisfaction, champion_version
    ) VALUES (
        :checked_at, :latest_period, :total_rows, :churn_rate,
        :avg_num_complaints, :avg_customer_satisfaction, :champion_version
    )
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, payload)

    return payload


if __name__ == "__main__":
    print(log_monitor_snapshot())
