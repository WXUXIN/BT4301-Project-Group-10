import sys
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# make project root importable at parse time (lightweight — no heavy libs)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

with DAG(
    dag_id="customer_churn_mlops_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule=None,  # triggered by ETL DAG
    catchup=False,
    max_active_runs=1,
    tags=["bt4301", "mlops", "mlflow", "xgboost"],
) as dag:

    @task
    def retrain_if_needed():
        # imports loaded only at runtime
        from dataops.airflow.mlops.retrain_xgb_mlflow import retrain_and_register

        result = retrain_and_register()
        if result["action"] == "skip" if "action" in result else False:
            raise AirflowSkipException(result["reason"])
        return result

    @task
    def monitor():
        # imports loaded only at runtime
        from dataops.airflow.mlops.monitor_model import log_monitor_snapshot

        return log_monitor_snapshot()

    retrain_result = retrain_if_needed()
    monitor_result = monitor()

    retrain_result >> monitor_result
