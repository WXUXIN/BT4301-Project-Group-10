# MLOps Runbook – Customer Churn Pipeline

Step-by-step guide to set up and run the full DataOps + MLOps pipeline from scratch.

---

## Prerequisites

All commands assume you are working inside the project directory with the virtual environment activated.

```bash
cd /root/bt4301/BT4301-Project-Group-10
source venv/bin/activate
```

---

## Step 1 – Configure Persistent MLflow Paths (One-Time Setup)

Add the following two lines to `~/.bashrc` so that the MLflow server always reads from and writes to the same database and artifact store, regardless of which directory you run it from.

```bash
echo 'export MLFLOW_BACKEND_STORE_URI="sqlite:////root/bt4301/BT4301-Project-Group-10/mlflow.db"' >> ~/.bashrc
echo 'export MLFLOW_DEFAULT_ARTIFACT_ROOT="/root/bt4301/BT4301-Project-Group-10/mlflow-artifacts"' >> ~/.bashrc
source ~/.bashrc
```

Verify the variables are set:

```bash
echo $MLFLOW_BACKEND_STORE_URI
echo $MLFLOW_DEFAULT_ARTIFACT_ROOT
```

> **Note:** `mlflow.db` and `mlflow-artifacts/` are already listed in `.gitignore` and will not be committed to the repository.

---

## Step 2 – Reset the Environment (Fresh Start)

Run the following commands to wipe all existing data and start clean.

### 2a. Drop and recreate the MySQL database

```bash
mysql -u root -p -e "DROP DATABASE customer_churn; CREATE DATABASE customer_churn;"
```

### 2b. Reset the DataOps period counter

```bash
rm -f /tmp/airflow_churn_period_counter.txt
```

### 2c. Delete existing MLflow database and artifacts (if any)

```bash
rm -f /root/bt4301/BT4301-Project-Group-10/mlflow.db
rm -rf /root/bt4301/BT4301-Project-Group-10/mlflow-artifacts
```

---

## Step 3 – Start the MLflow Tracking Server

Open a **dedicated terminal** and keep it running throughout the session. The server must be running before any Airflow DAG run that triggers model training.

```bash
cd /root/bt4301/BT4301-Project-Group-10
source venv/bin/activate
python -m mlflow server --host 127.0.0.1 --port 9080
```

The MLflow UI is accessible at: **http://localhost:9080**

> The server automatically uses the paths set in `~/.bashrc` — no additional flags are needed.

---

## Step 4 – Start Airflow (if not already running)

Open a **second terminal**:

```bash
cd /root/bt4301/BT4301-Project-Group-10
source venv/bin/activate
export AIRFLOW_HOME=/root/bt4301/BT4301-Project-Group-10/dataops/airflow
airflow scheduler &
airflow dag-processor &
airflow api-server
```

The Airflow UI is accessible at: **http://localhost:8089**

---

## Step 5 – Run the Pipeline

The DAG `customer_churn_incremental_etl` runs automatically every minute. Each run processes one monthly period of data. You can also trigger it manually:

```bash
airflow dags trigger customer_churn_incremental_etl
```

### What happens each run

| Period | ETL | MLOps |
|--------|-----|-------|
| 1 | Ingests period 1 data, builds `train_churn_model` | **Trains model** on period 1 data, logs to MLflow, sets as champion |
| 2 | Ingests period 2 data, rebuilds `train_churn_model` | Skipped (next retraining at period 4) |
| 3 | Ingests period 3 data, rebuilds `train_churn_model` | Skipped (next retraining at period 4) |
| 4 | Ingests period 4 data, rebuilds `train_churn_model` | **Retrains model** on all 4 periods, compares against champion |
| 7 | … | **Retrains** on all 7 periods |
| … | … | Every 3 periods thereafter |

### Airflow task flow per run

```
get_current_period  →  etl_process  →  trigger_model_training
```

---

## Step 6 – Verify Results

### MLflow UI
- **Experiments** → `Customer Churn MLOps` — view logged parameters and metrics for each training run
- **Model Registry** → `customer_churn_xgboost` — view all registered model versions; the best-performing version carries the `champion` alias

### MySQL audit log
```bash
mysql -u bt4301 -ppassword customer_churn -e \
  "SELECT period, mlflow_model_version, roc_auc, f1_score, promoted_to_champion, trained_at
   FROM mlops_training_log ORDER BY trained_at;"
```

### Airflow task logs
Navigate in the Airflow UI to the DAG run → click `trigger_model_training` → **Logs** tab.

---

## Summary of Key Paths

| Item | Path |
|------|------|
| Project root | `/root/bt4301/BT4301-Project-Group-10/` |
| Virtual environment | `venv/` |
| Airflow DAGs | `dataops/airflow/dags/` |
| ETL DAG | `dataops/airflow/dags/customer_churn_etl_dag.py` |
| MLOps functions | `dataops/airflow/dags/customer_churn_mlops_functions.py` |
| MLflow database | `mlflow.db` (gitignored) |
| MLflow artifacts | `mlflow-artifacts/` (gitignored) |
| Period counter | `/tmp/airflow_churn_period_counter.txt` |
| MLflow UI | http://localhost:9080 |
| Airflow UI | http://localhost:8089 |
