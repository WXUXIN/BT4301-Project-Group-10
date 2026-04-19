# BT4301 Project – Group 10 (Customer Churn Analytics)

End-to-end **Customer Churn** analytics project covering DataOps (Airflow-orchestrated ETL into a MySQL star schema with row-level fingerprint watermarking and lineage), MLOps (MLflow tracking + XGBoost retraining with champion promotion), and DevOps (FastAPI + dashboard deployment via Docker).

### 1. Prerequisites

- **Operating system (recommended)**:
  - Ubuntu running **inside Docker** (e.g. dev container), or
  - Ubuntu on Windows via **WSL2 (Windows Subsystem for Linux)**.
- **Python**: 3.10 (or compatible 3.10.x)
- **MySQL Server**: 8.x (or compatible)
- **pip**: bundled with Python

You should have MySQL credentials (host, port, user, password, database) that you can use for this project.

Follow the **BT4301 SLAs Week 6** document for:

- Installing **MySQL** on Ubuntu.
- Ensuring the MySQL service is running and accessible from your Ubuntu environment.

### 1.1. Clone this repository in Ubuntu

Inside your Ubuntu environment (Docker container or WSL2):

```bash
# Create a workspace folder (only once)
mkdir -p ~/bt4301
cd ~/bt4301

# Clone this repository
git clone <REPOSITORY_URL> BT4301-Project-Group-10
cd BT4301-Project-Group-10
```

Replace `<REPOSITORY_URL>` with the Git URL provided for your group (e.g. from GitHub or your course Git server).

### 2. Create a Python venv and install dependencies

In your **remote Ubuntu environment** (e.g. Docker or WSL2), from the project root:

```bash
cd BT4301-Project-Group-10   # or your clone path

# Create a Python virtual environment (only once)
python3 -m venv venv

# Activate the venv (required every time you open a new terminal)
source venv/bin/activate

# Install dependencies from dataops
pip install -r dataops/requirements.txt
```

To deactivate the venv later: `deactivate`.

For Jupyter notebook usage from this venv, install `ipykernel` (already in `requirements.txt`) and register the kernel:

```bash
python -m ipykernel install --user --name bt4301-venv --display-name "Python (bt4301-venv)"
```

Then, in your notebook UI, select the kernel **"Python (bt4301-venv)"** so the notebook uses this virtual environment.

### 3. Start MySQL (Ubuntu / Docker)

In the **same Ubuntu terminal** (or before running the pipeline), start the MySQL service:

```bash
sudo service mysql start
```

If you use **Ubuntu in Docker**, run this inside the container so the notebook can connect to `localhost` MySQL. Check status with: `sudo service mysql status`.

### 4. MySQL setup

1. **Create the data warehouse database**

   In MySQL:

   ```sql
   CREATE DATABASE customer_churn;
   USE customer_churn;
   ```

   Tables (`dim_customer`, `dim_account`, `dim_service`, `dim_behavior`, `fact_customer_churn`, `train_churn_model`, `data_lineage_log`, `mlops_monitor_log`) are created by the ETL/MLOps DAGs on first run — no manual DDL is required.

2. **Source data**

   Place the source file `customer_churn_1M.csv` under `data/` (i.e. `data/customer_churn_1M.csv`). The ETL slices it into monthly periods keyed by `signup_date`.

### 5. SQLTools connection (view ingested tables)

Use **SQLTools** in Cursor/VS Code to connect to MySQL and view the tables after the pipeline has ingested data.

1. **Install SQLTools and the MySQL driver**
   - Install the **SQLTools** extension (and **SQLTools MySQL/MariaDB** or **SQLTools MySQL** driver) from the Extensions view in Cursor/VS Code.

2. **Create a new connection**
   - Open the **SQLTools** sidebar (database icon or "SQLTools" in the activity bar).
   - Click **"Add new connection"** (or the + icon).
   - Choose **MySQL** (or MySQL/MariaDB) as the driver.
   - Fill in the connection details to match your MySQL credentials:
     - **Host:** `localhost` (or your MySQL host; if MySQL runs inside Docker, use the host that Cursor can reach, e.g. `localhost` if port 3306 is forwarded).
     - **Port:** `3306`
     - **User:** `bt4301` (or whatever you configured).
     - **Password:** as configured.
     - **Database:** `customer_churn`
   - Give the connection a name (e.g. "Customer Churn DWH") and save.

3. **Connect and view tables**
   - Select the new connection and click **Connect** (or right‑click → Connect).
   - Once connected, expand the connection → **customer_churn** → **Tables** to see the ingested star-schema tables: **dim_customer**, **dim_account**, **dim_service**, **dim_behavior**, **fact_customer_churn**, the denormalized **train_churn_model** view used for ML training, the **data_lineage_log** audit table, and the **mlops_monitor_log** monitoring snapshots.
   - You can run `SELECT` queries or open table data from the explorer.

Ensure MySQL is running (`sudo service mysql start` in Ubuntu/Docker) and that the host/port are reachable from where Cursor runs.

### 6. MLflow Tracking Server

The MLOps DAG logs training runs and registers model versions to a local **MLflow** server backed by SQLite.

**Start MLflow** from the project root (in its own terminal):

```bash
cd BT4301-Project-Group-10
source venv/bin/activate

python -m mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root file:./mlruns \
  --host 127.0.0.1 --port 9080
```

- **UI:** http://127.0.0.1:9080
- **Backend store:** `mlflow.db` (SQLite, project root) — holds experiments, runs, registered models, aliases.
- **Artifact root:** `./mlruns/` — model binaries, params, metrics, tags.
- **Registered model:** `customer_churn_xgb` (with `champion` alias on the promoted version).

The retraining DAG queries the **champion** alias from this server. The deployment sync script (Section 8) reads the champion's storage location and copies the model into `deploy_to_production/artifacts/current/`.

To reset MLflow:

```bash
rm -f mlflow.db
rm -rf mlruns
rm -rf deploy_to_production/artifacts/current/*
```

### 7. Apache Airflow (orchestration)

The project uses **Apache Airflow** for workflow orchestration. Airflow configuration and DAGs live under `dataops/airflow/`.

**What's included:**

- **`dataops/airflow/airflow.cfg`** — Airflow configuration (paths, executor, database, logging).
- **`dataops/airflow/dags/`** — DAG Python files:
  - `customer_churn_etl_dag.py` — incremental monthly ETL (`customer_churn_incremental_etl`); ingests one period per run, fingerprints rows, logs lineage, then chains into the MLOps DAG and finally syncs the champion model to the production artifacts directory.
  - `customer_churn_mlops_dag.py` — XGBoost retraining and monitoring (`customer_churn_mlops_pipeline`), triggered by the ETL DAG.
  - `customer_churn_fingerprint_verification_dag.py` — re-hashes business columns and compares against stored `row_fp` watermarks (manual trigger, audit).
  - `customer_churn_etl_functions.py` / `customer_churn_fingerprint_verification_functions.py` — shared helper modules imported by the DAGs.

**Airflow metadata database (MySQL):**

Airflow uses a **separate** MySQL database from the customer churn data warehouse:

- **Database name:** `airflow_group10`
- **Connection** (in `airflow.cfg`): `mysql+mysqldb://bt4301:password@localhost:3306/airflow_group10`

Create the database in MySQL:

```sql
CREATE DATABASE airflow_group10;
```

**Setup steps:**

1. Re-install requirements if needed:

   ```bash
   pip install -r dataops/requirements.txt
   ```

2. **Set `AIRFLOW_HOME`** to the project's Airflow directory. **You must do this in the same shell before every `airflow` command**, otherwise Airflow uses `~/airflow` and will not see the project's DAGs.

   From the project root `BT4301-Project-Group-10`:

   ```bash
   export AIRFLOW_HOME="$(pwd)/dataops/airflow"
   ```

   Or with an absolute path:

   ```bash
   export AIRFLOW_HOME=/root/bt4301-group10/BT4301-Project-Group-10/dataops/airflow
   ```

   To avoid setting it every time, add the `export` line to your `~/.bashrc` and run `source ~/.bashrc` (or open a new terminal) before starting Airflow.

3. Initialize the Airflow metadata database:

   ```bash
   airflow db migrate
   ```

4. Create an admin user (for the web UI):

   ```bash
   airflow users create --role Admin --username admin --email admin@example.com --firstname Admin --lastname User --password password
   ```

5. **Start Airflow** (one terminal). **Ensure `AIRFLOW_HOME` is set in this shell**:

   ```bash
   cd BT4301-Project-Group-10
   source venv/bin/activate
   export AIRFLOW_HOME="$(pwd)/dataops/airflow"

   airflow scheduler & airflow dag-processor & airflow triggerer & airflow api-server
   ```

   Open the UI at http://localhost:8088 (api-server). The three DAGs (`customer_churn_incremental_etl`, `customer_churn_mlops_pipeline`, `customer_churn_fingerprint_verification`) will appear once the scheduler reads from the project's `dags` folder.

**Config highlights (in `airflow.cfg`):**

- **Executor:** `LocalExecutor`
- **DAGs folder:** `dataops/airflow/dags`
- **Logs:** `dataops/airflow/logs`
- **Metadata DB:** MySQL `airflow_group10`

**ETL DAG task chain:**

```
get_current_period
   → etl_process
   → wait_before_mlops          (10s buffer to ensure lineage table is committed)
   → trigger_mlops_pipeline     (TriggerDagRunOperator with wait_for_completion=True, reset_dag_run=True)
   → sync_champion_if_promoted  (pulls new champion from MLflow into deploy_to_production/artifacts/current/)
```

The MLOps DAG (`customer_churn_mlops_pipeline`) runs `retrain_if_needed` (skips if fewer than `RETRAIN_EVERY_N_PERIODS` new periods, otherwise trains a new XGBoost model and promotes if it beats the champion on ROC-AUC / PR-AUC) followed by `monitor` (logs a snapshot to `mlops_monitor_log`).

**Reset the period counter**

The `customer_churn_incremental_etl` DAG tracks which monthly `signup_date` slice to load next using a local file: `/tmp/airflow_churn_period_counter.txt`. To restart ingestion from **period 1**:

```bash
rm -f /tmp/airflow_churn_period_counter.txt
```

**Clear DAG run history (fresh start)**

```bash
pkill -9 -f "airflow"; sleep 3

mysql -u bt4301 -ppassword airflow_group10 <<'EOF'
DELETE FROM task_instance WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
DELETE FROM dag_run WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
DELETE FROM log WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
DELETE FROM xcom WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
DELETE FROM rendered_task_instance_fields WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
DELETE FROM job WHERE dag_id IN ('customer_churn_incremental_etl', 'customer_churn_mlops_pipeline');
EOF

rm -rf dataops/airflow/logs/dag_id=customer_churn_incremental_etl
rm -rf dataops/airflow/logs/dag_id=customer_churn_mlops_pipeline
rm -f /tmp/airflow_churn_period_counter.txt
```

### 8. Production serving (FastAPI + Dashboard)

The `deploy_to_production/` directory contains a FastAPI inference backend and the **ChurnGuard** dashboard for serving the MLflow champion model.

**Auto-sync from MLflow:** The ETL DAG's final task (`sync_champion_if_promoted`) automatically pulls the current champion model from MLflow into `deploy_to_production/artifacts/current/` after every successful pipeline run. No manual sync is required.

**Serve the model locally:**

```bash
cd deploy_to_production
pip install -r requirements.txt

# Terminal 1 — FastAPI backend (port 8000)
ARTIFACTS_DIR=./artifacts/current uvicorn app:app --reload --port 8000

# Terminal 2 — Static dashboard (port 3000)
python3 -m http.server 3000
```

Open **http://localhost:3000** and click **Run Predictions**. If the API is offline, the dashboard auto-falls-back to client-side demo mode.

**Serve via Docker:**

```bash
cd deploy_to_production
docker compose up --build
```

The compose file mounts `./artifacts/current/` into the container, so any new champion synced by the ETL DAG is picked up on container restart.

See `deploy_to_production/README.md` for full serving-layer details, payload schema, and frontend features.

### 9. Running the customer churn pipeline

**a) Production-style (Airflow, recommended).** Once Airflow + MLflow are up (Sections 6–7), enable the `customer_churn_incremental_etl` DAG. Each run ingests one monthly period from `data/customer_churn_1M.csv`, writes to the star schema with SHA-256 row-level `row_fp` watermarks, logs into `data_lineage_log`, triggers the MLOps DAG (which retrains XGBoost when enough new data has accumulated and promotes the new model to `champion` if it beats the current one on ROC-AUC / PR-AUC), and finally syncs the champion model into `deploy_to_production/artifacts/current/` for serving.

**b) Watermark audit.** Manually trigger `customer_churn_fingerprint_verification` from the Airflow UI to re-hash business columns and verify they still match the stored `row_fp` for every dimension and the fact table. Demonstrates tamper detection — see §3.5 of the project report.

**c) EDA notebook.** Open `dataops/churn_eda.ipynb` and select the **Python (bt4301-venv)** kernel. The notebook reads from MySQL via SQLAlchemy and walks through churn-rate breakdowns, correlations, and Cramér's V / mutual-information feature signals.

**d) Production dashboard.** With the FastAPI backend and dashboard running (Section 8), open http://localhost:3000 to score customers against the current champion model.

After the ETL has loaded data, use the **SQLTools** connection (Section 5) to confirm the star-schema tables, `train_churn_model` view, `data_lineage_log` audit table, and `mlops_monitor_log` snapshots are all present.

This should fully set up the Python environment, MySQL data warehouse, MLflow tracking server, Airflow orchestration, notebook execution, and production serving for the project.
