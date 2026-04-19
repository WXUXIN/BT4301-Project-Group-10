# BT4301 Project – Group 10 (Customer Churn Analytics)

End-to-end **Customer Churn** analytics project covering DataOps (Airflow-orchestrated ETL into a MySQL star schema with row-level fingerprint watermarking and lineage), MLOps (MLflow tracking + XGBoost retraining with champion promotion), and DevOps (FastAPI + dashboard deployment via Docker).

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Create a Python venv and install dependencies](#2-create-a-python-venv-and-install-dependencies)
3. [Start MySQL](#3-start-mysql-ubuntu--docker)
4. [MySQL setup](#4-mysql-setup)
5. [SQLTools connection](#5-sqltools-connection-view-ingested-tables)
6. [MLflow Tracking Server](#6-mlflow-tracking-server)
7. [Apache Airflow (orchestration)](#7-apache-airflow-orchestration)
8. [Production serving (FastAPI + ChurnGuard Dashboard)](#8-production-serving-fastapi--churnguard-dashboard)
9. [Running the customer churn pipeline](#9-running-the-customer-churn-pipeline)

---

### 1. Prerequisites

- **Operating system (recommended)**:
  - Ubuntu running **inside Docker** (e.g. dev container), or
  - Ubuntu on Windows via **WSL2 (Windows Subsystem for Linux)**.
- **Python**: 3.10 (or compatible 3.10.x)
- **MySQL Server**: 8.x (or compatible)
- **Docker + Docker Compose v2**: for containerised deployment (Section 8)
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

   airflow scheduler & airflow dag-processor & airflow api-server
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

---

### 8. Production serving (FastAPI + ChurnGuard Dashboard)

The `deploy_to_production/` directory contains a FastAPI inference backend and the **ChurnGuard** dashboard for serving the MLflow champion model. The Docker image contains only app code; the champion model artifacts are mounted from the host at runtime.

#### 8.1 Directory layout

```
deploy_to_production/
├── app.py                          # FastAPI serving application
├── index.html                      # ChurnGuard frontend dashboard
├── latest_predictions_input.json   # Customer feature data for scoring
├── sync_champion_from_mlflow.py    # Pulls champion artifacts from MLflow
├── requirements.txt                # Serving container dependencies
├── Dockerfile                      # Image build (no model artifacts baked in)
├── compose.yaml                    # Docker Compose with volume mount
├── .dockerignore
└── artifacts/
    └── current/                    # ← populated by sync script (gitignored)
        ├── model/
        │   ├── model.pkl           # sklearn Pipeline (preprocessor + XGBClassifier)
        │   └── MLmodel
        ├── feature_lists.json
        └── metadata.json
```

#### 8.2 Running locally (without Docker)

Two terminals, both from `deploy_to_production/`:

```bash
# Terminal 1 — FastAPI backend (requires model artifacts)
pip install -r requirements.txt
ARTIFACTS_DIR=./artifacts/current uvicorn app:app --reload --port 8000

# Terminal 2 — Static file server for the frontend
python3 -m http.server 3000
```

Open **http://localhost:3000**, then click **Run Predictions**.

> **No model artifacts?** The dashboard automatically detects the API is offline and switches to **Demo Mode** (client-side mock scoring). The header pill turns orange. All UI features — filtering, email modal, export — still work.

> **Why a static file server?** `fetch('latest_predictions_input.json')` is blocked by browsers when opening `index.html` directly from the filesystem (`file://`). A local HTTP server (`python3 -m http.server`) bypasses this restriction.

#### 8.3 Running via Docker

**First-time setup — build the image:**

```bash
# from the repo root
docker compose -f deploy_to_production/compose.yaml build
```

**Sync the current champion from MLflow:**

| Mode | Command | When to use |
|------|---------|-------------|
| Remote (HTTP) | `python deploy_to_production/sync_champion_from_mlflow.py` | MLflow server is up |
| Local (offline) | `python deploy_to_production/sync_champion_from_mlflow.py --local` | Server is down / for demos |

Local mode reads `mlflow.db` with Python's built-in `sqlite3` module — no extra dependencies — then copies `model.pkl` directly from `mlruns/`. The result is identical: `artifacts/current/model/`, `feature_lists.json`, `metadata.json`.

Custom database path (if `mlflow.db` is not at the project root):

```bash
python deploy_to_production/sync_champion_from_mlflow.py --local --db /path/to/mlflow.db
```

**Start the serving container:**

```bash
docker compose -f deploy_to_production/compose.yaml up -d
```

The compose file mounts `./artifacts/current/` into the container, so any new champion synced by the ETL DAG is picked up on container restart. No image rebuild is needed.

**Verify the service is running:**

```bash
# health check
curl http://localhost:8000/health

# Swagger UI (browser)
open http://localhost:8000/docs
```

**Stopping / removing the container:**

```bash
docker compose -f deploy_to_production/compose.yaml down
```

#### 8.4 Updating the champion (after a new retrain promotion)

**Auto-sync from MLflow:** The ETL DAG's final task (`sync_champion_if_promoted`) automatically pulls the current champion model from MLflow into `deploy_to_production/artifacts/current/` after every successful pipeline run. No manual sync is required.

To manually update:

```bash
# 1. Sync new champion artifacts to the host
#    (use --local if the MLflow HTTP server is not running)
python deploy_to_production/sync_champion_from_mlflow.py --local

# 2. Restart the container (picks up the new artifacts from the volume)
docker compose -f deploy_to_production/compose.yaml restart churn-api

# 3. Confirm new version is live
curl http://localhost:8000/health
```

#### 8.5 API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service info + champion provenance |
| GET | `/health` | Health check + loaded model metadata |
| POST | `/predict` | Score a single customer |
| POST | `/predict-batch` | Score a list of customers |
| GET | `/docs` | Swagger UI |
| GET | `/redoc` | ReDoc UI |

**POST /predict — example request:**

```bash
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "customer_satisfaction": 2.5,
    "num_complaints": 3,
    "num_service_calls": 5,
    "late_payments": 2,
    "has_phone_service": 1,
    "has_internet_service": 1,
    "has_tech_support": 0,
    "has_streaming_tv": 1,
    "has_streaming_movies": 0,
    "high_risk_flag": 1,
    "is_auto_pay": 0,
    "tenure_segment": "0_6",
    "contract": "Month-to-month"
  }' | python -m json.tool
```

**POST /predict-batch — example request:**

```bash
curl -s -X POST http://localhost:8000/predict-batch \
  -H "Content-Type: application/json" \
  -d '[
    {
      "customer_satisfaction": 4.5, "num_complaints": 0,
      "num_service_calls": 1, "late_payments": 0,
      "has_phone_service": 1, "has_internet_service": 1,
      "has_tech_support": 1, "has_streaming_tv": 0,
      "has_streaming_movies": 0, "high_risk_flag": 0,
      "is_auto_pay": 1, "tenure_segment": "24_plus",
      "contract": "Two year"
    },
    {
      "customer_satisfaction": 1.5, "num_complaints": 5,
      "num_service_calls": 8, "late_payments": 3,
      "has_phone_service": 1, "has_internet_service": 1,
      "has_tech_support": 0, "has_streaming_tv": 1,
      "has_streaming_movies": 1, "high_risk_flag": 1,
      "is_auto_pay": 0, "tenure_segment": "0_6",
      "contract": "Month-to-month"
    }
  ]' | python -m json.tool
```

**Response shape:**

```json
{
  "registered_model_name": "customer_churn_xgb",
  "champion_version": "3",
  "decision_threshold": 0.57,
  "churn_probability": 0.7842,
  "churn_prediction": 1,
  "risk_tier": "high"
}
```

Risk tiers:
- `low` — probability < 0.3
- `medium` — 0.3 ≤ probability < 0.6
- `high` — probability ≥ 0.6

#### 8.6 ChurnGuard Dashboard features

`index.html` is a single-page dashboard for viewing churn predictions, exploring customer profiles, and drafting retention emails. It calls the FastAPI backend for scoring and falls back to client-side mock scoring automatically if the API is unavailable.

**Header bar** — Displays the active model name and version, decision threshold, and an API status pill:
- **Green — API Connected**: predictions come from the live FastAPI model
- **Orange — Demo Mode**: API is offline; predictions use client-side heuristic scoring
- **Grey — Not connected**: predictions have not been run yet

**Summary stat cards** — Four cards update whenever the period or filters change:

| Card | What it shows |
|---|---|
| Customers Scored | Total customers in the selected period |
| At Risk | Count and percentage of customers predicted to churn |
| High Risk | Count of `high` risk-tier customers; medium count shown below |
| Avg Churn Prob | Average churn probability across at-risk customers only |

**Period selector** — The 1,142 customers in `latest_predictions_input.json` are split into three equal mock periods to simulate a rolling monthly data pipeline:

| Option | Records |
|---|---|
| Period 3 — Latest *(default)* | Last third (~381 customers) |
| Period 2 | Middle third |
| Period 1 | First third |
| All Periods | All 1,142 customers |

Switching periods instantly re-filters the already-scored predictions — no re-call to the API.

**Risk filter toggle:**
- **At-Risk Only** *(default)*: shows only customers where `churn_prediction = 1`
- **All Customers**: shows everyone, including low-risk customers

**Customer ID search** — Live text filter applied on top of the period and risk filters. Useful for looking up a specific customer before sending a retention email.

**Predictions table** — Each row shows:

| Column | Detail |
|---|---|
| Customer ID | Monospace, coloured to stand out |
| Churn Probability | Inline bar + percentage, colour-coded by risk tier |
| Risk Tier | Badge: red = high, orange = medium, green = low |
| Contract | Badge: yellow = Monthly (highest risk), teal = 1-Year, green = 2-Year |
| Tenure Segment | Badge: Infant / Stable / Loyal / Veteran |
| Actions | Email button (at-risk only) + expand toggle |

**Expandable customer detail row** — Click **▼** on any row to reveal a detail panel showing:
- Satisfaction score, complaint count, service call count, late payments — each colour-coded (red if concerning, orange if moderate, green if healthy)
- Churn probability to 2 decimal places
- Whether the `high_risk_flag` is set
- Service subscription chips: Phone, Internet, Tech Support, Streaming TV, Movies, Auto Pay — each shown as active (green) or inactive (grey strikethrough)

**Retention email modal** — The **✉ Email** button appears on every at-risk customer row. Clicking it opens a modal pre-populated with:

- **To**: a mock email address derived from the customer ID
- **Subject** and **Message**: generated based on the customer's risk profile

The message template is chosen by matching against the customer's features in priority order:

| Condition | Message angle |
|---|---|
| High risk + month-to-month contract | Offer 20% discount to switch to annual plan |
| High risk + ≥2 complaints | Apology + service credit + dedicated account manager |
| High risk + ≥1 late payment | Flexible payment plan options |
| High risk + satisfaction ≤ 4 | Personal outreach to understand dissatisfaction |
| High risk (other) | Loyalty rewards + plan review |
| Medium risk | General check-in and plan optimisation offer |

All fields are fully editable before sending. Clicking **Send Email** closes the modal and shows a toast notification — no real email is sent (mock only).

**Export CSV** — Downloads a `.csv` of the currently visible records (respects the active period and risk filter). Columns: `customer_id`, `churn_probability`, `churn_prediction`, `risk_tier`, `contract`, `tenure_segment`, `customer_satisfaction`, `num_complaints`, `num_service_calls`, `late_payments`, `high_risk_flag`. File is named `churn_<period>_<date>.csv`.

#### 8.7 Environment variables

| Variable | Default (in container) | Description |
|---|---|---|
| `ARTIFACTS_DIR` | `/app/artifacts/current` | Path to synced champion artifacts |

---

### 9. Running the customer churn pipeline

**a) Production-style (Airflow, recommended).** Once Airflow + MLflow are up (Sections 6–7), enable the `customer_churn_incremental_etl` DAG. Each run ingests one monthly period from `data/customer_churn_1M.csv`, writes to the star schema with SHA-256 row-level `row_fp` watermarks, logs into `data_lineage_log`, triggers the MLOps DAG (which retrains XGBoost when enough new data has accumulated and promotes the new model to `champion` if it beats the current one on ROC-AUC / PR-AUC), and finally syncs the champion model into `deploy_to_production/artifacts/current/` for serving.

**b) Watermark audit.** Manually trigger `customer_churn_fingerprint_verification` from the Airflow UI to re-hash business columns and verify they still match the stored `row_fp` for every dimension and the fact table. Demonstrates tamper detection — see §3.5 of the project report.

**c) EDA notebook.** Open `dataops/churn_eda.ipynb` and select the **Python (bt4301-venv)** kernel. The notebook reads from MySQL via SQLAlchemy and walks through churn-rate breakdowns, correlations, and Cramér's V / mutual-information feature signals.

**d) Production dashboard.** With the FastAPI backend and dashboard running (Section 8), open http://localhost:3000 to score customers against the current champion model.

After the ETL has loaded data, use the **SQLTools** connection (Section 5) to confirm the star-schema tables, `train_churn_model` view, `data_lineage_log` audit table, and `mlops_monitor_log` snapshots are all present.

This should fully set up the Python environment, MySQL data warehouse, MLflow tracking server, Airflow orchestration, notebook execution, and production serving for the project.
