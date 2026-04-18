# BT4301 Project – Group 10 (Customer Churn Analytics)

End-to-end **Customer Churn** analytics project covering DataOps (Airflow-orchestrated ETL into a MySQL star schema with row-level fingerprint watermarking and lineage), MLOps (MLflow tracking + XGBoost retraining), and DevOps (deployment).

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

Then, in your notebook UI, select the kernel **“Python (bt4301-venv)”** so the notebook uses this virtual environment.

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

   Tables (`dim_customer`, `dim_account`, `dim_service`, `dim_behavior`, `fact_customer_churn`, `train_churn_model`, `data_lineage_log`) are created by the ETL DAG on first run — no manual DDL is required.

2. **Source data**

   Place the source file `customer_churn_1M.csv` under `data/` (i.e. `data/customer_churn_1M.csv`). The ETL slices it into monthly periods keyed by `signup_date`.

### 5. SQLTools connection (view ingested tables)

Use **SQLTools** in Cursor/VS Code to connect to MySQL and view the tables after the pipeline has ingested data.

1. **Install SQLTools and the MySQL driver**
   - Install the **SQLTools** extension (and **SQLTools MySQL/MariaDB** or **SQLTools MySQL** driver) from the Extensions view in Cursor/VS Code.

2. **Create a new connection**
   - Open the **SQLTools** sidebar (database icon or “SQLTools” in the activity bar).
   - Click **“Add new connection”** (or the + icon).
   - Choose **MySQL** (or MySQL/MariaDB) as the driver.
   - Fill in the connection details to match your MySQL credentials (and the database where the pipeline writes):
     - **Host:** `localhost` (or your MySQL host; if MySQL runs inside Docker, use the host that Cursor can reach, e.g. `localhost` if port 3306 is forwarded).
     - **Port:** `3306`
     - **User:** `bt4301` (or whatever you configured).
     - **Password:** as configured.
     - **Database:** `customer_churn`
   - Give the connection a name (e.g. “Customer Churn DWH”) and save.

3. **Connect and view tables**
   - Select the new connection and click **Connect** (or right‑click → Connect).
   - Once connected, expand the connection → **customer_churn** → **Tables** to see the ingested star-schema tables: **dim_customer**, **dim_account**, **dim_service**, **dim_behavior**, **fact_customer_churn**, the denormalized **train_churn_model** view used for ML training, and the **data_lineage_log** audit table.
   - You can run `SELECT` queries or open table data from the explorer.

Ensure MySQL is running (`sudo service mysql start` in Ubuntu/Docker) and that the host/port are reachable from where Cursor runs (e.g. port 3306 forwarded from the container if MySQL is inside Docker).

### 6. Apache Airflow (orchestration)

The project uses **Apache Airflow** for workflow orchestration. Airflow configuration and DAGs live under `dataops/airflow/`.

**What's included:**

- **`dataops/airflow/airflow.cfg`** — Airflow configuration (paths, executor, database, logging).
- **`dataops/airflow/dags/`** — DAG Python files (scheduler loads DAGs from here):
  - `customer_churn_etl_dag.py` — incremental monthly ETL (`customer_churn_incremental_etl`); chains into the MLOps DAG via `TriggerDagRunOperator`.
  - `customer_churn_fingerprint_verification_dag.py` — re-hashes business columns and compares against stored `row_fp` watermarks (manual trigger, audit).
  - `customer_churn_mlops_dag.py` — XGBoost retraining and monitoring against MLflow (`customer_churn_mlops_pipeline`).
- **`dataops/airflow/plugins/`** — Custom Airflow plugins (if any).
- **`dataops/airflow/logs/`** — Task and DAG processor logs.

**Airflow metadata database (MySQL):**

Airflow uses a **separate** MySQL database from the customer churn data warehouse:

- **Database name:** `airflow_group10`
- **Connection** (in `airflow.cfg`): `mysql+mysqldb://bt4301:password@localhost:3306/airflow_group10`

Create the database in MySQL (same host/user as your project MySQL, or adjust the connection in `airflow.cfg`):

```sql
CREATE DATABASE airflow_group10;
```

**Setup steps:**

1. Airflow + the MySQL provider + `mysqlclient` are already pinned in `dataops/requirements.txt`. Re-install if needed:

   ```bash
   pip install -r dataops/requirements.txt
   ```

2. **Set `AIRFLOW_HOME`** to the project's Airflow directory. **You must do this in the same shell before every `airflow` command**, otherwise Airflow uses `~/airflow` and will not see the project's DAGs (e.g. `customer_churn_incremental_etl`).

   From the project root `BT4301-Project-Group-10`:

   ```bash
   export AIRFLOW_HOME="$(pwd)/dataops/airflow"
   ```

   Or with an absolute path (adjust if your clone path is different):

   ```bash
   export AIRFLOW_HOME=/root/bt4301-group10/BT4301-Project-Group-10/dataops/airflow
   ```

   To avoid setting it every time, add the `export` line to your `~/.bashrc` and run `source ~/.bashrc` (or open a new terminal) before starting Airflow.

3. Initialize the Airflow metadata database:

   ```bash
   airflow db init
   ```

4. Create an admin user (for the web UI):

   ```bash
   airflow users create --role Admin --username admin --email admin@example.com --firstname Admin --lastname User --password password
   ```

   (Change username/password as needed; `airflow.cfg` has `simple_auth_manager_users = admin:admin`.)

5. Start Airflow (in one terminal). **Ensure `AIRFLOW_HOME` is set in this shell** (see step 2):

   ```bash
   cd BT4301-Project-Group-10
   export AIRFLOW_HOME="$(pwd)/dataops/airflow"
   airflow scheduler &
   airflow dag-processor &
   airflow api-server
   ```

   If you see "Searching for files in dags-folder at `/root/airflow/dags`" or only tutorial DAGs, Airflow is using the default home. Stop the processes, set `AIRFLOW_HOME` as above, and start again.

Then open the UI at `http://localhost:8088` (api-server) or your webserver port. The **customer_churn_incremental_etl**, **customer_churn_mlops_pipeline**, and **customer_churn_fingerprint_verification** DAGs will appear once the scheduler reads from the project's `dags` folder.

**Config highlights (in `airflow.cfg`):**

- **Executor:** `LocalExecutor`
- **DAGs folder:** `dataops/airflow/dags`
- **Logs:** `dataops/airflow/logs`
- **Metadata DB:** MySQL `airflow_group10` (see above)

**Customer churn DAG — reset the period counter**

The `customer_churn_incremental_etl` DAG tracks which monthly `signup_date` slice to load next using a local file: `/tmp/airflow_churn_period_counter.txt`. To restart ingestion from **period 1** (e.g. after testing or reprocessing the CSV), remove that file on the **same machine** where the Airflow scheduler runs:

```bash
rm -f /tmp/airflow_churn_period_counter.txt
```

Wait until no run is in the middle of updating the counter before deleting it, so the next scheduled run starts cleanly from period 1.

### 7. Running the customer churn pipeline

Three ways to drive the pipeline, depending on what you want to do:

**a) Production-style (Airflow, recommended).** Once Airflow is up (Section 6), enable the `customer_churn_incremental_etl` DAG. It runs every minute by default, with each run ingesting one monthly period from `data/customer_churn_1M.csv`, writing to the star schema, attaching SHA-256 row-level `row_fp` watermarks, logging into `data_lineage_log`, then triggering `customer_churn_mlops_pipeline` for retraining + monitoring.

**b) Watermark audit.** Manually trigger `customer_churn_fingerprint_verification` from the Airflow UI to re-hash business columns and verify they still match the stored `row_fp` for every dimension and the fact table.

**c) EDA notebook.** Open `dataops/churn_eda.ipynb` and select the **Python (bt4301-venv)** kernel. The notebook reads from MySQL via SQLAlchemy and walks through churn-rate breakdowns, correlations, and Cramér's V / mutual-information feature signals.

After the ETL has loaded data, use the **SQLTools** connection (Section 5) to confirm the star-schema tables (`dim_customer`, `dim_account`, `dim_service`, `dim_behavior`, `fact_customer_churn`), the denormalized `train_churn_model` view, and the `data_lineage_log` audit table are all present and to run queries.

This should fully set up the Python environment, MySQL data warehouse, SQLTools connection, Airflow orchestration, and notebook execution for the project.
