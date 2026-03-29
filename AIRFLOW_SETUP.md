# Airflow Setup Guide

This documents the exact steps taken to set up Apache Airflow for the BT4301 Group 10 project, using the `customer_churn_etl_dag.py` pipeline.

---

## Prerequisites

- MySQL is installed and accessible
- Python venv exists at `~/bt4301/BT4301-Project-Group-10/venv`
- The CSV data file exists at `~/bt4301/BT4301-Project-Group-10/data/customer_churn_1M.csv`

---

## Step 1: Start MySQL

```bash
service mysql start
```

Verify it is running:

```bash
service mysql status
```

---

## Step 2: Create the required databases

Log in as root and create two databases — one for Airflow's metadata, one for the customer churn data warehouse:

```bash
mysql -u root -e "
  CREATE DATABASE IF NOT EXISTS airflow_group10;
  CREATE DATABASE IF NOT EXISTS customer_churn;
  GRANT ALL PRIVILEGES ON airflow_group10.* TO 'bt4301'@'localhost';
  GRANT ALL PRIVILEGES ON customer_churn.*  TO 'bt4301'@'localhost';
  FLUSH PRIVILEGES;
"
```

> The `airflow_group10` database stores Airflow's internal metadata (DAG runs, task instances, etc.).
> The `customer_churn` database is the data warehouse the ETL pipeline writes into.

---

## Step 3: Fix the path mismatch in `airflow.cfg`

The config file originally pointed to `/root/bt4301_group_project/...` but the repo is cloned at `/root/bt4301/...`. All six occurrences need to be updated:

File: `dataops/airflow/airflow.cfg`

| Setting | Old value | New value |
|---|---|---|
| `dags_folder` | `.../bt4301_group_project/...` | `.../bt4301/...` |
| `plugins_folder` | `.../bt4301_group_project/...` | `.../bt4301/...` |
| `sql_alchemy_conn` | `.../bt4301_group_project/...` | `.../bt4301/...` |
| `base_log_folder` | `.../bt4301_group_project/...` | `.../bt4301/...` |
| `dag_processor_child_process_log_directory` | `.../bt4301_group_project/...` | `.../bt4301/...` |
| `config_file` (webserver) | `.../bt4301_group_project/...` | `.../bt4301/...` |

The corrected values use the actual repo path:

```
dags_folder = /root/bt4301/BT4301-Project-Group-10/dataops/airflow/dags
```

---

## Step 4: Fix the CSV path in `customer_churn_etl_functions.py`

File: `dataops/airflow/dags/customer_churn_etl_functions.py`

Change:

```python
CSV_FILE_PATH = "/root/bt4301_group_project/BT4301-Project-Group-10/data/customer_churn_1M.csv"
```

To:

```python
CSV_FILE_PATH = "/root/bt4301/BT4301-Project-Group-10/data/customer_churn_1M.csv"
```

---

## Step 5: Set `AIRFLOW_HOME` and activate the project venv

**This must be done before every `airflow` command.** Airflow defaults to `~/airflow` if `AIRFLOW_HOME` is not set, and will not find the project's DAGs or config.

```bash
cd ~/bt4301/BT4301-Project-Group-10
source venv/bin/activate
export AIRFLOW_HOME="$(pwd)/dataops/airflow"
```

> Use the **project venv** (`~/bt4301/.../venv`), not `~/python3venv`.
> `~/python3venv` has Airflow 3.0.0 which crashes on SQLAlchemy 2.x due to a `TaskInstance.dag_model` annotation bug.
> The project venv has Airflow 3.1.8 which is fixed.

---

## Step 6: Initialise the Airflow metadata database

> Note: Airflow 3.x replaced `airflow db init` with `airflow db migrate`.

```bash
airflow db migrate
```

This creates the SQLite database at `dataops/airflow/airflow.db` and stamps it with the latest migration revision.

---

## Step 7: Start Airflow

### Option A — Standalone (all-in-one, easiest)

```bash
airflow standalone
```

This starts the scheduler, dag-processor, triggerer, and api-server in one process. On first run it auto-generates a password for the `admin` user and prints it to stdout:

```
Simple auth manager | Password for user 'admin': <generated-password>
```

The password is also saved to:

```
dataops/airflow/simple_auth_manager_passwords.json.generated
```

### Option B — Individual processes (as used in `start_labs.sh`)

```bash
airflow scheduler &
airflow dag-processor &
airflow api-server
```

### `start_labs.sh` (fixed version)

```bash
#!/bin/bash
source ~/.bashrc
source ~/bt4301/BT4301-Project-Group-10/venv/bin/activate
export AIRFLOW_HOME=~/bt4301/BT4301-Project-Group-10/dataops/airflow
airflow scheduler &
airflow dag-processor &
airflow api-server
```

---

## Step 8: Access the UI

Open `http://localhost:8089` in your browser.

- **Username:** `admin`
- **Password:** printed on first start (or check `dataops/airflow/simple_auth_manager_passwords.json.generated`)

---

## Step 9: Verify the DAG is loaded

In the Airflow UI, the `customer_churn_incremental_etl` DAG should appear under the DAGs list.

It is scheduled to run every minute (`*/1 * * * *`), and on each run it:
1. Reads the current period from `/tmp/airflow_churn_period_counter.txt`
2. Extracts one month's worth of rows from `customer_churn_1M.csv`
3. Transforms the data into dimension and fact tables
4. Loads new rows into the `customer_churn` MySQL database

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `invalid choice: 'init'` | Airflow 3.x removed `db init` | Use `airflow db migrate` |
| `invalid choice: 'users'` | Airflow 3.x removed `users create` | Edit `simple_auth_manager_users` in `airflow.cfg` |
| `MappedAnnotationError: TaskInstance.dag_model` | Airflow 3.0.0 bug with SQLAlchemy 2.x | Use the project venv (Airflow 3.1.8), not `~/python3venv` |
| DAG not appearing | `AIRFLOW_HOME` not set, pointing to `~/airflow` | `export AIRFLOW_HOME=.../dataops/airflow` before starting |
| `address already in use` on port 8793 | Previous Airflow process still holding the scheduler port | `pkill -f airflow` then restart |
