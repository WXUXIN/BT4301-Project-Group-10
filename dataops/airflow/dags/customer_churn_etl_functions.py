import pandas as pd
from sqlalchemy import inspect
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

# Update this to your real file path
CSV_FILE_PATH = "/root/bt4301/BT4301-Project-Group-10/data/customer_churn_1M.csv"

# Target MySQL data warehouse
DATAWAREHOUSE_DB = "mysql://bt4301:password@localhost:3306/customer_churn"


# ---------------------------------------------------------
# PERIOD LOGIC
# ---------------------------------------------------------

def get_base_signup_date():
    """
    Read only the signup_date column from the CSV and return
    the earliest signup_date.

    This becomes the starting point for period 1.
    """
    df_dates = pd.read_csv(
        CSV_FILE_PATH,
        usecols=["signup_date"],
        parse_dates=["signup_date"]
    )

    base_date = df_dates["signup_date"].min()
    return base_date


def get_period_date_range(period):
    """
    Convert a period number into a monthly date range.

    Example:
    period = 1 -> [base_date, base_date + 1 month)
    period = 2 -> [base_date + 1 month, base_date + 2 months)

    We use half-open intervals:
    start_date <= signup_date < end_date
    """
    base_date = get_base_signup_date()

    start_date = base_date + relativedelta(months=period - 1)       # eg. start_date = month 3 and end_date = month 4
    end_date = base_date + relativedelta(months=period)

    return start_date, end_date


def get_max_period():
    """
    Calculate total number of monthly periods existing in the dataset
    based on the minimum and maximum signup_date.
    """
    df_dates = pd.read_csv(
        CSV_FILE_PATH,
        usecols=["signup_date"],
        parse_dates=["signup_date"]
    )

    min_date = df_dates["signup_date"].min()
    max_date = df_dates["signup_date"].max()

    # count number of months between min and max
    total_months = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
    return total_months


# ---------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------

def extract_customer_churn_batch(start_date, end_date, chunksize=100000):
    """
    Extract only rows whose signup_date falls within the given monthly period.

    Since the source is a CSV, we read it in chunks so it is more memory-friendly.
    This is the CSV equivalent of querying a source database by date range.
    """
    batch_list = []

    for chunk in pd.read_csv(           # reads all the 1,000,000 rows but in 100,000 chunks
        CSV_FILE_PATH,
        parse_dates=["signup_date"],
        chunksize=chunksize
    ):
        filtered_chunk = chunk[        # filters each chunk based on whether its between start_date and end_date
            (chunk["signup_date"] >= start_date) &
            (chunk["signup_date"] < end_date)
        ].copy()

        if not filtered_chunk.empty:
            batch_list.append(filtered_chunk)

    if batch_list:
        df_batch = pd.concat(batch_list, ignore_index=True)
    else:
        df_batch = pd.DataFrame()

    return df_batch


# ---------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------

def transform_dim_customer(df):
    """
    Build the customer dimension.
    These are mostly demographic/customer profile attributes.
    """
    dim_customer = df[[         # Picks only the profile columns from the raw dataframe. .copy() creates an independent copy so any changes don't affect the original df.
        "customer_id",
        "signup_date",
        "age",
        "gender",
        "annual_income",
        "education",
        "marital_status",
        "dependents",
        "senior_citizen"
    ]].copy()

    # simple missing value handling
    dim_customer["annual_income"] = dim_customer["annual_income"].fillna(
        dim_customer["annual_income"].median()
    )

    dim_customer = dim_customer.drop_duplicates(subset=["customer_id"])
    return dim_customer


def transform_dim_account(df):
    """
    Build the account dimension.
    These are account/contract-related features.
    """
    dim_account = df[[
        "customer_id",
        "tenure",
        "contract",
        "payment_method",
        "paperless_billing"
    ]].copy()

    # # example engineered column                       # can decide if we want this column
    # dim_account["tenure_group"] = pd.cut(
    #     dim_account["tenure"],
    #     bins=[0, 12, 24, 48, 72],
    #     labels=["0_12", "13_24", "25_48", "49_72"],
    #     include_lowest=True
    # )

    dim_account = dim_account.drop_duplicates(subset=["customer_id"])
    return dim_account


def transform_dim_service(df):
    """
    Build the service dimension.
    These are the subscribed services for each customer / usage setup features.
    """
    dim_service = df[[
        "customer_id",
        "num_services",
        "has_phone_service",
        "has_internet_service",
        "has_online_security",
        "has_online_backup",
        "has_device_protection",
        "has_tech_support",
        "has_streaming_tv",
        "has_streaming_movies"
    ]].copy()

    dim_service = dim_service.drop_duplicates(subset=["customer_id"])
    return dim_service


def transform_dim_behavior(df):
    """
    Build the behavior dimension.
    These are behavioral and risk-related features.
    """
    dim_behavior = df[[
        "customer_id",
        "customer_satisfaction",
        "num_complaints",
        "num_service_calls",
        "late_payments",
        "avg_monthly_gb",
        "days_since_last_interaction",
        "credit_score"
    ]].copy()

    # simple missing value handling
    dim_behavior["customer_satisfaction"] = dim_behavior["customer_satisfaction"].fillna(
        dim_behavior["customer_satisfaction"].median()
    )
    dim_behavior["num_complaints"] = dim_behavior["num_complaints"].fillna(0)

    dim_behavior["avg_monthly_gb"] = dim_behavior["avg_monthly_gb"].fillna(     # Median is more representative of what a normal customer uses rather than being pulled up by those heavy users.
        dim_behavior["avg_monthly_gb"].median()
    )
    dim_behavior["credit_score"] = dim_behavior["credit_score"].fillna(
        dim_behavior["credit_score"].median()
    )

    # engineered feature, Flag customer as high risk (1) if: late_payments >= 2, num_complaints >= 3, or credit_score < 580
    dim_behavior["high_risk_flag"] = (
        (dim_behavior["late_payments"] >= 2) |
        (dim_behavior["num_complaints"] >= 3) |
        (dim_behavior["credit_score"] < 580)
    ).astype(int)

    # removes duplicate rows where customer_id appears more than once, particularly focusing on customer_id
    dim_behavior = dim_behavior.drop_duplicates(subset=["customer_id"])
    return dim_behavior


def transform_fact_customer_churn(df):
    """
    Build the fact table.
    This contains the main measurable business values and target.
    """
    fact_customer_churn = df[[
        "customer_id",
        "monthlycharges",
        "totalcharges",
        "churn"
    ]].copy()

    # example engineered metric
    fact_customer_churn["avg_charge_per_month"] = (
        fact_customer_churn["totalcharges"] /
        df["tenure"].replace(0, 1) # swaps 0 to 1 before dividing
    )

    fact_customer_churn = fact_customer_churn.drop_duplicates(subset=["customer_id"])
    return fact_customer_churn



# ---------------------------------------------------------
# LOAD HELPERS
# ---------------------------------------------------------

def load_new_dimension_rows(df, table_name, key_cols, dwh_engine):
    """
    Insert only new dimension members into the target table.
    """
    # 1. Remove duplicates within the incoming dataframe itself
    df = df.drop_duplicates(subset=key_cols).copy()
    # helps retrieve detailed metadata about a database engine
    inspector = inspect(dwh_engine)

    # If table does not exist, create it and load all rows
    if not inspector.has_table(table_name):
        df.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False
        )
        print(f"{table_name} created and {len(df)} rows inserted.")
        return

    # Read existing keys only
    key_list = ", ".join(key_cols)
    query = f"SELECT {key_list} FROM {table_name}"
    existing_keys = pd.read_sql(query, con=dwh_engine)

    # Keep only rows not already in target
    df_new = df.merge(existing_keys, on=key_cols, how="left", indicator=True)
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False
        )
        print(f"{len(df_new)} new rows inserted into {table_name}.")
    else:
        print(f"No new rows to insert into {table_name}.")



def load_new_fact_rows(df, table_name, key_cols, dwh_engine):
    """
    Insert only new fact rows.
    Since each customer appears once in this dataset,
    customer_id can be used as the business key.
    """
    df = df.drop_duplicates(subset=key_cols).copy()
    inspector = inspect(dwh_engine)

    if not inspector.has_table(table_name):
        df.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False
        )
        print(f"{table_name} created and {len(df)} rows inserted.")
        return

    key_list = ", ".join(key_cols)
    query = f"SELECT {key_list} FROM {table_name}"
    existing_keys = pd.read_sql(query, con=dwh_engine)

    df_new = df.merge(existing_keys, on=key_cols, how="left", indicator=True)
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False
        )
        print(f"{len(df_new)} new rows inserted into {table_name}.")
    else:
        print(f"No new rows to insert into {table_name}.")




























# import logging
# import time
# from pathlib import Path

# import mysql.connector
# import numpy as np
# import pandas as pd
# from sqlalchemy import create_engine, text

# _THIS_DIR = Path(__file__).resolve().parent
# # base_dir: project root
# BASE_DIR = _THIS_DIR.parent if _THIS_DIR.name == "dataops" else _THIS_DIR.parent.parent.parent
# DATA_PATH = BASE_DIR / "data"
# DEFAULT_STAGING_DIR = BASE_DIR / "dataops" / "airflow" / "staging"

# logger = logging.getLogger(__name__)

# TABLE_CONFIG = {
#     "application_train.csv": ("application_train", "application_train"),
#     "application_test.csv": ("application_test", "application_test"),
#     "bureau.csv": ("bureau", "bureau"),
#     "bureau_balance.csv": ("bureau_balance", "bureau_balance"),
# }

# AMT_CAP_COLS = ["AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY"]
# VALID_STATUS_BUREAU_BALANCE = ["C", "X", "0", "1", "2", "3", "4", "5"]


# def get_default_mysql_config():
#     return {
#         "host": "localhost",
#         "user": "bt4301",
#         "password": "password",
#         "database": "home_credit",
#         "port": 3306,
#     }


# def extract(data_path: Path = None, staging_dir: Path = None,) -> dict:         # Take raw CSV files → copy them into a staging folder → return where these files are stored
#     data_path = data_path or DATA_PATH                                          # extract function returns a dictionary
#     staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
#     raw_dir = staging_dir / "raw"         # inside staging folder → create a subfolder called raw
#     raw_dir.mkdir(parents=True, exist_ok=True)      # create the folder if it doesn’t exist, don’t crash if it already exists

#     result = {}
#     for csv_name, (stage_name, _) in TABLE_CONFIG.items():      # Loop through all CSV files
#         csv_path = data_path / csv_name     # Combine folder path + file name
#         if not csv_path.exists():
#             raise FileNotFoundError(f"CSV not found: {csv_path}. Place datasets in {data_path}")
#         logger.info("Extracting %s from %s", csv_name, csv_path)        # Log what’s happening
#         df = pd.read_csv(csv_path)      # actual extraction, reading the csv
#         out_path = raw_dir / f"{stage_name}.csv"        
#         df.to_csv(out_path, index=False)        # # Save into staging
#         result[stage_name] = str(out_path)
#         logger.info("Wrote %s rows to %s", len(df), out_path)

#     return result


# def transform(staging_dir: Path = None) -> dict:
#     staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
#     raw_dir = staging_dir / "raw"
#     out_dir = staging_dir / "transformed"
#     out_dir.mkdir(parents=True, exist_ok=True)

#     result = {}

#     # --- application_train ---
#     path_train = raw_dir / "application_train.csv"
#     if not path_train.exists():
#         raise FileNotFoundError(f"Staging file not found: {path_train}. Run extract first.")
#     app_clean = pd.read_csv(path_train)

#     if "CODE_GENDER" in app_clean.columns:
#         app_clean["CODE_GENDER"] = app_clean["CODE_GENDER"].fillna("XNA")
#     if "DAYS_EMPLOYED" in app_clean.columns:
#         app_clean["FLAG_UNEMPLOYED"] = (app_clean["DAYS_EMPLOYED"] == 365243).astype(int)
#         app_clean["DAYS_EMPLOYED"] = app_clean["DAYS_EMPLOYED"].replace(365243, 0)

#     AMT_Q99_CAPS = {}
#     for col in AMT_CAP_COLS:
#         if col in app_clean.columns and app_clean[col].notna().any():
#             AMT_Q99_CAPS[col] = app_clean[col].quantile(0.99)
#             app_clean[col] = app_clean[col].clip(upper=AMT_Q99_CAPS[col])

#     before_dedup = len(app_clean)
#     app_clean = app_clean.drop_duplicates(subset=["SK_ID_CURR"], keep="first")
#     if len(app_clean) < before_dedup:
#         logger.warning("Dropped %s duplicate SK_ID_CURR in application_train", before_dedup - len(app_clean))

#     num_cols = [
#         c for c in app_clean.select_dtypes(include=[np.number]).columns
#         if c != "TARGET" and app_clean[c].isna().any()
#     ]
#     app_clean = app_clean.fillna({c: 0 for c in num_cols})

#     app_clean.to_csv(out_dir / "application_train.csv", index=False)
#     result["application_train"] = str(out_dir / "application_train.csv")
#     logger.info("Transformed application_train: %s rows", len(app_clean))

#     # --- application_test (use train caps) ---
#     path_test = raw_dir / "application_test.csv"
#     if not path_test.exists():
#         raise FileNotFoundError(f"Staging file not found: {path_test}. Run extract first.")
#     app_test_clean = pd.read_csv(path_test)

#     if "CODE_GENDER" in app_test_clean.columns:
#         app_test_clean["CODE_GENDER"] = app_test_clean["CODE_GENDER"].fillna("XNA")
#     if "DAYS_EMPLOYED" in app_test_clean.columns:
#         app_test_clean["FLAG_UNEMPLOYED"] = (app_test_clean["DAYS_EMPLOYED"] == 365243).astype(int)
#         app_test_clean["DAYS_EMPLOYED"] = app_test_clean["DAYS_EMPLOYED"].replace(365243, 0)
#     for col, cap in AMT_Q99_CAPS.items():
#         if col in app_test_clean.columns:
#             app_test_clean[col] = app_test_clean[col].clip(upper=cap)
#     app_test_clean = app_test_clean.drop_duplicates(subset=["SK_ID_CURR"], keep="first")
#     num_cols_test = [c for c in app_test_clean.select_dtypes(include=[np.number]).columns if app_test_clean[c].isna().any()]
#     app_test_clean = app_test_clean.fillna({c: 0 for c in num_cols_test})

#     app_test_clean.to_csv(out_dir / "application_test.csv", index=False)
#     result["application_test"] = str(out_dir / "application_test.csv")
#     logger.info("Transformed application_test: %s rows", len(app_test_clean))

#     # --- bureau ---
#     path_bureau = raw_dir / "bureau.csv"
#     if not path_bureau.exists():
#         raise FileNotFoundError(f"Staging file not found: {path_bureau}. Run extract first.")
#     bureau_clean = pd.read_csv(path_bureau).fillna(0)
#     bureau_clean.to_csv(out_dir / "bureau.csv", index=False)
#     result["bureau"] = str(out_dir / "bureau.csv")
#     logger.info("Transformed bureau: %s rows", len(bureau_clean))

#     # --- bureau_balance ---
#     path_bb = raw_dir / "bureau_balance.csv"
#     if not path_bb.exists():
#         raise FileNotFoundError(f"Staging file not found: {path_bb}. Run extract first.")
#     bureau_balance_clean = pd.read_csv(path_bb)
#     if "STATUS" in bureau_balance_clean.columns:
#         bureau_balance_clean["STATUS"] = bureau_balance_clean["STATUS"].where(
#             bureau_balance_clean["STATUS"].isin(VALID_STATUS_BUREAU_BALANCE), "X"
#         )
#     bureau_balance_clean.to_csv(out_dir / "bureau_balance.csv", index=False)
#     result["bureau_balance"] = str(out_dir / "bureau_balance.csv")
#     logger.info("Transformed bureau_balance: %s rows", len(bureau_balance_clean))

#     return result


# def load(staging_dir: Path = None, mysql_config: dict = None, batch_size: int = 10_000, batch_delay_seconds: float = 0.5) -> None:      # Takes cleaned CSV files → puts them into MySQL database tables
#     staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
#     out_dir = staging_dir / "transformed"       # This is where your cleaned CSV files are, staging/transformed/
#     mysql_config = mysql_config or get_default_mysql_config()       # Get host, username, password & database name (database details)

#     conn = mysql.connector.connect(     # connect to database
#         host=mysql_config["host"],
#         user=mysql_config["user"],
#         password=mysql_config["password"],
#         port=mysql_config["port"],
#     )
#     cur = conn.cursor()
#     cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_config['database']}`;")     # if database doesn't exist, create it, if not, do nothing
#     conn.commit()
#     cur.close()
#     conn.close()

#     connection_string = (
#         f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
#         f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
#     )
#     engine = create_engine(connection_string, echo=False)       # connect to mySQL using SQLalchemy

#     with engine.begin() as conn:            # remove old data completely
#         conn.execute(text("DROP TABLE IF EXISTS bureau_balance;"))
#         conn.execute(text("DROP TABLE IF EXISTS bureau;"))
#         conn.execute(text("DROP TABLE IF EXISTS application_test;"))
#         conn.execute(text("DROP TABLE IF EXISTS application_train;"))

#     tables = ["application_train", "application_test", "bureau", "bureau_balance"]
#     for table in tables:        # for each table, load the cleaned csv into memory
#         path = out_dir / f"{table}.csv"
#         if not path.exists():
#             raise FileNotFoundError(f"Transformed file not found: {path}. Run transform first.")
#         logger.info("Loading %s into MySQL (chunksize=%s, delay=%.1fs)", table, batch_size, batch_delay_seconds)
#         df = pd.read_csv(path)
#         n_rows = len(df)
#         n_chunks = (n_rows + batch_size - 1)

#         for i in range(0, n_rows, batch_size):      # Instead of loading everything at once, load 10,000 rows at a time
#             chunk = df.iloc[i : i + batch_size]
#             chunk_num = i 
#             if_exists = "replace" if chunk_num == 1 else "append"
#             chunk.to_sql(       # sends data into mySQL
#                 name=table,
#                 con=engine,
#                 if_exists=if_exists,
#                 index=False,
#                 chunksize=batch_size,
#             )
#             rows_this_batch = len(chunk)
#             rows_so_far = min(i + batch_size, n_rows)
#             logger.info(
#                 "%s: batch %s/%s ingested %s rows (total so far: %s/%s)",
#                 table, chunk_num, n_chunks, rows_this_batch, rows_so_far, n_rows,
#             )
#             if chunk_num < n_chunks:
#                 time.sleep(batch_delay_seconds)

#         logger.info("Loaded %s: %s rows complete", table, n_rows)

#     logger.info("Loaded application_train, application_test, bureau, bureau_balance into MySQL.")
