import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text
from dateutil.relativedelta import relativedelta
from sqlalchemy import text

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

# Update this to your real file path
CSV_FILE_PATH = (
    "/root/bt4301_group_project/BT4301-Project-Group-10/data/customer_churn_1M.csv"
)

# Target MySQL data warehouse
DATAWAREHOUSE_DB = "mysql+pymysql://bt4301:password@localhost:3306/customer_churn"


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
        CSV_FILE_PATH, usecols=["signup_date"], parse_dates=["signup_date"]
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

    start_date = base_date + relativedelta(
        months=period - 1
    )  # eg. start_date = month 3 and end_date = month 4
    end_date = base_date + relativedelta(months=period)

    return start_date, end_date


def get_max_period():
    """
    Calculate total number of monthly periods existing in the dataset
    based on the minimum and maximum signup_date.
    """
    df_dates = pd.read_csv(
        CSV_FILE_PATH, usecols=["signup_date"], parse_dates=["signup_date"]
    )

    min_date = df_dates["signup_date"].min()
    max_date = df_dates["signup_date"].max()

    # count number of months between min and max
    total_months = (
        (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
    )
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

    for chunk in pd.read_csv(  # reads all the 1,000,000 rows but in 100,000 chunks
        CSV_FILE_PATH, parse_dates=["signup_date"], chunksize=chunksize
    ):
        filtered_chunk = chunk[  # filters each chunk based on whether its between start_date and end_date
            (chunk["signup_date"] >= start_date) & (chunk["signup_date"] < end_date)
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
    dim_customer = df[
        [  # Picks only the profile columns from the raw dataframe. .copy() creates an independent copy so any changes don't affect the original df.
            "customer_id",
            "signup_date",
            "age",
            "gender",
            "annual_income",
            "education",
            "marital_status",
            "dependents",
            "senior_citizen",
        ]
    ].copy()

    dim_customer["signup_date"] = pd.to_datetime(
        dim_customer["signup_date"]
    ).dt.normalize()

    for col in ("age", "dependents", "senior_citizen"):
        dim_customer[col] = pd.to_numeric(dim_customer[col], errors="coerce").astype(
            "Int64"
        )

    # simple missing value handling
    dim_customer["annual_income"] = dim_customer["annual_income"].fillna(
        dim_customer["annual_income"].median()
    )

    # Feature Engineer --- age groups & tenure segments

    # Age groups
    age_bins = [0, 18, 30, 40, 50, 60, 70, 100]
    age_labels = ["0-18", "18-29", "30-39", "40-49", "50-59", "60-69", "70+"]
    dim_customer["age_group"] = pd.cut(
        dim_customer["age"], bins=age_bins, labels=age_labels, right=True
    )

    # Tenure Segment
    tenure_df = df[["customer_id", "tenure"]].copy()

    dim_customer = dim_customer.merge(tenure_df, on="customer_id", how="left")

    tenure_bins = [0, 6, 24, 60, 100]
    tenure_labels = ["Infant", "Stable", "Loyal", "Veteran"]
    dim_customer["tenure_segment"] = pd.cut(
        dim_customer["tenure"], bins=tenure_bins, labels=tenure_labels
    )

    # Drop tenure months since we have it in account df
    dim_customer = dim_customer.drop(columns=["tenure"])
    dim_customer = dim_customer.drop_duplicates(subset=["customer_id"])

    return dim_customer


def transform_dim_account(df):
    """
    Build the account dimension.
    These are account/contract-related features.
    """
    dim_account = df[
        ["customer_id", "tenure", "contract", "payment_method", "paperless_billing"]
    ].copy()

    # Feature engineered --- 'is_auto_pay'
    autopay_methods = {"credit_card", "bank_transfer"}
    dim_account["is_auto_pay"] = (
        dim_account["payment_method"].isin(autopay_methods).astype(int)
    )

    dim_account = dim_account.drop_duplicates(subset=["customer_id"])
    return dim_account


def transform_dim_service(df):
    """
    Build the service dimension.
    These are the subscribed services for each customer / usage setup features.
    """
    dim_service = df[
        [
            "customer_id",
            "num_services",
            "has_phone_service",
            "has_internet_service",
            "has_online_security",
            "has_online_backup",
            "has_device_protection",
            "has_tech_support",
            "has_streaming_tv",
            "has_streaming_movies",
        ]
    ].copy()

    dim_service = dim_service.drop_duplicates(subset=["customer_id"])
    return dim_service


def transform_dim_behavior(df):
    """
    Build the behavior dimension.
    These are behavioral and risk-related features.
    """
    dim_behavior = df[
        [
            "customer_id",
            "customer_satisfaction",
            "num_complaints",
            "num_service_calls",
            "late_payments",
            "avg_monthly_gb",
            "days_since_last_interaction",
            "credit_score",
        ]
    ].copy()

    # simple missing value handling
    dim_behavior["customer_satisfaction"] = dim_behavior[
        "customer_satisfaction"
    ].fillna(dim_behavior["customer_satisfaction"].median())
    dim_behavior["num_complaints"] = dim_behavior["num_complaints"].fillna(0)

    dim_behavior["avg_monthly_gb"] = dim_behavior[
        "avg_monthly_gb"
    ].fillna(  # Median is more representative of what a normal customer uses rather than being pulled up by those heavy users.
        dim_behavior["avg_monthly_gb"].median()
    )
    dim_behavior["credit_score"] = dim_behavior["credit_score"].fillna(
        dim_behavior["credit_score"].median()
    )

    # engineered feature, Flag customer as high risk (1) if: late_payments >= 2, num_complaints >= 3, or credit_score < 580
    dim_behavior["high_risk_flag"] = (
        (dim_behavior["late_payments"] >= 2)
        | (dim_behavior["num_complaints"] >= 3)
        | (dim_behavior["credit_score"] < 580)
    ).astype(int)

    # removes duplicate rows where customer_id appears more than once, particularly focusing on customer_id
    dim_behavior = dim_behavior.drop_duplicates(subset=["customer_id"])
    return dim_behavior


def transform_fact_customer_churn(df):
    """
    Build the fact table.
    This contains the main measurable business values and target.
    """
    fact_customer_churn = df[
        ["customer_id", "monthlycharges", "totalcharges", "churn"]
    ].copy()

    # example engineered metric
    fact_customer_churn["avg_charge_per_month"] = fact_customer_churn[
        "totalcharges"
    ] / df["tenure"].replace(
        0, 1
    )  # swaps 0 to 1 before dividing

    fact_customer_churn = fact_customer_churn.drop_duplicates(subset=["customer_id"])
    return fact_customer_churn


# ---------------------------------------------------------
# ROW-LEVEL WATERMARKING
# ---------------------------------------------------------

fingerprint_column = "row_fp"


def normalize_value(value):
    """
    Map a cell to a JSON-serializable, stable form for fingerprinting.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    if isinstance(value, (bool, np.bool_)):
        return int(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, Decimal):
        fv = float(value)
        if math.isnan(fv):
            return None
        return f"{fv:.12g}"
    if isinstance(value, (float, np.floating)):
        fv = float(value)
        if pd.isna(fv) or math.isnan(fv):
            return None
        return f"{fv:.12g}"

    return str(value).strip()


def row_fingerprint(row: pd.Series, exclude_cols=None) -> str:
    """
    SHA-256 fingerprint for one row: canonical JSON with sorted object keys.
    """
    if exclude_cols is None:
        exclude_cols = set()
    exclude_cols = set(exclude_cols)

    record = {
        col: normalize_value(row[col])
        for col in sorted(row.index)
        if col not in exclude_cols
    }
    canonical = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def add_row_fingerprints(df, cols_to_hash):
    """
    Append `row_fp` using JSON-canonical row fingerprints.
    """
    df = df.copy()
    cols_sorted = sorted(cols_to_hash)

    def _fp(r):
        return row_fingerprint(r[cols_sorted], exclude_cols=set())

    df[fingerprint_column] = df.apply(_fp, axis=1)
    return df


def columns_for_fingerprint(df):
    """
    All loaded columns except the fingerprint column, sorted for hashing.
    """
    return sorted(c for c in df.columns if c != fingerprint_column)


def ensure_fingerprint_column_exists(table_name, dwh_engine):
    """
    Add `row_fp` to an existing warehouse table if missing.
    """
    inspector = inspect(dwh_engine)

    if not inspector.has_table(table_name):
        return

    existing_columns = {col["name"] for col in inspector.get_columns(table_name)}

    if fingerprint_column not in existing_columns:
        alter_sql = text(
            f"ALTER TABLE `{table_name}` "
            f"ADD COLUMN `{fingerprint_column}` CHAR(64) NULL"
        )
        with dwh_engine.begin() as conn:
            conn.execute(alter_sql)

        print(f"Added {fingerprint_column} column to {table_name}.")


# def build_train_churn_model(engine):
#     """
#     Build the final training dataset by joining fact + dimension tables.
#     This creates a denormalized feature table for ML.
#     """

#     query = """
#     SELECT
#         f.customer_id,
#         f.churn,
#         c.annual_income,
#         a.contract,
#         s.has_phone_service,
#         s.has_internet_service,
#         s.has_tech_support,
#         s.has_streaming_tv,
#         s.has_streaming_movies,
#         b.customer_satisfaction,
#         b.num_complaints,
#         b.num_service_calls,
#         b.late_payments,
#         b.high_risk_flag,
#         c.age_group,
#         c.tenure_segment,
#         a.is_auto_pay
#     FROM fact_customer_churn f
#     LEFT JOIN dim_customer  c ON f.customer_id = c.customer_id
#     LEFT JOIN dim_account   a ON f.customer_id = a.customer_id
#     LEFT JOIN dim_service   s ON f.customer_id = s.customer_id
#     LEFT JOIN dim_behavior  b ON f.customer_id = b.customer_id
#     """

#     df = pd.read_sql(query, engine)

#     print(f"Building train_churn_model with {len(df)} rows...")

#     df.to_sql(
#         'train_churn_model',
#         engine,
#         if_exists='replace',
#         index=False,
#         chunksize=10000
#     )

#     print("train_churn_model table successfully built.")


def build_train_churn_model(dwh_engine):
    drop_sql = text("DROP TABLE IF EXISTS train_churn_model")

    create_sql = text("""
    CREATE TABLE train_churn_model AS
    SELECT
        dc.customer_id,
        dc.signup_date,
        TIMESTAMPDIFF(
            MONTH,
            (SELECT MIN(signup_date) FROM dim_customer),
            dc.signup_date
        ) + 1 AS ingestion_period,
        fc.churn,
        dc.annual_income,
        da.contract,
        ds.has_phone_service,
        ds.has_internet_service,
        ds.has_tech_support,
        ds.has_streaming_tv,
        ds.has_streaming_movies,
        db.customer_satisfaction,
        db.num_complaints,
        db.num_service_calls,
        db.late_payments,
        db.high_risk_flag,
        CASE
            WHEN dc.age < 30 THEN 0
            WHEN dc.age < 50 THEN 1
            ELSE 2
        END AS age_group,
        CASE
            WHEN da.tenure < 12 THEN 0
            WHEN da.tenure < 24 THEN 1
            ELSE 2
        END AS tenure_segment,
        CASE
            WHEN LOWER(da.payment_method) LIKE '%auto%'
              OR LOWER(da.payment_method) LIKE '%automatic%'
            THEN 1 ELSE 0
        END AS is_auto_pay
    FROM dim_customer dc
    JOIN dim_account da ON dc.customer_id = da.customer_id
    JOIN dim_service ds ON dc.customer_id = ds.customer_id
    JOIN dim_behavior db ON dc.customer_id = db.customer_id
    JOIN fact_customer_churn fc ON dc.customer_id = fc.customer_id
    """)

    with dwh_engine.begin() as conn:
        conn.execute(drop_sql)
        conn.execute(create_sql)

    print("train_churn_model rebuilt successfully.")


# ---------------------------------------------------------
# LOAD HELPERS
# ---------------------------------------------------------


def load_new_dimension_rows(
    df, table_name, key_cols, dwh_engine, fingerprint_cols=None
):
    """
    Insert only new dimension members into the target table. Row-level `row_fp`
    are computed from business columns at load time.
    """
    # 1. Remove duplicates within the incoming dataframe itself
    df = df.drop_duplicates(subset=key_cols).copy()

    # helps retrieve detailed metadata about a database engine
    inspector = inspect(dwh_engine)

    fp_cols = fingerprint_cols or sorted(
        c for c in df.columns if c != fingerprint_column
    )
    df = add_row_fingerprints(df, cols_to_hash=fp_cols)

    # If table does not exist, create it and load all rows
    if not inspector.has_table(table_name):
        df.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False,
        )
        print(f"{table_name} created and {len(df)} rows inserted, with row_fp.")
        return len(df)

    ensure_fingerprint_column_exists(table_name, dwh_engine)

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
            index=False,
        )
        print(f"{len(df_new)} new rows inserted into {table_name}, with row_fp.")
        return len(df_new)
    else:
        print(f"No new rows to insert into {table_name}.")
        return 0


def load_new_fact_rows(df, table_name, key_cols, dwh_engine, fingerprint_cols=None):
    """
    Insert only new fact rows. Row-level `row_fp` is set at load time from
    fact business columns.
    Since each customer appears once in this dataset,
    customer_id can be used as the business key.
    """
    df = df.drop_duplicates(subset=key_cols).copy()
    inspector = inspect(dwh_engine)

    fp_cols = fingerprint_cols or sorted(
        c for c in df.columns if c != fingerprint_column
    )  # sorted for stability
    df = add_row_fingerprints(df, cols_to_hash=fp_cols)

    if not inspector.has_table(table_name):
        df.to_sql(
            name=table_name,
            con=dwh_engine,
            if_exists="append",
            index=False,
        )
        print(f"{table_name} created and {len(df)} rows inserted, with row_fp.")
        return len(df)

    ensure_fingerprint_column_exists(table_name, dwh_engine)

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
            index=False,
        )
        print(f"{len(df_new)} new rows inserted into {table_name}, with row_fp.")
        return len(df_new)
    else:
        print(f"No new rows to insert into {table_name}.")
        return 0


# ---------------------------------------------------------
# DATA LINEAGE
# ---------------------------------------------------------

LINEAGE_TABLE = "data_lineage_log"


def ensure_lineage_table_exists(dwh_engine):
    """
    Create the lineage table if it does not already exist.
    This table stores metadata about how each warehouse table was created. Can trace where data came from, what transformed it,
    how many rows moved, and whether it succeeded — essentially giving you full observability over your data pipeline.
    """
    create_sql = text(f"""
    CREATE TABLE IF NOT EXISTS {LINEAGE_TABLE} (
        lineage_id INT AUTO_INCREMENT PRIMARY KEY,
        etl_run_id VARCHAR(100),
        dag_id VARCHAR(100),
        task_id VARCHAR(100),
        source_name VARCHAR(255),
        source_type VARCHAR(50),
        target_table VARCHAR(100),
        transformation_name VARCHAR(255),
        period INT,
        period_start_date DATE,
        period_end_date DATE,
        input_row_count INT,
        output_row_count INT,
        rows_inserted INT,
        status VARCHAR(50),
        log_timestamp DATETIME
    )
    """)
    with dwh_engine.begin() as conn:
        conn.execute(create_sql)


def log_lineage(
    dwh_engine,
    etl_run_id,
    dag_id,
    task_id,
    source_name,
    source_type,
    target_table,
    transformation_name,
    period,
    period_start_date,
    period_end_date,
    input_row_count,
    output_row_count,
    rows_inserted,
    status,
):
    """
    Insert one lineage record into the lineage table.
    """
    ensure_lineage_table_exists(dwh_engine)

    insert_sql = text(f"""
    INSERT INTO {LINEAGE_TABLE} (
        etl_run_id,
        dag_id,
        task_id,
        source_name,
        source_type,
        target_table,
        transformation_name,
        period,
        period_start_date,
        period_end_date,
        input_row_count,
        output_row_count,
        rows_inserted,
        status,
        log_timestamp
    )
    VALUES (
        :etl_run_id,
        :dag_id,
        :task_id,
        :source_name,
        :source_type,
        :target_table,
        :transformation_name,
        :period,
        :period_start_date,
        :period_end_date,
        :input_row_count,
        :output_row_count,
        :rows_inserted,
        :status,
        :log_timestamp
    )
    """)

    with dwh_engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "etl_run_id": etl_run_id,
                "dag_id": dag_id,
                "task_id": task_id,
                "source_name": source_name,
                "source_type": source_type,
                "target_table": target_table,
                "transformation_name": transformation_name,
                "period": period,
                "period_start_date": period_start_date,
                "period_end_date": period_end_date,
                "input_row_count": input_row_count,
                "output_row_count": output_row_count,
                "rows_inserted": rows_inserted,
                "status": status,
                "log_timestamp": datetime.now(),
            },
        )
