import logging
import time
from pathlib import Path

import mysql.connector
import numpy as np
import pandas as pd

_THIS_DIR = Path(__file__).resolve().parent
# base_dir: project root
BASE_DIR = _THIS_DIR.parent if _THIS_DIR.name == "dataops" else _THIS_DIR.parent.parent.parent
DATA_PATH = BASE_DIR / "data"
DEFAULT_STAGING_DIR = BASE_DIR / "dataops" / "airflow" / "staging"

logger = logging.getLogger(__name__)

TABLE_CONFIG = {
    "application_train.csv": ("application_train", "application_train"),
    "application_test.csv": ("application_test", "application_test"),
    "bureau.csv": ("bureau", "bureau"),
    "bureau_balance.csv": ("bureau_balance", "bureau_balance"),
}

AMT_CAP_COLS = ["AMT_INCOME_TOTAL", "AMT_CREDIT", "AMT_ANNUITY"]
VALID_STATUS_BUREAU_BALANCE = ["C", "X", "0", "1", "2", "3", "4", "5"]


def get_default_mysql_config():
    return {
        "host": "localhost",
        "user": "bt4301",
        "password": "password",
        "database": "home_credit",
        "port": 3306,
    }


def extract(
    data_path: Path = None,
    staging_dir: Path = None,
) -> dict:
    data_path = data_path or DATA_PATH
    staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
    raw_dir = staging_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    result = {}
    for csv_name, (stage_name, _) in TABLE_CONFIG.items():
        csv_path = data_path / csv_name
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}. Place datasets in {data_path}")
        logger.info("Extracting %s from %s", csv_name, csv_path)
        df = pd.read_csv(csv_path)
        out_path = raw_dir / f"{stage_name}.csv"
        df.to_csv(out_path, index=False)
        result[stage_name] = str(out_path)
        logger.info("Wrote %s rows to %s", len(df), out_path)

    return result


def transform(staging_dir: Path = None) -> dict:
    staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
    raw_dir = staging_dir / "raw"
    out_dir = staging_dir / "transformed"
    out_dir.mkdir(parents=True, exist_ok=True)

    result = {}

    # --- application_train ---
    path_train = raw_dir / "application_train.csv"
    if not path_train.exists():
        raise FileNotFoundError(f"Staging file not found: {path_train}. Run extract first.")
    app_clean = pd.read_csv(path_train)

    if "CODE_GENDER" in app_clean.columns:
        app_clean["CODE_GENDER"] = app_clean["CODE_GENDER"].fillna("XNA")
    if "DAYS_EMPLOYED" in app_clean.columns:
        app_clean["FLAG_UNEMPLOYED"] = (app_clean["DAYS_EMPLOYED"] == 365243).astype(int)
        app_clean["DAYS_EMPLOYED"] = app_clean["DAYS_EMPLOYED"].replace(365243, 0)

    AMT_Q99_CAPS = {}
    for col in AMT_CAP_COLS:
        if col in app_clean.columns and app_clean[col].notna().any():
            AMT_Q99_CAPS[col] = app_clean[col].quantile(0.99)
            app_clean[col] = app_clean[col].clip(upper=AMT_Q99_CAPS[col])

    before_dedup = len(app_clean)
    app_clean = app_clean.drop_duplicates(subset=["SK_ID_CURR"], keep="first")
    if len(app_clean) < before_dedup:
        logger.warning("Dropped %s duplicate SK_ID_CURR in application_train", before_dedup - len(app_clean))

    num_cols = [
        c for c in app_clean.select_dtypes(include=[np.number]).columns
        if c != "TARGET" and app_clean[c].isna().any()
    ]
    app_clean = app_clean.fillna({c: 0 for c in num_cols})

    app_clean.to_csv(out_dir / "application_train.csv", index=False)
    result["application_train"] = str(out_dir / "application_train.csv")
    logger.info("Transformed application_train: %s rows", len(app_clean))

    # --- application_test (use train caps) ---
    path_test = raw_dir / "application_test.csv"
    if not path_test.exists():
        raise FileNotFoundError(f"Staging file not found: {path_test}. Run extract first.")
    app_test_clean = pd.read_csv(path_test)

    if "CODE_GENDER" in app_test_clean.columns:
        app_test_clean["CODE_GENDER"] = app_test_clean["CODE_GENDER"].fillna("XNA")
    if "DAYS_EMPLOYED" in app_test_clean.columns:
        app_test_clean["FLAG_UNEMPLOYED"] = (app_test_clean["DAYS_EMPLOYED"] == 365243).astype(int)
        app_test_clean["DAYS_EMPLOYED"] = app_test_clean["DAYS_EMPLOYED"].replace(365243, 0)
    for col, cap in AMT_Q99_CAPS.items():
        if col in app_test_clean.columns:
            app_test_clean[col] = app_test_clean[col].clip(upper=cap)
    app_test_clean = app_test_clean.drop_duplicates(subset=["SK_ID_CURR"], keep="first")
    num_cols_test = [c for c in app_test_clean.select_dtypes(include=[np.number]).columns if app_test_clean[c].isna().any()]
    app_test_clean = app_test_clean.fillna({c: 0 for c in num_cols_test})

    app_test_clean.to_csv(out_dir / "application_test.csv", index=False)
    result["application_test"] = str(out_dir / "application_test.csv")
    logger.info("Transformed application_test: %s rows", len(app_test_clean))

    # --- bureau ---
    path_bureau = raw_dir / "bureau.csv"
    if not path_bureau.exists():
        raise FileNotFoundError(f"Staging file not found: {path_bureau}. Run extract first.")
    bureau_clean = pd.read_csv(path_bureau).fillna(0)
    bureau_clean.to_csv(out_dir / "bureau.csv", index=False)
    result["bureau"] = str(out_dir / "bureau.csv")
    logger.info("Transformed bureau: %s rows", len(bureau_clean))

    # --- bureau_balance ---
    path_bb = raw_dir / "bureau_balance.csv"
    if not path_bb.exists():
        raise FileNotFoundError(f"Staging file not found: {path_bb}. Run extract first.")
    bureau_balance_clean = pd.read_csv(path_bb)
    if "STATUS" in bureau_balance_clean.columns:
        bureau_balance_clean["STATUS"] = bureau_balance_clean["STATUS"].where(
            bureau_balance_clean["STATUS"].isin(VALID_STATUS_BUREAU_BALANCE), "X"
        )
    bureau_balance_clean.to_csv(out_dir / "bureau_balance.csv", index=False)
    result["bureau_balance"] = str(out_dir / "bureau_balance.csv")
    logger.info("Transformed bureau_balance: %s rows", len(bureau_balance_clean))

    return result


def _pd_to_mysql_type(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(dtype):
        return "TINYINT(1)"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    return "TEXT"


def _row_to_tuple(row) -> tuple:
    out = []
    for v in row:
        if pd.isna(v) or (isinstance(v, float) and np.isinf(v)):
            out.append(None)
        elif isinstance(v, (np.integer, np.int64, np.int32)):
            out.append(int(v))
        elif isinstance(v, (np.floating, np.float64, np.float32)):
            out.append(float(v))
        elif isinstance(v, bool):
            out.append(1 if v else 0)
        else:
            out.append(v)
    return tuple(out)


def load(
    staging_dir: Path = None,
    mysql_config: dict = None,
    batch_size: int = 10_000,
    batch_delay_seconds: float = 0.5,
) -> None:
    staging_dir = Path(staging_dir or DEFAULT_STAGING_DIR)
    out_dir = staging_dir / "transformed"
    mysql_config = mysql_config or get_default_mysql_config()

    conn = mysql.connector.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        port=mysql_config["port"],
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_config['database']}`;")
    conn.commit()
    cur.close()
    conn.close()

    conn = mysql.connector.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        port=mysql_config["port"],
    )
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS bureau_balance;")
        cur.execute("DROP TABLE IF EXISTS bureau;")
        cur.execute("DROP TABLE IF EXISTS application_test;")
        cur.execute("DROP TABLE IF EXISTS application_train;")
        conn.commit()

        tables = ["application_train", "application_test", "bureau", "bureau_balance"]
        for table in tables:
            path = out_dir / f"{table}.csv"
            if not path.exists():
                raise FileNotFoundError(f"Transformed file not found: {path}. Run transform first.")
            logger.info("Loading %s into MySQL (batch_size=%s, delay=%.1fs)", table, batch_size, batch_delay_seconds)
            df = pd.read_csv(path)
            n_rows = len(df)
            cols = list(df.columns)

            defs = [f"`{c}` {_pd_to_mysql_type(df[c].dtype)}" for c in cols]
            cur.execute(f"CREATE TABLE `{table}` ({', '.join(defs)});")
            conn.commit()

            placeholders = ", ".join(["%s"] * len(cols))
            col_list = ", ".join(f"`{c}`" for c in cols)
            insert_sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"

            for i in range(0, n_rows, batch_size):
                chunk = df.iloc[i : i + batch_size]
                rows = [_row_to_tuple(tuple(r)) for r in chunk.itertuples(index=False)]
                if rows:
                    cur.executemany(insert_sql, rows)
                conn.commit()
                chunk_num = i // batch_size + 1
                n_chunks = (n_rows + batch_size - 1) // batch_size
                rows_this_batch = len(rows)
                rows_so_far = min(i + batch_size, n_rows)
                logger.info(
                    "%s: batch %s/%s ingested %s rows (total so far: %s/%s)",
                    table, chunk_num, n_chunks, rows_this_batch, rows_so_far, n_rows,
                )
                if chunk_num < n_chunks:
                    time.sleep(batch_delay_seconds)
            logger.info("Loaded %s: %s rows complete", table, n_rows)

        logger.info("Loaded application_train, application_test, bureau, bureau_balance into MySQL.")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
