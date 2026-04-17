import pandas as pd
from sqlalchemy import text

from customer_churn_etl_functions import (
    add_row_fingerprints,
    columns_for_fingerprint,
    fingerprint_column,
)

# Tables that participate in row_fp verification
TABLES_FINGERPRINT_AUDIT = frozenset(
    {
        "dim_customer",
        "dim_account",
        "dim_service",
        "dim_behavior",
        "fact_customer_churn",
    }
)


def recompute_row_fingerprints_from_db(df):
    """
    Recompute `row_fp` for a dataframe as loaded from MySQL.
    Expects the same business columns as at ETL; drops/compares existing `row_fp`.
    """
    df = df.copy()
    if fingerprint_column in df.columns:
        df = df.drop(columns=[fingerprint_column])
    cols = columns_for_fingerprint(df)
    return add_row_fingerprints(df, cols_to_hash=cols)


def verify_row_fingerprints(df_loaded, key_col="customer_id"):
    """
    Compare stored `row_fp` on rows from MySQL with recomputed `row_fp`.
    Returns (all_match: bool, mismatch_df: DataFrame of key + row_fp + row_fp_expected).
    """
    if df_loaded.empty:
        return True, pd.DataFrame()
    if fingerprint_column not in df_loaded.columns:
        raise ValueError(f"Loaded data must include {fingerprint_column!r}")

    recalc = recompute_row_fingerprints_from_db(
        df_loaded.drop(columns=[fingerprint_column])
    )
    merged = df_loaded[[key_col, fingerprint_column]].merge(
        recalc[[key_col, fingerprint_column]].rename(
            columns={fingerprint_column: "row_fp_expected"}
        ),
        on=key_col,
        how="left",
    )
    mismatches = merged[merged[fingerprint_column] != merged["row_fp_expected"]]
    return mismatches.empty, mismatches


def read_table_for_fingerprint_audit(dwh_engine, table_name: str):
    """
    Load full warehouse tables for row fingerprint verification.
    """
    if table_name not in TABLES_FINGERPRINT_AUDIT:
        raise ValueError(f"Table not allowed for audit: {table_name!r}")

    return pd.read_sql(text(f"SELECT * FROM `{table_name}`"), con=dwh_engine)


def verify_fingerprints_for_table(dwh_engine, table_name: str):
    """
    Recompute row fingerprints from current DB values and compare to stored `row_fp`.

    Rows with NULL `row_fp` are skipped.

    Returns (ok: bool, summary: dict).
    """
    df = read_table_for_fingerprint_audit(dwh_engine, table_name)
    if df.empty:
        return True, {
            "table": table_name,
            "rows_checked": 0,
            "note": "table empty",
        }

    if fingerprint_column not in df.columns:
        raise ValueError(f"{table_name} has no {fingerprint_column!r} column")

    df_ok = df.loc[df[fingerprint_column].notna()].copy()
    skipped = len(df) - len(df_ok)

    if df_ok.empty:
        return True, {
            "table": table_name,
            "rows_checked": 0,
            "rows_skipped_null_fp": skipped,
            "note": "no rows with stored fingerprint",
        }

    match, mismatches = verify_row_fingerprints(df_ok, key_col="customer_id")
    summary = {
        "table": table_name,
        "rows_checked": len(df_ok),
        "rows_skipped_null_fp": skipped,
        "mismatch_count": int(len(mismatches)) if not match else 0,
        "match": match,
    }
    if not match and not mismatches.empty:
        summary["mismatch_customer_ids"] = mismatches["customer_id"].tolist()
    return match, summary
