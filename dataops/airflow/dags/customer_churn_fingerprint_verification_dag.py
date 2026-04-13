import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

from customer_churn_etl_functions import DATAWAREHOUSE_DB
from customer_churn_fingerprint_verification_functions import (
    TABLES_FINGERPRINT_AUDIT,
    verify_fingerprints_for_table,
)



with DAG(
    dag_id="customer_churn_fingerprint_verification",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bt4301", "customer_churn", "watermark", "verification"],
) as dag:

    @task(task_id="verify_all_tables")
    def verify_all_tables():
        """
        For each table, re-hash business columns and compare to `row_fp`.
        """
        engine = create_engine(DATAWAREHOUSE_DB, echo=False)

        all_summaries = []
        failed = []

        for table in sorted(TABLES_FINGERPRINT_AUDIT):
            ok, summary = verify_fingerprints_for_table(engine, table)
            all_summaries.append(summary)
            print(f"[fingerprint] {summary}")
            if not ok:
                failed.append(summary)
                logger.warning(
                    "Fingerprint mismatch: table=%s summary=%s",
                    table,
                    summary,
                )
                print(f"[fingerprint][WARNING] table={table} mismatch: {summary}")

        if failed:
            logger.warning(
                "Fingerprint verification completed with %s table(s) failing check - "
                "stored `row_fp` does not match recomputed. Details: %s",
                len(failed),
                failed,
            )
            print(
                f"[fingerprint][WARNING] {len(failed)} table(s) with mismatches; "
                f"Details: {failed}"
            )

        return all_summaries

    verify_all_tables()
