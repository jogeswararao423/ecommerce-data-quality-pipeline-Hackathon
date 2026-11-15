import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
import sqlite3

from src.config import (
    LOGS_DIR,
    OUTPUT_DIR,
    REPORTS_DIR,
    DEFAULT_LOG_PATH,
    QUALITY_REPORT_PATH,
    CLEAN_ORDERS_CSV,
    REJECTED_RECORDS_CSV,
    ALLOWED_ORDER_STATUSES
)


def setup_logger(log_file: Path = DEFAULT_LOG_PATH) -> logging.Logger:
    """Configures structured console and file logging."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ecommerce_pipeline")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers on multiple runs
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File Handler
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Console Handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


class PipelineMonitor:
    """
    Tracks run metrics, generates quality reports, writes clean and rejected CSV outputs,
    and runs SQL reconciliation assertions against SQLite.
    """

    def __init__(self, db_conn_getter):
        self.db_conn_getter = db_conn_getter
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    def export_csv_outputs(
        self,
        clean_orders: List[Dict[str, Any]],
        rejected_records: List[Dict[str, Any]]
    ) -> None:
        """Exports clean orders and rejected records to output CSV files."""
        # 1. Clean Orders
        if clean_orders:
            df_clean = pd.DataFrame(clean_orders)
            df_clean.to_csv(CLEAN_ORDERS_CSV, index=False)
        else:
            pd.DataFrame(columns=[
                "order_id", "customer_id", "order_amount", "order_status",
                "order_date", "updated_at", "is_high_value", "run_id", "loaded_at"
            ]).to_csv(CLEAN_ORDERS_CSV, index=False)

        # 2. Rejected Records
        if rejected_records:
            df_rejects = pd.DataFrame(rejected_records)
            df_rejects.to_csv(REJECTED_RECORDS_CSV, index=False)
        else:
            pd.DataFrame(columns=[
                "run_id", "source_file", "source_row_number",
                "raw_record", "rejection_reason", "rejected_at"
            ]).to_csv(REJECTED_RECORDS_CSV, index=False)

    def run_reconciliation(self, expected_input_count: int = 0) -> Dict[str, Any]:
        """
        Executes comprehensive SQL reconciliation and integrity queries across tables.
        """
        conn = self.db_conn_getter()
        try:
            # 1. Duplicate order IDs in clean_orders
            dup_query = """
            SELECT order_id, COUNT(*) as cnt
            FROM clean_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1;
            """
            duplicates = conn.execute(dup_query).fetchall()

            # 2. Null primary keys in clean_orders
            null_pk_query = "SELECT COUNT(*) FROM clean_orders WHERE order_id IS NULL OR TRIM(order_id) = '';"
            null_pks = conn.execute(null_pk_query).fetchone()[0]

            # 3. Negative or zero order amounts
            invalid_amt_query = "SELECT COUNT(*) FROM clean_orders WHERE order_amount <= 0.0;"
            invalid_amounts = conn.execute(invalid_amt_query).fetchone()[0]

            # 4. Invalid customer foreign keys
            fk_query = """
            SELECT o.order_id, o.customer_id
            FROM clean_orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
            """
            orphans = conn.execute(fk_query).fetchall()

            # 5. Invalid order statuses
            status_placeholders = ",".join(f"'{s}'" for s in ALLOWED_ORDER_STATUSES)
            status_query = f"SELECT COUNT(*) FROM clean_orders WHERE order_status NOT IN ({status_placeholders});"
            invalid_statuses = conn.execute(status_query).fetchone()[0]

            # 6. Aggregate metrics
            agg_query = "SELECT COUNT(*) as total_orders, COALESCE(SUM(order_amount), 0.0) as total_amount FROM clean_orders;"
            agg_row = conn.execute(agg_query).fetchone()
            total_orders = agg_row[0]
            total_amount = round(float(agg_row[1]), 2)

            # 7. Total rejected records in database
            reject_count_query = "SELECT COUNT(*) FROM rejected_records;"
            total_rejects = conn.execute(reject_count_query).fetchone()[0]

            # 8. Rejection reasons breakdown
            breakdown_query = """
            SELECT rejection_reason, COUNT(*) as count
            FROM rejected_records
            GROUP BY rejection_reason
            ORDER BY count DESC;
            """
            breakdown_rows = conn.execute(breakdown_query).fetchall()
            rejection_breakdown = {row[0]: row[1] for row in breakdown_rows}

            checks = [
                {
                    "check": "No duplicate order IDs in clean_orders",
                    "status": "PASSED" if len(duplicates) == 0 else "FAILED",
                    "details": f"{len(duplicates)} duplicates found"
                },
                {
                    "check": "No null primary keys in clean_orders",
                    "status": "PASSED" if null_pks == 0 else "FAILED",
                    "details": f"{null_pks} null primary keys found"
                },
                {
                    "check": "No negative or zero order amounts in clean_orders",
                    "status": "PASSED" if invalid_amounts == 0 else "FAILED",
                    "details": f"{invalid_amounts} non-positive amounts found"
                },
                {
                    "check": "No orphaned customer foreign keys in clean_orders",
                    "status": "PASSED" if len(orphans) == 0 else "FAILED",
                    "details": f"{len(orphans)} orphans found"
                },
                {
                    "check": "No invalid order statuses in clean_orders",
                    "status": "PASSED" if invalid_statuses == 0 else "FAILED",
                    "details": f"{invalid_statuses} invalid statuses found"
                }
            ]

            return {
                "reconciliation_checks": checks,
                "all_passed": all(c["status"] == "PASSED" for c in checks),
                "target_aggregates": {
                    "total_clean_orders": total_orders,
                    "total_order_amount_usd": total_amount,
                    "total_quarantined_records": total_rejects
                },
                "quarantine_breakdown": rejection_breakdown
            }
        finally:
            conn.close()

    def generate_quality_report(
        self,
        run_metrics: Dict[str, Any],
        reconciliation: Dict[str, Any],
        output_file: Path = QUALITY_REPORT_PATH
    ) -> Dict[str, Any]:
        """Compiles and saves a comprehensive structured JSON quality report in both reports/ and output/."""
        report = {
            "report_generated_at": datetime.now(timezone.utc).isoformat(),
            "execution_metrics": run_metrics,
            "reconciliation": reconciliation
        }
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        # Mirror report to output/quality_report.json to satisfy Section B.5 and Section F requirements
        from src.config import OUTPUT_QUALITY_REPORT_PATH
        OUTPUT_QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_QUALITY_REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report
