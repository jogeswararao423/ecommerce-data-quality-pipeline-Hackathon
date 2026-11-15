import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager

from src.config import DEFAULT_DB_PATH

logger = logging.getLogger("ecommerce_pipeline.database")


class DatabaseManager:
    """
    Manages SQLite connections, transactions, table schemas,
    idempotent upserts, and order history preservation.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        if self.db_path.parent != Path(":memory:"):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize_schema()

    def get_connection(self) -> sqlite3.Connection:
        """Returns a configured SQLite connection with foreign keys enabled."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    @contextmanager
    def transaction(self):
        """Context manager providing an atomic transaction with automatic rollback on error."""
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction failed and was rolled back: {e}")
            raise
        finally:
            conn.close()

    def initialize_schema(self) -> None:
        """Creates required tables and indexes if they do not exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT NOT NULL,
            city TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS clean_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_amount REAL NOT NULL,
            order_status TEXT NOT NULL,
            order_date TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_high_value INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        );

        CREATE TABLE IF NOT EXISTS rejected_records (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_row_number INTEGER,
            raw_record TEXT NOT NULL,
            rejection_reason TEXT NOT NULL,
            rejected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            start_time TEXT NOT NULL,
            end_time TEXT,
            execution_duration REAL,
            pipeline_status TEXT NOT NULL,
            input_count INTEGER DEFAULT 0,
            valid_count INTEGER DEFAULT 0,
            rejected_count INTEGER DEFAULT 0,
            duplicate_count INTEGER DEFAULT 0,
            output_count INTEGER DEFAULT 0,
            failure_reason TEXT
        );

        CREATE TABLE IF NOT EXISTS order_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            order_amount REAL NOT NULL,
            order_status TEXT NOT NULL,
            order_date TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            run_id TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_clean_orders_customer ON clean_orders (customer_id);
        CREATE INDEX IF NOT EXISTS idx_order_history_order ON order_history (order_id);
        CREATE INDEX IF NOT EXISTS idx_rejected_run ON rejected_records (run_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (pipeline_status);
        """
        with self.transaction() as conn:
            conn.executescript(ddl)
        logger.info(f"Database schema initialized successfully at {self.db_path}")

    def load_customers(self, customer_records: List[Dict[str, Any]]) -> int:
        """Idempotently loads customers into the customers table."""
        if not customer_records:
            return 0
        sql = """
        INSERT INTO customers (customer_id, customer_name, email, city, created_at)
        VALUES (:customer_id, :customer_name, :email, :city, :created_at)
        ON CONFLICT(customer_id) DO UPDATE SET
            customer_name = excluded.customer_name,
            email = excluded.email,
            city = excluded.city,
            created_at = excluded.created_at;
        """
        with self.transaction() as conn:
            conn.executemany(sql, customer_records)
        return len(customer_records)

    def load_clean_orders(self, orders: List[Dict[str, Any]]) -> int:
        """
        Idempotently inserts or updates clean_orders and records history.
        Uses SQLite transactions for ACID compliance.
        """
        if not orders:
            return 0

        upsert_sql = """
        INSERT INTO clean_orders (
            order_id, customer_id, order_amount, order_status,
            order_date, updated_at, is_high_value, run_id, loaded_at
        ) VALUES (
            :order_id, :customer_id, :order_amount, :order_status,
            :order_date, :updated_at, :is_high_value, :run_id, :loaded_at
        )
        ON CONFLICT(order_id) DO UPDATE SET
            customer_id = excluded.customer_id,
            order_amount = excluded.order_amount,
            order_status = excluded.order_status,
            order_date = excluded.order_date,
            updated_at = excluded.updated_at,
            is_high_value = excluded.is_high_value,
            run_id = excluded.run_id,
            loaded_at = excluded.loaded_at;
        """

        history_sql = """
        INSERT INTO order_history (
            order_id, customer_id, order_amount, order_status,
            order_date, updated_at, run_id, recorded_at
        ) VALUES (
            :order_id, :customer_id, :order_amount, :order_status,
            :order_date, :updated_at, :run_id, :loaded_at
        );
        """

        with self.transaction() as conn:
            conn.executemany(upsert_sql, orders)
            conn.executemany(history_sql, orders)

        return len(orders)

    def load_rejected_records(self, rejects: List[Dict[str, Any]]) -> int:
        """Persists quarantined rejected records with diagnostic metadata."""
        if not rejects:
            return 0
        sql = """
        INSERT INTO rejected_records (
            run_id, source_file, source_row_number, raw_record,
            rejection_reason, rejected_at
        ) VALUES (
            :run_id, :source_file, :source_row_number, :raw_record,
            :rejection_reason, :rejected_at
        );
        """
        with self.transaction() as conn:
            conn.executemany(sql, rejects)
        return len(rejects)

    def record_run_start(self, run_id: str, start_time: str) -> None:
        """Records initial pipeline run execution."""
        sql = """
        INSERT INTO pipeline_runs (run_id, start_time, pipeline_status)
        VALUES (:run_id, :start_time, 'RUNNING');
        """
        with self.transaction() as conn:
            conn.execute(sql, {"run_id": run_id, "start_time": start_time})

    def record_run_end(
        self,
        run_id: str,
        end_time: str,
        execution_duration: float,
        pipeline_status: str,
        input_count: int,
        valid_count: int,
        rejected_count: int,
        duplicate_count: int,
        output_count: int,
        failure_reason: Optional[str] = None
    ) -> None:
        """Finalizes pipeline run execution metrics in pipeline_runs."""
        sql = """
        UPDATE pipeline_runs SET
            end_time = :end_time,
            execution_duration = :execution_duration,
            pipeline_status = :pipeline_status,
            input_count = :input_count,
            valid_count = :valid_count,
            rejected_count = :rejected_count,
            duplicate_count = :duplicate_count,
            output_count = :output_count,
            failure_reason = :failure_reason
        WHERE run_id = :run_id;
        """
        with self.transaction() as conn:
            conn.execute(sql, {
                "run_id": run_id,
                "end_time": end_time,
                "execution_duration": execution_duration,
                "pipeline_status": pipeline_status,
                "input_count": input_count,
                "valid_count": valid_count,
                "rejected_count": rejected_count,
                "duplicate_count": duplicate_count,
                "output_count": output_count,
                "failure_reason": failure_reason
            })

    def get_valid_customer_map(self) -> Dict[str, str]:
        """Returns a mapping of customer_id -> created_at timestamp for validation."""
        sql = "SELECT customer_id, created_at FROM customers;"
        conn = self.get_connection()
        try:
            rows = conn.execute(sql).fetchall()
            return {row["customer_id"]: row["created_at"] for row in rows}
        finally:
            conn.close()
