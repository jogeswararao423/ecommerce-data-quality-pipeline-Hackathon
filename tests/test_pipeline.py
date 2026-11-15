import pytest
import sqlite3
from pathlib import Path
import pandas as pd

from src.pipeline import OrderPipeline
from src.database import DatabaseManager


@pytest.fixture
def test_pipeline(tmp_path):
    test_db = tmp_path / "test_ecommerce.db"
    return OrderPipeline(db_path=test_db), test_db, tmp_path


def test_pipeline_happy_path(test_pipeline):
    pipeline, db_path, tmp_path = test_pipeline
    cust_file = tmp_path / "customers.csv"
    orders_file = tmp_path / "orders.csv"

    # Create 1 customer
    pd.DataFrame([{
        "customer_id": "CUST_TEST_1",
        "customer_name": "Test User",
        "email": "test@example.com",
        "city": "Chicago",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    # Create 2 valid orders
    pd.DataFrame([
        {
            "order_id": "ORD_TEST_1",
            "customer_id": "CUST_TEST_1",
            "order_amount": "250.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        },
        {
            "order_id": "ORD_TEST_2",
            "customer_id": "CUST_TEST_1",
            "order_amount": "1800.00",  # High value
            "order_status": "DELIVERED",
            "order_date": "2026-06-01T11:00:00Z",
            "updated_at": "2026-06-01T11:00:00Z"
        }
    ]).to_csv(orders_file, index=False)

    metrics = pipeline.run(orders_file=orders_file, customers_file=cust_file)

    assert metrics["pipeline_status"] == "SUCCESS"
    assert metrics["input_count"] == 2
    assert metrics["valid_count"] == 2
    assert metrics["rejected_count"] == 0
    assert metrics["output_count"] == 2

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("SELECT order_id, is_high_value FROM clean_orders ORDER BY order_id").fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0] == ("ORD_TEST_1", 0)
    assert rows[1] == ("ORD_TEST_2", 1)


def test_pipeline_empty_orders_file(test_pipeline):
    pipeline, db_path, tmp_path = test_pipeline
    cust_file = tmp_path / "customers.csv"
    empty_file = tmp_path / "empty_orders.csv"

    pd.DataFrame([{
        "customer_id": "CUST_TEST_1",
        "customer_name": "Test User",
        "email": "test@example.com",
        "city": "Chicago",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    pd.DataFrame(columns=["order_id", "customer_id", "order_amount", "order_status", "order_date", "updated_at"]).to_csv(empty_file, index=False)

    metrics = pipeline.run(orders_file=empty_file, customers_file=cust_file)

    assert metrics["pipeline_status"] == "SUCCESS"
    assert metrics["input_count"] == 0
    assert metrics["output_count"] == 0


def test_pipeline_partial_failure(test_pipeline):
    pipeline, db_path, tmp_path = test_pipeline
    cust_file = tmp_path / "customers.csv"
    orders_file = tmp_path / "partial_orders.csv"

    pd.DataFrame([{
        "customer_id": "CUST_TEST_1",
        "customer_name": "Test User",
        "email": "test@example.com",
        "city": "Chicago",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    # 1 valid, 1 negative amount
    pd.DataFrame([
        {
            "order_id": "ORD_VALID",
            "customer_id": "CUST_TEST_1",
            "order_amount": "100.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        },
        {
            "order_id": "ORD_INVALID",
            "customer_id": "CUST_TEST_1",
            "order_amount": "-25.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        }
    ]).to_csv(orders_file, index=False)

    metrics = pipeline.run(orders_file=orders_file, customers_file=cust_file)

    assert metrics["pipeline_status"] == "PARTIAL_SUCCESS"
    assert metrics["input_count"] == 2
    assert metrics["valid_count"] == 1
    assert metrics["rejected_count"] == 1
    assert metrics["output_count"] == 1


def test_pipeline_corrupted_schema(test_pipeline):
    pipeline, db_path, tmp_path = test_pipeline
    cust_file = tmp_path / "customers.csv"
    corrupt_file = tmp_path / "corrupted_schema.csv"

    pd.DataFrame([{
        "customer_id": "CUST_TEST_1",
        "customer_name": "Test User",
        "email": "test@example.com",
        "city": "Chicago",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    # Missing customer_id, order_status, order_date, etc.
    pd.DataFrame([
        {"wrong_column": "data"}
    ]).to_csv(corrupt_file, index=False)

    metrics = pipeline.run(orders_file=corrupt_file, customers_file=cust_file)

    assert metrics["pipeline_status"] == "FAILED"
    assert metrics["valid_count"] == 0
    assert metrics["rejected_count"] == 1


def test_pipeline_late_arriving_records(test_pipeline):
    """Verifies that late-arriving valid historical orders are successfully processed."""
    pipeline, db_path, tmp_path = test_pipeline
    cust_file = tmp_path / "customers.csv"
    orders_file = tmp_path / "late_orders.csv"

    pd.DataFrame([{
        "customer_id": "CUST_TEST_1",
        "customer_name": "Test User",
        "email": "test@example.com",
        "city": "Chicago",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    # Order occurred 90 days ago (late-arriving valid order)
    pd.DataFrame([{
        "order_id": "ORD_LATE_1",
        "customer_id": "CUST_TEST_1",
        "order_amount": "450.00",
        "order_status": "DELIVERED",
        "order_date": "2025-06-01T10:00:00Z",
        "updated_at": "2025-06-01T10:00:00Z"
    }]).to_csv(orders_file, index=False)

    metrics = pipeline.run(orders_file=orders_file, customers_file=cust_file)

    assert metrics["pipeline_status"] == "SUCCESS"
    assert metrics["valid_count"] == 1
    assert metrics["output_count"] == 1

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT order_id, order_amount FROM clean_orders WHERE order_id = 'ORD_LATE_1'").fetchone()
    conn.close()

    assert row is not None
    assert row[1] == 450.00


def test_database_transaction_rollback(tmp_path):
    """Verifies that an error during transaction automatically rolls back changes."""
    test_db = tmp_path / "rollback_test.db"
    db = DatabaseManager(db_path=test_db)

    # Insert a valid customer
    db.load_customers([{
        "customer_id": "CUST_RB",
        "customer_name": "Rollback User",
        "email": "rb@example.com",
        "city": "Dallas",
        "created_at": "2025-01-01T00:00:00Z"
    }])

    # Attempt a transaction that raises an exception halfway through
    with pytest.raises(RuntimeError):
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO clean_orders (order_id, customer_id, order_amount, order_status, order_date, updated_at, is_high_value, run_id, loaded_at) "
                "VALUES ('ORD_SHOULD_ROLLBACK', 'CUST_RB', 50.0, 'PLACED', '2026-06-01', '2026-06-01', 0, 'r1', 'now');"
            )
            raise RuntimeError("Simulated mid-transaction failure")

    # Confirm the record was NOT committed
    conn = db.get_connection()
    count = conn.execute("SELECT COUNT(*) FROM clean_orders WHERE order_id = 'ORD_SHOULD_ROLLBACK';").fetchone()[0]
    conn.close()
    assert count == 0
