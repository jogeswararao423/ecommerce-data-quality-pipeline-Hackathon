import pytest
import sqlite3
from pathlib import Path
import pandas as pd

from src.pipeline import OrderPipeline


@pytest.fixture
def test_setup(tmp_path):
    test_db = tmp_path / "idempotency_test.db"
    pipeline = OrderPipeline(db_path=test_db)
    cust_file = tmp_path / "customers.csv"

    pd.DataFrame([{
        "customer_id": "CUST_IDEM",
        "customer_name": "Idem User",
        "email": "idem@example.com",
        "city": "Austin",
        "created_at": "2025-01-01T00:00:00Z"
    }]).to_csv(cust_file, index=False)

    return pipeline, test_db, cust_file, tmp_path


def test_rerun_same_input_is_idempotent(test_setup):
    """Rerunning the same input multiple times must not inflate target counts."""
    pipeline, db_path, cust_file, tmp_path = test_setup
    orders_file = tmp_path / "orders.csv"

    pd.DataFrame([
        {
            "order_id": f"ORD_RERUN_{i}",
            "customer_id": "CUST_IDEM",
            "order_amount": "100.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        }
        for i in range(1, 6)
    ]).to_csv(orders_file, index=False)

    # First run
    m1 = pipeline.run(orders_file=orders_file, customers_file=cust_file, run_id="run_1")
    assert m1["output_count"] == 5

    conn = sqlite3.connect(str(db_path))
    count_1 = conn.execute("SELECT COUNT(*) FROM clean_orders").fetchone()[0]
    conn.close()
    assert count_1 == 5

    # Second run with exact same file
    m2 = pipeline.run(orders_file=orders_file, customers_file=cust_file, run_id="run_2")
    assert m2["output_count"] == 5

    conn = sqlite3.connect(str(db_path))
    count_2 = conn.execute("SELECT COUNT(*) FROM clean_orders").fetchone()[0]
    conn.close()

    # Target table clean_orders row count MUST NOT DOUBLE! Must remain 5!
    assert count_2 == 5


def test_order_lifecycle_update_and_history(test_setup):
    """Updating order status preserves single row in clean_orders while capturing history."""
    pipeline, db_path, cust_file, tmp_path = test_setup
    order_v1 = tmp_path / "order_v1.csv"
    order_v2 = tmp_path / "order_v2.csv"

    # Version 1: Initial order placed
    pd.DataFrame([{
        "order_id": "ORD_LIFECYCLE",
        "customer_id": "CUST_IDEM",
        "order_amount": "300.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T10:00:00Z",
        "updated_at": "2026-06-01T10:00:00Z"
    }]).to_csv(order_v1, index=False)

    pipeline.run(orders_file=order_v1, customers_file=cust_file, run_id="run_v1")

    conn = sqlite3.connect(str(db_path))
    status_1 = conn.execute("SELECT order_status FROM clean_orders WHERE order_id = 'ORD_LIFECYCLE'").fetchone()[0]
    conn.close()
    assert status_1 == "PLACED"

    # Version 2: Status updated to DELIVERED
    pd.DataFrame([{
        "order_id": "ORD_LIFECYCLE",
        "customer_id": "CUST_IDEM",
        "order_amount": "300.00",
        "order_status": "DELIVERED",
        "order_date": "2026-06-01T10:00:00Z",
        "updated_at": "2026-06-02T15:00:00Z"
    }]).to_csv(order_v2, index=False)

    pipeline.run(orders_file=order_v2, customers_file=cust_file, run_id="run_v2")

    conn = sqlite3.connect(str(db_path))
    clean_rows = conn.execute("SELECT order_id, order_status FROM clean_orders WHERE order_id = 'ORD_LIFECYCLE'").fetchall()
    history_rows = conn.execute("SELECT order_status, run_id FROM order_history WHERE order_id = 'ORD_LIFECYCLE' ORDER BY history_id").fetchall()
    conn.close()

    # Still exactly 1 row in clean_orders, with updated status
    assert len(clean_rows) == 1
    assert clean_rows[0][1] == "DELIVERED"

    # History preserves both events
    assert len(history_rows) == 2
    assert history_rows[0][0] == "PLACED"
    assert history_rows[1][0] == "DELIVERED"


def test_retry_after_failure(test_setup):
    """Corrupted batch followed by corrected batch succeeds and maintains consistency."""
    pipeline, db_path, cust_file, tmp_path = test_setup
    bad_batch = tmp_path / "bad.csv"
    good_batch = tmp_path / "good.csv"

    # Bad batch: missing order_id
    pd.DataFrame([{
        "order_id": "",
        "customer_id": "CUST_IDEM",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T10:00:00Z",
        "updated_at": "2026-06-01T10:00:00Z"
    }]).to_csv(bad_batch, index=False)

    m_bad = pipeline.run(orders_file=bad_batch, customers_file=cust_file, run_id="run_fail")
    assert m_bad["pipeline_status"] == "FAILED"

    # Corrected batch
    pd.DataFrame([{
        "order_id": "ORD_FIXED_01",
        "customer_id": "CUST_IDEM",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T10:00:00Z",
        "updated_at": "2026-06-01T10:00:00Z"
    }]).to_csv(good_batch, index=False)

    m_good = pipeline.run(orders_file=good_batch, customers_file=cust_file, run_id="run_retry")
    assert m_good["pipeline_status"] == "SUCCESS"
    assert m_good["output_count"] == 1

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM clean_orders WHERE order_id = 'ORD_FIXED_01'").fetchone()[0]
    conn.close()
    assert count == 1
