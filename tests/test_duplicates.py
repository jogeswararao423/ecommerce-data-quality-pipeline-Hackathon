import pytest
import pandas as pd
from src.validation import DataValidator


@pytest.fixture
def validator():
    customer_map = {"CUST_0001": "2025-01-01T10:00:00+00:00"}
    return DataValidator(valid_customer_map=customer_map)


def test_exact_duplicates_kept_once(validator):
    """Exact duplicate records: keep one copy and record duplicate count."""
    df = pd.DataFrame([
        {
            "order_id": "ORD_EXACT",
            "customer_id": "CUST_0001",
            "order_amount": "100.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T12:00:00+00:00",
            "updated_at": "2026-06-01T12:00:00+00:00",
            "_source_row_number": 2
        },
        {
            "order_id": "ORD_EXACT",
            "customer_id": "CUST_0001",
            "order_amount": "100.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T12:00:00+00:00",
            "updated_at": "2026-06-01T12:00:00+00:00",
            "_source_row_number": 3
        }
    ])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 1
    assert len(rejects) == 0
    assert dups == 1


def test_conflicting_duplicates_quarantined(validator):
    """Conflicting duplicate key: same order_id with differing attributes quarantined."""
    df = pd.DataFrame([
        {
            "order_id": "ORD_CONFLICT",
            "customer_id": "CUST_0001",
            "order_amount": "100.00",
            "order_status": "PLACED",
            "order_date": "2026-06-01T12:00:00+00:00",
            "updated_at": "2026-06-01T12:00:00+00:00",
            "_source_row_number": 2
        },
        {
            "order_id": "ORD_CONFLICT",
            "customer_id": "CUST_0001",
            "order_amount": "999.00",  # Different amount
            "order_status": "CANCELLED",  # Different status
            "order_date": "2026-06-01T12:00:00+00:00",
            "updated_at": "2026-06-01T12:00:00+00:00",
            "_source_row_number": 3
        }
    ])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 1
    assert len(rejects) == 1
    assert dups == 1
    assert "CONFLICTING_DUPLICATE_KEY" in rejects[0]["rejection_reason"]


def test_duplicate_customers_quarantined(validator):
    df = pd.DataFrame([
        {
            "customer_id": "CUST_DUP",
            "customer_name": "Customer 1",
            "email": "cust1@example.com",
            "city": "Dallas",
            "created_at": "2025-01-01T00:00:00Z",
            "_source_row_number": 2
        },
        {
            "customer_id": "CUST_DUP",
            "customer_name": "Customer 2",
            "email": "cust2@example.com",
            "city": "Austin",
            "created_at": "2025-01-02T00:00:00Z",
            "_source_row_number": 3
        }
    ])
    valid, rejects = validator.validate_customers(df, "customers.csv", "run_1")
    assert len(valid) == 1
    assert len(rejects) == 1
    assert "DUPLICATE_CUSTOMER_ID" in rejects[0]["rejection_reason"]
