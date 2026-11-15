import pytest
import pandas as pd
from datetime import datetime, timezone

from src.validation import DataValidator


@pytest.fixture
def validator():
    # Customer created on 2025-01-01
    customer_map = {"CUST_0001": "2025-01-01T10:00:00+00:00"}
    return DataValidator(valid_customer_map=customer_map)


def test_validation_happy_path(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_001",
        "customer_id": "CUST_0001",
        "order_amount": "150.50",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 1
    assert len(rejects) == 0
    assert dups == 0
    assert valid[0]["order_amount"] == 150.50


def test_validation_missing_primary_key(validator):
    df = pd.DataFrame([{
        "order_id": "",
        "customer_id": "CUST_0001",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "MISSING_PRIMARY_KEY" in rejects[0]["rejection_reason"]


def test_validation_missing_customer_id(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_002",
        "customer_id": "",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "MISSING_CUSTOMER_ID" in rejects[0]["rejection_reason"]


def test_validation_negative_order_amount(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_003",
        "customer_id": "CUST_0001",
        "order_amount": "-50.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "NEGATIVE_ORDER_AMOUNT" in rejects[0]["rejection_reason"]


def test_validation_zero_order_amount(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_004",
        "customer_id": "CUST_0001",
        "order_amount": "0.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "ZERO_ORDER_AMOUNT" in rejects[0]["rejection_reason"]


def test_validation_malformed_order_amount(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_005",
        "customer_id": "CUST_0001",
        "order_amount": "not_a_number",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "MALFORMED_ORDER_AMOUNT" in rejects[0]["rejection_reason"]


def test_validation_invalid_order_status(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_006",
        "customer_id": "CUST_0001",
        "order_amount": "100.00",
        "order_status": "REFUNDED",  # Not in allowed statuses
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "INVALID_ORDER_STATUS" in rejects[0]["rejection_reason"]


def test_validation_invalid_customer_reference(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_007",
        "customer_id": "CUST_NONEXISTENT",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "INVALID_CUSTOMER_REFERENCE" in rejects[0]["rejection_reason"]


def test_validation_order_before_customer_creation(validator):
    # Customer was created on 2025-01-01, but order is dated 2024-05-01
    df = pd.DataFrame([{
        "order_id": "ORD_008",
        "customer_id": "CUST_0001",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2024-05-01T10:00:00+00:00",
        "updated_at": "2024-05-01T10:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "ORDER_BEFORE_CUSTOMER_CREATION" in rejects[0]["rejection_reason"]


def test_validation_invalid_timestamp(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_009_TIME",
        "customer_id": "CUST_0001",
        "order_amount": "100.00",
        "order_status": "PLACED",
        "order_date": "2026-99-99T99:99:99Z",  # Unparseable timestamp
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "MALFORMED_TIMESTAMP" in rejects[0]["rejection_reason"]


def test_validation_boundary_minimum_amount(validator):
    df = pd.DataFrame([{
        "order_id": "ORD_010_MIN",
        "customer_id": "CUST_0001",
        "order_amount": "0.01",  # Exact boundary minimum valid
        "order_status": "PLACED",
        "order_date": "2026-06-01T12:00:00+00:00",
        "updated_at": "2026-06-01T12:00:00+00:00",
        "_source_row_number": 2
    }])
    valid, rejects, dups = validator.validate_orders(df, "test.csv", "run_1")
    assert len(valid) == 1
    assert len(rejects) == 0
    assert valid[0]["order_amount"] == 0.01


def test_validation_customer_email_format(validator):
    df = pd.DataFrame([
        {
            "customer_id": "CUST_BAD",
            "customer_name": "Bad User",
            "email": "invalid_email_format",
            "city": "Seattle",
            "created_at": "2025-01-01T00:00:00Z"
        }
    ])
    valid, rejects = validator.validate_customers(df, "customers.csv", "run_1")
    assert len(valid) == 0
    assert len(rejects) == 1
    assert "INVALID_EMAIL_FORMAT" in rejects[0]["rejection_reason"]
