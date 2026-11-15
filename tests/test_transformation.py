import pytest
from src.transformation import DataTransformer
from src.config import HIGH_VALUE_THRESHOLD


def test_transform_high_value_flag():
    valid_orders = [
        {
            "order_id": "ORD_1",
            "customer_id": "CUST_1",
            "order_amount": 500.00,
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        },
        {
            "order_id": "ORD_2",
            "customer_id": "CUST_1",
            "order_amount": 2500.00,  # >= HIGH_VALUE_THRESHOLD
            "order_status": "PLACED",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        }
    ]
    transformed = DataTransformer.transform_orders(valid_orders, run_id="run_test")

    assert len(transformed) == 2
    assert transformed[0]["is_high_value"] == 0
    assert transformed[1]["is_high_value"] == 1
    assert transformed[0]["run_id"] == "run_test"
    assert "loaded_at" in transformed[0]
    assert transformed[0]["order_status"] == "PLACED"


def test_transform_rounding():
    valid_orders = [
        {
            "order_id": "ORD_3",
            "customer_id": "CUST_1",
            "order_amount": 12.3456,
            "order_status": "placed",
            "order_date": "2026-06-01T10:00:00Z",
            "updated_at": "2026-06-01T10:00:00Z"
        }
    ]
    transformed = DataTransformer.transform_orders(valid_orders, run_id="run_test")
    assert transformed[0]["order_amount"] == 12.35
    assert transformed[0]["order_status"] == "PLACED"
