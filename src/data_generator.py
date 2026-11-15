import csv
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple
import numpy as np

from src.config import (
    RANDOM_SEED,
    RAW_DATA_DIR,
    GENERATED_DATA_DIR,
    ALLOWED_ORDER_STATUSES,
    DEFAULT_CUSTOMERS_FILE,
    DEFAULT_ORDERS_FILE
)


class SyntheticDataGenerator:
    """
    Generates reproducible synthetic customer and order datasets
    with controlled error injection and edge cases using NumPy.
    """

    def __init__(self, seed: int = RANDOM_SEED):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Austin", "Denver"]
        self.statuses = sorted(list(ALLOWED_ORDER_STATUSES))

    def reset_seed(self) -> None:
        """Resets the random number generator to the configured seed."""
        self.rng = np.random.default_rng(self.seed)

    def generate_customers(
        self,
        count: int = 50,
        output_file: Path = DEFAULT_CUSTOMERS_FILE,
        inject_invalid: bool = True
    ) -> List[Dict[str, Any]]:
        """Generates reproducible customer master dataset."""
        self.reset_seed()
        output_file.parent.mkdir(parents=True, exist_ok=True)

        customers: List[Dict[str, Any]] = []
        base_date = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        for i in range(1, count + 1):
            cust_id = f"CUST_{i:04d}"
            name = f"Customer_{i}"
            email = f"user_{i}@example.com"
            city = self.cities[i % len(self.cities)]
            created_at = (base_date + timedelta(days=i)).isoformat()

            customers.append({
                "customer_id": cust_id,
                "customer_name": name,
                "email": email,
                "city": city,
                "created_at": created_at
            })

        if inject_invalid and count >= 5:
            # Inject invalid email format
            customers.append({
                "customer_id": "CUST_BAD_EMAIL",
                "customer_name": "Bad Email User",
                "email": "invalid_email_format_without_at",
                "city": "Boston",
                "created_at": base_date.isoformat()
            })
            # Inject missing required field
            customers.append({
                "customer_id": "CUST_MISSING_CITY",
                "customer_name": "Missing City User",
                "email": "cityless@example.com",
                "city": "",
                "created_at": base_date.isoformat()
            })

        # Save to CSV
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["customer_id", "customer_name", "email", "city", "created_at"])
            writer.writeheader()
            writer.writerows(customers)

        return customers

    def generate_orders(
        self,
        count: int = 100,
        customer_ids: List[str] = None,
        output_file: Path = DEFAULT_ORDERS_FILE,
        inject_edge_cases: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Generates configurable synthetic order records with controlled
        edge cases (duplicates, nulls, invalid references, boundary values).
        """
        self.reset_seed()
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if not customer_ids:
            customer_ids = [f"CUST_{i:04d}" for i in range(1, 21)]

        orders: List[Dict[str, Any]] = []
        base_time = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Standard valid orders
        for i in range(1, count + 1):
            order_id = f"ORD_{i:06d}"
            cust_id = customer_ids[i % len(customer_ids)]
            amount = round(float(self.rng.uniform(10.0, 500.0)), 2)
            status = self.statuses[i % len(self.statuses)]
            order_time = (base_time + timedelta(hours=i * 2)).isoformat()
            updated_time = (base_time + timedelta(hours=i * 2, minutes=15)).isoformat()

            orders.append({
                "order_id": order_id,
                "customer_id": cust_id,
                "order_amount": str(amount),
                "order_status": status,
                "order_date": order_time,
                "updated_at": updated_time
            })

        if inject_edge_cases:
            edge_cases = [
                # 1. Boundary: Exact minimum allowed amount ($0.01 - Valid)
                {
                    "order_id": "ORD_EDGE_001_MIN_VALID",
                    "customer_id": customer_ids[0],
                    "order_amount": "0.01",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 2. Boundary: High-value order ($2500.00 - Valid, flagged for review)
                {
                    "order_id": "ORD_EDGE_002_HIGH_VAL",
                    "customer_id": customer_ids[1],
                    "order_amount": "2500.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 3. Boundary: Zero amount ($0.00 - Invalid)
                {
                    "order_id": "ORD_EDGE_003_ZERO_AMT",
                    "customer_id": customer_ids[2],
                    "order_amount": "0.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 4. Boundary: Negative amount (-$45.00 - Invalid)
                {
                    "order_id": "ORD_EDGE_004_NEG_AMT",
                    "customer_id": customer_ids[3],
                    "order_amount": "-45.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 5. Missing Primary Key (order_id is empty - Invalid)
                {
                    "order_id": "",
                    "customer_id": customer_ids[0],
                    "order_amount": "120.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 6. Missing Customer ID (customer_id is empty - Invalid)
                {
                    "order_id": "ORD_EDGE_006_NO_CUST",
                    "customer_id": "",
                    "order_amount": "80.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 7. Referential Integrity Violation (Unknown customer - Invalid)
                {
                    "order_id": "ORD_EDGE_007_BAD_FK",
                    "customer_id": "CUST_NONEXISTENT_99999",
                    "order_amount": "95.00",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 8. Malformed Amount (Non-numeric string - Invalid)
                {
                    "order_id": "ORD_EDGE_008_MALFORMED_AMT",
                    "customer_id": customer_ids[1],
                    "order_amount": "NaN_CORRUPT",
                    "order_status": "PLACED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 9. Malformed Timestamp (Unparseable date string - Invalid)
                {
                    "order_id": "ORD_EDGE_009_BAD_DATE",
                    "customer_id": customer_ids[2],
                    "order_amount": "150.00",
                    "order_status": "PLACED",
                    "order_date": "2026-99-99T99:99:99Z",
                    "updated_at": "2026-99-99T99:99:99Z"
                },
                # 10. Invalid Order Status ('INVALID_STATUS' - Invalid)
                {
                    "order_id": "ORD_EDGE_010_BAD_STATUS",
                    "customer_id": customer_ids[3],
                    "order_amount": "60.00",
                    "order_status": "PENDING_APPROVAL",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 11. Order Dated Before Customer Creation (e.g. 2020 order for 2025 customer - Invalid)
                {
                    "order_id": "ORD_EDGE_011_BEFORE_CUST",
                    "customer_id": customer_ids[0],  # Created in 2025
                    "order_amount": "110.00",
                    "order_status": "PLACED",
                    "order_date": "2020-01-01T00:00:00+00:00",
                    "updated_at": "2020-01-01T00:00:00+00:00"
                },
                # 12. Exact Duplicate: Replicates ORD_000001 exactly (Keep one, count duplicate)
                {
                    "order_id": "ORD_000001",
                    "customer_id": orders[0]["customer_id"],
                    "order_amount": orders[0]["order_amount"],
                    "order_status": orders[0]["order_status"],
                    "order_date": orders[0]["order_date"],
                    "updated_at": orders[0]["updated_at"]
                },
                # 13. Conflicting Duplicate Key: Reuses ORD_000002 with completely different amount & date (Invalid)
                {
                    "order_id": "ORD_000002",
                    "customer_id": orders[1]["customer_id"],
                    "order_amount": "9999.99",
                    "order_status": "CANCELLED",
                    "order_date": base_time.isoformat(),
                    "updated_at": base_time.isoformat()
                },
                # 14. Late-Arriving Record (Valid, from 90 days ago)
                {
                    "order_id": "ORD_EDGE_014_LATE_ARRIVING",
                    "customer_id": customer_ids[2],
                    "order_amount": "320.00",
                    "order_status": "DELIVERED",
                    "order_date": (base_time - timedelta(days=90)).isoformat(),
                    "updated_at": (base_time - timedelta(days=90)).isoformat()
                }
            ]
            orders.extend(edge_cases)

        # Save to CSV
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["order_id", "customer_id", "order_amount", "order_status", "order_date", "updated_at"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(orders)

        return orders
