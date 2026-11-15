import re
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Set, Optional
import pandas as pd

from src.config import (
    ALLOWED_ORDER_STATUSES,
    CUSTOMER_REQUIRED_COLUMNS,
    ORDER_REQUIRED_COLUMNS,
    EMAIL_REGEX,
    HIGH_VALUE_THRESHOLD
)


class DataValidator:
    """
    Validates source customer and order data against structural,
    domain, boundary, referential integrity, and business timeline rules.
    """

    def __init__(self, valid_customer_map: Optional[Dict[str, str]] = None):
        # customer_id -> created_at ISO timestamp
        self.valid_customer_map = valid_customer_map or {}

    def set_customer_map(self, customer_map: Dict[str, str]) -> None:
        """Sets or updates the valid customer reference map."""
        self.valid_customer_map = customer_map

    def validate_customers(
        self,
        df: pd.DataFrame,
        source_file: str,
        run_id: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validates customer records."""
        valid_customers: List[Dict[str, Any]] = []
        quarantine_records: List[Dict[str, Any]] = []

        if df.empty:
            return valid_customers, quarantine_records

        now_iso = datetime.now(timezone.utc).isoformat()
        seen_ids: Set[str] = set()

        for _, row in df.iterrows():
            row_dict = {k: str(v).strip() for k, v in row.to_dict().items() if not k.startswith("_")}
            row_num = row.get("_source_row_number", 0)

            # 1. Missing required fields
            missing_cols = [c for c in CUSTOMER_REQUIRED_COLUMNS if not row_dict.get(c)]
            if missing_cols:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"MISSING_REQUIRED_FIELDS: Missing {', '.join(missing_cols)}",
                    "rejected_at": now_iso
                })
                continue

            cust_id = row_dict["customer_id"]

            # 2. Duplicate customer ID in batch
            if cust_id in seen_ids:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"DUPLICATE_CUSTOMER_ID: customer_id '{cust_id}' appears multiple times",
                    "rejected_at": now_iso
                })
                continue

            # 3. Email format validation
            email = row_dict["email"]
            if not re.match(EMAIL_REGEX, email):
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"INVALID_EMAIL_FORMAT: Email '{email}' is not a valid email address",
                    "rejected_at": now_iso
                })
                continue

            # 4. Valid ISO timestamp
            try:
                datetime.fromisoformat(row_dict["created_at"].replace("Z", "+00:00"))
            except Exception:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"INVALID_TIMESTAMP: created_at '{row_dict['created_at']}' is not ISO-8601",
                    "rejected_at": now_iso
                })
                continue

            seen_ids.add(cust_id)
            valid_customers.append(row_dict)

        return valid_customers, quarantine_records

    def validate_orders(
        self,
        df: pd.DataFrame,
        source_file: str,
        run_id: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
        """
        Validates order records, handles exact and conflicting duplicates,
        verifies boundary values, FK relationships, and business timelines.
        Returns (valid_orders, quarantine_records, duplicate_count).
        """
        valid_orders: List[Dict[str, Any]] = []
        quarantine_records: List[Dict[str, Any]] = []
        duplicate_count = 0

        if df.empty:
            return valid_orders, quarantine_records, duplicate_count

        now_iso = datetime.now(timezone.utc).isoformat()

        # Track first seen record for each order_id to detect exact vs conflicting duplicates
        seen_order_records: Dict[str, Dict[str, Any]] = {}

        for _, row in df.iterrows():
            row_dict = {k: str(v).strip() for k, v in row.to_dict().items() if not k.startswith("_")}
            row_num = row.get("_source_row_number", 0)

            # 1. Missing Primary Key (order_id)
            order_id = row_dict.get("order_id", "").strip()
            if not order_id:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": "MISSING_PRIMARY_KEY: order_id is missing or empty",
                    "rejected_at": now_iso
                })
                continue

            # 2. Missing Customer ID
            customer_id = row_dict.get("customer_id", "").strip()
            if not customer_id:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": "MISSING_CUSTOMER_ID: customer_id is missing or empty",
                    "rejected_at": now_iso
                })
                continue

            # 3. Duplicate Order ID Detection (Exact vs Conflicting)
            if order_id in seen_order_records:
                first_record = seen_order_records[order_id]
                # Compare critical attributes to distinguish exact duplicate from conflict
                is_exact = (
                    first_record.get("customer_id") == customer_id and
                    first_record.get("order_amount") == row_dict.get("order_amount") and
                    first_record.get("order_status") == row_dict.get("order_status") and
                    first_record.get("order_date") == row_dict.get("order_date")
                )
                if is_exact:
                    # Keep one copy of exact duplicate records, record duplicate counts
                    duplicate_count += 1
                    continue
                else:
                    # Conflicting duplicate key -> quarantine with clear reason
                    duplicate_count += 1
                    quarantine_records.append({
                        "run_id": run_id,
                        "source_file": source_file,
                        "source_row_number": row_num,
                        "raw_record": json.dumps(row_dict),
                        "rejection_reason": (
                            f"CONFLICTING_DUPLICATE_KEY: order_id '{order_id}' was already observed "
                            f"with differing attributes (original amount: {first_record.get('order_amount')}, "
                            f"current amount: {row_dict.get('order_amount')})"
                        ),
                        "rejected_at": now_iso
                    })
                    continue

            # 4. Numeric Order Amount & Boundary Validation
            raw_amount = row_dict.get("order_amount", "")
            try:
                amount = float(raw_amount)
                if math.isnan(amount) or math.isinf(amount):
                    raise ValueError("Amount is NaN or Inf")
            except (ValueError, TypeError):
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"MALFORMED_ORDER_AMOUNT: Amount '{raw_amount}' is not a valid numeric value",
                    "rejected_at": now_iso
                })
                continue

            if amount <= 0.0:
                reason = "ZERO_ORDER_AMOUNT" if amount == 0.0 else "NEGATIVE_ORDER_AMOUNT"
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"{reason}: order_amount {amount} must be greater than zero",
                    "rejected_at": now_iso
                })
                continue

            # 5. Allowed Order Status
            status = row_dict.get("order_status", "").strip().upper()
            if status not in ALLOWED_ORDER_STATUSES:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": (
                        f"INVALID_ORDER_STATUS: Status '{status}' is not in allowed statuses "
                        f"{sorted(list(ALLOWED_ORDER_STATUSES))}"
                    ),
                    "rejected_at": now_iso
                })
                continue

            # 6. Valid ISO-8601 Timestamps
            raw_order_date = row_dict.get("order_date", "")
            raw_updated_at = row_dict.get("updated_at", "")
            try:
                parsed_order_dt = datetime.fromisoformat(raw_order_date.replace("Z", "+00:00"))
                parsed_updated_dt = datetime.fromisoformat(raw_updated_at.replace("Z", "+00:00"))
            except Exception:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"MALFORMED_TIMESTAMP: order_date or updated_at is not valid ISO-8601",
                    "rejected_at": now_iso
                })
                continue

            # 7. Customer Foreign Key Relationship
            if customer_id not in self.valid_customer_map:
                quarantine_records.append({
                    "run_id": run_id,
                    "source_file": source_file,
                    "source_row_number": row_num,
                    "raw_record": json.dumps(row_dict),
                    "rejection_reason": f"INVALID_CUSTOMER_REFERENCE: customer_id '{customer_id}' does not exist in customers master",
                    "rejected_at": now_iso
                })
                continue

            # 8. Check Orders Dated Before Customer Creation
            cust_created_str = self.valid_customer_map[customer_id]
            try:
                cust_created_dt = datetime.fromisoformat(cust_created_str.replace("Z", "+00:00"))
                if parsed_order_dt < cust_created_dt:
                    quarantine_records.append({
                        "run_id": run_id,
                        "source_file": source_file,
                        "source_row_number": row_num,
                        "raw_record": json.dumps(row_dict),
                        "rejection_reason": (
                            f"ORDER_BEFORE_CUSTOMER_CREATION: order_date '{raw_order_date}' "
                            f"is earlier than customer creation date '{cust_created_str}'"
                        ),
                        "rejected_at": now_iso
                    })
                    continue
            except Exception as e:
                # Fallback if customer date parse fails
                pass

            # Valid order record
            cleaned_row = {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_amount": round(amount, 2),
                "order_status": status,
                "order_date": parsed_order_dt.isoformat(),
                "updated_at": parsed_updated_dt.isoformat()
            }
            seen_order_records[order_id] = row_dict
            valid_orders.append(cleaned_row)

        return valid_orders, quarantine_records, duplicate_count
