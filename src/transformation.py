from datetime import datetime, timezone
from typing import List, Dict, Any
from src.config import HIGH_VALUE_THRESHOLD


class DataTransformer:
    """
    Transforms validated order records, injecting business flags,
    lineage metadata, and normalizing numerical and date formats.
    """

    @staticmethod
    def transform_orders(valid_orders: List[Dict[str, Any]], run_id: str) -> List[Dict[str, Any]]:
        """
        Transforms cleaned order records into the target clean_orders schema:
        - Flags unusually high order amounts (is_high_value = 1 if >= HIGH_VALUE_THRESHOLD else 0)
        - Injects run_id and loaded_at lineage timestamps.
        """
        transformed: List[Dict[str, Any]] = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for row in valid_orders:
            amount = float(row["order_amount"])
            is_high_value = 1 if amount >= HIGH_VALUE_THRESHOLD else 0

            transformed.append({
                "order_id": str(row["order_id"]),
                "customer_id": str(row["customer_id"]),
                "order_amount": round(amount, 2),
                "order_status": str(row["order_status"]).upper(),
                "order_date": str(row["order_date"]),
                "updated_at": str(row["updated_at"]),
                "is_high_value": is_high_value,
                "run_id": run_id,
                "loaded_at": now_iso
            })

        return transformed
