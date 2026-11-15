import logging
from pathlib import Path
from typing import Tuple, List, Optional
import pandas as pd

from src.config import ORDER_REQUIRED_COLUMNS, CUSTOMER_REQUIRED_COLUMNS

logger = logging.getLogger("ecommerce_pipeline.ingestion")


class IngestionEngine:
    """
    Ingests source CSV and JSON datasets safely into Pandas DataFrames,
    retaining original row numbers and verifying structural schema completeness.
    """

    @staticmethod
    def read_source_file(file_path: Path) -> Tuple[pd.DataFrame, bool, Optional[str]]:
        """
        Reads a source file into a DataFrame, tagging each row with its 1-based line number.
        Returns (df, is_empty, error_message).
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        if file_path.stat().st_size == 0:
            logger.warning(f"Source file is empty (0 bytes): {file_path.name}")
            return pd.DataFrame(), True, None

        try:
            # Read all columns as string to avoid premature type casting/loss of precision
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path.name}: {e}")
            return pd.DataFrame(), False, f"File parse error: {str(e)}"

        if df.empty:
            logger.warning(f"File {file_path.name} contains header only, zero data rows.")
            return df, True, None

        # Add 1-based source row number (header is row 1, data starts at row 2)
        df["_source_row_number"] = range(2, len(df) + 2)
        logger.info(f"Ingested {len(df)} rows from {file_path.name}")
        return df, False, None

    @staticmethod
    def validate_schema_columns(df: pd.DataFrame, expected_columns: List[str]) -> Tuple[bool, List[str]]:
        """Verifies that all expected required columns exist in the DataFrame."""
        if df.empty:
            return True, []
        missing = [col for col in expected_columns if col not in df.columns]
        return len(missing) == 0, missing
