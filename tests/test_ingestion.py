import pytest
from pathlib import Path
import pandas as pd

from src.ingestion import IngestionEngine
from src.config import ORDER_REQUIRED_COLUMNS, CUSTOMER_REQUIRED_COLUMNS


def test_ingest_valid_csv(tmp_path):
    csv_file = tmp_path / "valid.csv"
    csv_file.write_text("order_id,customer_id,order_amount,order_status,order_date,updated_at\nORD_1,CUST_1,100.0,PLACED,2026-06-01T10:00:00Z,2026-06-01T10:00:00Z\n", encoding="utf-8")

    df, is_empty, err = IngestionEngine.read_source_file(csv_file)
    assert not is_empty
    assert err is None
    assert len(df) == 1
    assert df["_source_row_number"].iloc[0] == 2  # Line 2 in file
    assert df["order_id"].iloc[0] == "ORD_1"


def test_ingest_empty_file_zero_bytes(tmp_path):
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("", encoding="utf-8")

    df, is_empty, err = IngestionEngine.read_source_file(empty_file)
    assert is_empty
    assert len(df) == 0


def test_ingest_header_only_file(tmp_path):
    header_file = tmp_path / "header_only.csv"
    header_file.write_text("order_id,customer_id,order_amount,order_status,order_date,updated_at\n", encoding="utf-8")

    df, is_empty, err = IngestionEngine.read_source_file(header_file)
    assert is_empty
    assert len(df) == 0


def test_ingest_missing_file_raises_error(tmp_path):
    missing_file = tmp_path / "non_existent.csv"
    with pytest.raises(FileNotFoundError):
        IngestionEngine.read_source_file(missing_file)


def test_validate_schema_columns_missing():
    df = pd.DataFrame([{"order_id": "ORD_1", "customer_id": "CUST_1"}])
    valid, missing = IngestionEngine.validate_schema_columns(df, ORDER_REQUIRED_COLUMNS)
    assert not valid
    assert "order_amount" in missing
    assert "order_status" in missing
