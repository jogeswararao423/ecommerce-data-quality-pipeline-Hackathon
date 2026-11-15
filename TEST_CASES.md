# Complete Test Cases Specification & Verification Matrix

**Project:** E-Commerce Data Quality & Automated Pipeline Testing Framework  
**Test Framework:** Pytest 8.0+  
**Execution Status:** 31 Passed / 0 Failed (100% Success Rate)

---

## 1. Test Summary by Category

| Category | Test Module | Total Tests | Status |
| :--- | :--- | :---: | :---: |
| **Ingestion & Schema** | `tests/test_ingestion.py` | 5 | **PASSED** |
| **Validation & Boundaries** | `tests/test_validation.py` | 12 | **PASSED** |
| **Transformations** | `tests/test_transformation.py` | 2 | **PASSED** |
| **Duplicate Detection** | `tests/test_duplicates.py` | 3 | **PASSED** |
| **Pipeline Integration** | `tests/test_pipeline.py` | 6 | **PASSED** |
| **Idempotency & History** | `tests/test_idempotency.py` | 3 | **PASSED** |
| **Total** | | **31** | **ALL PASSED** |

---

## 2. Granular Test Cases Catalog

### A. Ingestion & Schema Tests (`tests/test_ingestion.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-ING-01** | `test_ingest_valid_csv` | Standard valid CSV file with data | Reads rows, tags 1-based source row numbers (`_source_row_number = 2`) | `PASSED` |
| **TC-ING-02** | `test_ingest_empty_file_zero_bytes` | 0-byte physical file | Handled gracefully; returns empty DataFrame, `is_empty = True` | `PASSED` |
| **TC-ING-03** | `test_ingest_header_only_file` | File containing only column header row | Handled gracefully; returns 0 rows, `is_empty = True` | `PASSED` |
| **TC-ING-04** | `test_ingest_missing_file_raises_error` | Non-existent file path | Raises `FileNotFoundError` with meaningful message | `PASSED` |
| **TC-ING-05** | `test_validate_schema_columns_missing` | CSV missing `order_amount` & `order_status` | Detects schema discrepancy; flags missing columns | `PASSED` |

### B. Validation & Domain Boundary Tests (`tests/test_validation.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-VAL-01** | `test_validation_happy_path` | Fully valid order record | Accepted into `valid_orders`; 0 quarantined | `PASSED` |
| **TC-VAL-02** | `test_validation_missing_primary_key` | Blank / empty `order_id` (`""`) | Quarantined with `MISSING_PRIMARY_KEY` | `PASSED` |
| **TC-VAL-03** | `test_validation_missing_customer_id` | Blank / empty `customer_id` (`""`) | Quarantined with `MISSING_CUSTOMER_ID` | `PASSED` |
| **TC-VAL-04** | `test_validation_negative_order_amount` | Negative amount (`order_amount = -50.00`) | Quarantined with `NEGATIVE_ORDER_AMOUNT` | `PASSED` |
| **TC-VAL-05** | `test_validation_zero_order_amount` | Zero amount (`order_amount = 0.00`) | Quarantined with `ZERO_ORDER_AMOUNT` | `PASSED` |
| **TC-VAL-06** | `test_validation_malformed_order_amount` | Non-numeric string (`"not_a_number"`) | Quarantined with `MALFORMED_ORDER_AMOUNT` | `PASSED` |
| **TC-VAL-07** | `test_validation_invalid_order_status` | Status `"REFUNDED"` (not in whitelist) | Quarantined with `INVALID_ORDER_STATUS` | `PASSED` |
| **TC-VAL-08** | `test_validation_invalid_customer_reference` | `customer_id = "CUST_NONEXISTENT"` | Quarantined with `INVALID_CUSTOMER_REFERENCE` | `PASSED` |
| **TC-VAL-09** | `test_validation_order_before_customer_creation` | Order date (2024) < Customer create (2025) | Quarantined with `ORDER_BEFORE_CUSTOMER_CREATION` | `PASSED` |
| **TC-VAL-10** | `test_validation_invalid_timestamp` | Unparseable timestamp (`"2026-99-99T99:99:99Z"`) | Quarantined with `MALFORMED_TIMESTAMP` | `PASSED` |
| **TC-VAL-11** | `test_validation_boundary_minimum_amount` | Exact boundary minimum amount (`$0.01`) | Accepted as valid order; amount = 0.01 | `PASSED` |
| **TC-VAL-12** | `test_validation_customer_email_format` | Customer email `"invalid_email_format"` | Quarantined with `INVALID_EMAIL_FORMAT` | `PASSED` |

### C. Transformation Tests (`tests/test_transformation.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-TRF-01** | `test_transform_high_value_flag` | Order amount $500 vs $2500 | Sets `is_high_value = 0` for $500; `is_high_value = 1` for $2500 | `PASSED` |
| **TC-TRF-02** | `test_transform_rounding` | Floating point amount `12.3456` | Rounds to 2 decimal places (`12.35`), status uppercase | `PASSED` |

### D. Duplicate Detection Tests (`tests/test_duplicates.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-DUP-01** | `test_exact_duplicates_kept_once` | 2 identical orders with same attributes | 1 copy accepted in valid rows, `duplicate_count = 1` | `PASSED` |
| **TC-DUP-02** | `test_conflicting_duplicates_quarantined` | Same `order_id` with differing amounts | Quarantined with `CONFLICTING_DUPLICATE_KEY` | `PASSED` |
| **TC-DUP-03** | `test_duplicate_customers_quarantined` | 2 customers with same `customer_id` | Second customer quarantined with `DUPLICATE_CUSTOMER_ID` | `PASSED` |

### E. Pipeline Integration Tests (`tests/test_pipeline.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-PIP-01** | `test_pipeline_happy_path` | 2 valid orders | Status `SUCCESS`, 2 records loaded into `clean_orders` | `PASSED` |
| **TC-PIP-02** | `test_pipeline_empty_orders_file` | Empty orders file | Status `SUCCESS`, 0 records loaded, no crash | `PASSED` |
| **TC-PIP-03** | `test_pipeline_partial_failure` | 1 valid order + 1 negative amount order | Status `PARTIAL_SUCCESS`: 1 valid loaded, 1 quarantined | `PASSED` |
| **TC-PIP-04** | `test_pipeline_corrupted_schema` | File missing required schema columns | Status `FAILED`, all rows quarantined under `SCHEMA_CORRUPTION` | `PASSED` |
| **TC-PIP-05** | `test_pipeline_late_arriving_records` | Valid historical order from 90 days ago | Ingested successfully, historical date preserved | `PASSED` |
| **TC-PIP-06** | `test_database_transaction_rollback` | Mid-transaction exception simulated | Full rollback; zero uncommitted rows persist | `PASSED` |

### F. Idempotency & State History Tests (`tests/test_idempotency.py`)

| ID | Test Function | Input Condition | Expected Behavior | Result |
| :--- | :--- | :--- | :--- | :---: |
| **TC-IDM-01** | `test_rerun_same_input_is_idempotent` | Exact same file executed twice | Target row count remains identical (5 rows); 0 duplicate keys | `PASSED` |
| **TC-IDM-02** | `test_order_lifecycle_update_and_history` | Status changes from `PLACED` to `DELIVERED` | `clean_orders` updates in-place (1 row); `order_history` logs both states | `PASSED` |
| **TC-IDM-03** | `test_retry_after_failure` | Corrupted run followed by corrected batch | Corrected batch succeeds, loads clean records idempotently | `PASSED` |
