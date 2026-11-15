# E-Commerce Data Quality & Automated Pipeline Testing Framework

[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Test Suite](https://img.shields.io/badge/pytest-31%20passed-success.svg)](https://docs.pytest.org/)
[![Database](https://img.shields.io/badge/storage-SQLite%20WAL-lightgrey.svg)](https://www.sqlite.org/)
[![Status](https://img.shields.io/badge/pipeline-idempotent-brightgreen.svg)]()

> **Production-Grade Data Engineering Hackathon Solution (Target: 0–3 Years Experience)**  
> A clean, deterministic, maintainable, and fully tested Data Engineering pipeline for E-Commerce order ingestion, multi-stage validation, duplicate detection, quarantine isolation, idempotent publishing, operational audit logging, and automated SQL reconciliation.

---

## 1. Project Overview & Business Problem

In an e-commerce order management system, transactions and customer updates arrive continuously from web and mobile apps, partner feeds, and third-party gateways. Raw incoming records frequently contain:
- Corrupted data types (non-numeric strings in order amounts, malformed ISO timestamps).
- Missing primary keys or customer IDs.
- Invalid customer references (foreign keys pointing to non-existent customer records).
- Negative or zero dollar order amounts.
- Business rule timeline anomalies (orders placed before the referenced customer account was created).
- Exact and conflicting duplicate orders.

**Objective**: Build a reliable, reproducible, and verifiable Data Engineering pipeline that ingests customer and order batches, validates data quality, detects duplicates, quarantines invalid rows with forensic traceability, applies business transformations, publishes clean records to a local SQLite database idempotently, and generates structured execution and data-quality audit reports.

---

## 2. Architecture & Data Flow

```
                      +-----------------------------------+
                      |        Raw Source Batch           |
                      | (customers.csv / orders.csv)      |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------+-----------------+
                      |     Stage 1: Ingestion & Parsing  |
                      |   - Empty file handling (0 bytes) |
                      |   - Source row number tagging     |
                      |   - Schema column verification    |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------+-----------------+
                      |     Stage 2: Validation Engine    |
                      |   - Missing PK / Customer ID      |
                      |   - Amount parse & boundary (>0)  |
                      |   - Allowed status verification   |
                      |   - ISO-8601 UTC timestamp check  |
                      |   - Customer foreign key check    |
                      |   - Order dated < Customer create |
                      |   - Exact duplicate detection     |
                      |   - Conflicting key quarantine    |
                      +--------+-----------------+--------+
                               |                 |
                   Failed Rows |                 | Valid Rows
                               v                 v
           +-------------------+----+       +----+-------------------+
           | Stage 3A: Quarantine   |       | Stage 3B: Transform    |
           | - Error classification |       | - High-value flag      |
           | - Diagnostic reason    |       | - Lineage metadata     |
           | - Source file & row #  |       |   (run_id, loaded_at)  |
           | - Full raw JSON record |       | - Status normalization |
           +-------------------+----+       +----+-------------------+
                               |                 |
                               v                 v
           +-------------------+----+       +----+-------------------+
           | rejected_records       |       | clean_orders           |
           | (Quarantine DB Table)  |       | (Idempotent UPSERT)    |
           +------------------------+       +----+-------------------+
                                                 |
                                                 v
                                            +----+-------------------+
                                            | order_history          |
                                            | (Preserve State Audit) |
                                            +----+-------------------+
                                                 |
                                                 v
                                            +----+-------------------+
                                            | pipeline_runs          |
                                            | (Operational Metrics)  |
                                            +----+-------------------+
                                                 |
                                                 v
                                            +----+-------------------+
                                            | Monitoring & Reports   |
                                            | - clean_orders.csv     |
                                            | - rejected_records.csv |
                                            | - quality_report.json  |
                                            | - performance_report   |
                                            +------------------------+
```

---

## 3. Technology Stack

- **Python 3.11+**: Core pipeline runtime and business logic.
- **Pandas**: Efficient tabular data ingestion, columnar checking, and CSV serialization.
- **SQLite**: Local relational database engine with ACID transactions and Write-Ahead Logging (`WAL`).
- **Pytest**: Automated test framework for unit, integration, and idempotency tests.
- **NumPy**: Reproducible synthetic dataset generation with controlled error distributions (`seed=42`).
- **Python Logging**: Structured console and file execution logs (`logs/pipeline.log`).
- **SQL**: In-database reconciliation queries and integrity assertions.

*Zero external cloud services, zero paid APIs, zero Docker containers, and zero external credentials required.*

---

## 4. Source & Target Schemas (Data Dictionary)

### A. Source Entity: Customers (`customers.csv`)
| Column Name | Data Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| `customer_id` | String | Primary Key, Not Null | Unique customer identifier (e.g. `CUST_0001`). |
| `customer_name` | String | Not Null | Full customer name. |
| `email` | String | Not Null, Regex Valid | Valid email format (`user@domain.com`). |
| `city` | String | Not Null | Customer residential city. |
| `created_at` | String (ISO-8601) | Not Null | Account creation timestamp in UTC. |

### B. Source Entity: Orders (`orders.csv`)
| Column Name | Data Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | String | Primary Key, Not Null | Unique order identifier (e.g. `ORD_000001`). |
| `customer_id` | String | Foreign Key, Not Null | References valid `customer_id` in customers master. |
| `order_amount` | Decimal / Float | $> 0.00$ | Order purchase amount. Cannot be zero or negative. |
| `order_status` | String | Enum Whitelist | Allowed: `PLACED`, `SHIPPED`, `DELIVERED`, `CANCELLED`. |
| `order_date` | String (ISO-8601) | Not Null | Order placement timestamp in UTC. |
| `updated_at` | String (ISO-8601) | Not Null | Last update timestamp in UTC. |

### C. Target Entity: Clean Orders (`clean_orders`)
| Column Name | Data Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | TEXT | PRIMARY KEY | Deterministic unique order identifier. |
| `customer_id` | TEXT | FOREIGN KEY | References `customers(customer_id)`. |
| `order_amount` | REAL | NOT NULL | Clean, validated numeric order amount. |
| `order_status` | TEXT | NOT NULL | Normalized order status. |
| `order_date` | TEXT | NOT NULL | Standardized ISO-8601 UTC timestamp. |
| `updated_at` | TEXT | NOT NULL | Standardized ISO-8601 UTC timestamp. |
| `is_high_value` | INTEGER | NOT NULL | Business flag: `1` if `order_amount >= 1500.00`, else `0`. |
| `run_id` | TEXT | NOT NULL | Lineage: specific pipeline run identifier. |
| `loaded_at` | TEXT | NOT NULL | Lineage: database ingestion timestamp (UTC). |

### D. Target Entity: Quarantined Records (`rejected_records`)
| Column Name | Data Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `rejection_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Unique rejection audit ID. |
| `run_id` | TEXT | NOT NULL | Associated pipeline run ID. |
| `source_file` | TEXT | NOT NULL | Origin file name (`orders.csv` / `customers.csv`). |
| `source_row_number` | INTEGER | NOT NULL | Exact 1-based row number in the physical source file. |
| `raw_record` | TEXT | NOT NULL | Full original verbatim record stored as JSON. |
| `rejection_reason` | TEXT | NOT NULL | Diagnostic rejection reason and violated rule code. |
| `rejected_at` | TEXT | NOT NULL | Quarantine timestamp (ISO-8601 UTC). |

### E. Operational Run History (`pipeline_runs`)
Tracks execution telemetry: `run_id`, `start_time`, `end_time`, `execution_duration`, `pipeline_status`, `input_count`, `valid_count`, `rejected_count`, `duplicate_count`, `output_count`, `failure_reason`.

### F. Order Change State History (`order_history`)
Preserves complete lifecycle audit trail on every insert/update: `history_id`, `order_id`, `customer_id`, `order_amount`, `order_status`, `order_date`, `updated_at`, `run_id`, `recorded_at`.

---

## 5. Business & Data Validation Rules

Every incoming row is evaluated against strict business validation gates:
1. **Schema Integrity**: The file must contain all required columns. Missing columns trigger `SCHEMA_CORRUPTION` batch quarantine.
2. **Primary Key Nullability**: Missing or empty `order_id` triggers `MISSING_PRIMARY_KEY`.
3. **Customer Foreign Key Nullability**: Missing `customer_id` triggers `MISSING_CUSTOMER_ID`.
4. **Referential Integrity**: `customer_id` must exist in `customers`. Unmatched IDs trigger `INVALID_CUSTOMER_REFERENCE`.
5. **Positive Order Amounts**: Amounts $\le 0.00$ trigger `ZERO_ORDER_AMOUNT` or `NEGATIVE_ORDER_AMOUNT`. Malformed strings trigger `MALFORMED_ORDER_AMOUNT`.
6. **Domain Status Whitelist**: Statuses outside `PLACED`, `SHIPPED`, `DELIVERED`, `CANCELLED` trigger `INVALID_ORDER_STATUS`.
7. **Timestamp Validity**: Must parse as ISO-8601 timestamps; otherwise triggers `MALFORMED_TIMESTAMP`.
8. **Business Timeline Anomaly**: If `order_date < customer.created_at`, triggers `ORDER_BEFORE_CUSTOMER_CREATION`.
9. **Duplicate Detection**:
   - **Exact Duplicates**: Rows sharing identical attributes have one copy retained, and `duplicate_count` is incremented.
   - **Conflicting Duplicates**: Rows sharing the same `order_id` with differing amounts, dates, or statuses are quarantined with `CONFLICTING_DUPLICATE_KEY`.
10. **High-Value Order Flagging**: Orders with `order_amount >= 1500.00` are flagged with `is_high_value = 1` for downstream analytics review.
11. **Forensic Quarantine Retention**: Rejected records are never silently dropped; they are stored in `rejected_records` and `output/rejected_records.csv` with original source line numbers and raw payloads.

---

## 6. Project Folder Structure

```
c:\Users\lokes\Downloads\project\
├── data/
│   ├── raw/                       # Default input files (customers.csv, orders.csv)
│   └── generated/                 # Benchmarking and scale datasets (1k, 10k, 100k)
├── src/
│   ├── __init__.py                # Package initialization
│   ├── config.py                  # Centralized paths, schemas, and rule thresholds
│   ├── ingestion.py               # Safe file reading, line numbering, schema checks
│   ├── validation.py              # Multi-rule validation engine and quarantine routing
│   ├── transformation.py          # Business transformations and high-value flagging
│   ├── database.py                # SQLite DDL, transactions, idempotent upserts, history
│   ├── monitoring.py              # Logger setup, reconciliation checks, JSON report exporter
│   ├── data_generator.py          # Reproducible synthetic data generator (seed=42)
│   └── pipeline.py                # Main end-to-end pipeline runner & benchmark CLI
├── tests/
│   ├── test_ingestion.py          # Ingestion unit tests (empty files, header-only, schema)
│   ├── test_validation.py         # Validation unit tests (bounds, nulls, status, timelines)
│   ├── test_transformation.py     # Transformation tests (high value flag, rounding)
│   ├── test_duplicates.py         # Exact and conflicting duplicate tests
│   ├── test_pipeline.py           # Integration tests (happy path, rollback, empty batch)
│   └── test_idempotency.py        # Idempotency and lifecycle update tests
├── output/
│   ├── clean_orders.csv           # Clean exported order dataset
│   ├── rejected_records.csv       # Quarantined invalid records with diagnostics
│   └── ecommerce.db               # SQLite database file
├── logs/
│   └── pipeline.log               # Execution logs
├── reports/
│   ├── quality_report.json        # Structured JSON data quality and reconciliation audit
│   ├── performance_report.txt     # Measured benchmark results across 1k, 10k, 100k records
│   ├── test_execution_report.txt  # Real recorded pytest test suite execution evidence
│   └── execution_evidence.txt     # Real recorded pipeline & benchmark execution evidence
├── run_pipeline.bat               # One-click Windows execution runner
├── run_pipeline.sh                # One-click Linux/Mac execution runner
├── requirements.txt               # Minimal Python dependencies
├── pytest.ini                     # Pytest configuration
├── README.md                      # Comprehensive project documentation
├── DATA_DICTIONARY.md             # Dedicated schema & entity specification
├── DESIGN_NOTES.md                # Dedicated architectural trade-off analysis
├── TEST_CASES.md                  # Comprehensive 31 test cases catalog & matrix
└── .gitignore                     # Git ignore rules
```

---

## 7. Installation & Quickstart

### Prerequisites
- Python 3.11 or higher
- PowerShell, Bash, or terminal

### Step 1: Install Dependencies
```powershell
pip install -r requirements.txt
```

### Step 2: Generate Synthetic Data & Run Pipeline
To generate a starter test dataset (100 orders + customer master with controlled edge cases) and execute the pipeline in one command:
```powershell
python src/pipeline.py --generate --records 100
```

### Step 3: View Execution Summary
The command outputs:
```
================== EXECUTION SUMMARY ==================
  run_id                : run_20261115_085629_6c8fce
  pipeline_status       : PARTIAL_SUCCESS
  input_count           : 114
  valid_count           : 99
  rejected_count        : 14
  duplicate_count       : 2
  output_count          : 99
  execution_duration    : 0.0492
=======================================================
```

---

## 8. Automated Test Suite Execution

Run the complete automated test suite using `pytest`:
```powershell
pytest -v
```

### Test Results: 31 Passed in 1.57 Seconds
| Test Module | Test Case | Target Scenario | Result |
| :--- | :--- | :--- | :--- |
| `test_ingestion.py` | `test_ingest_valid_csv` | Ingests CSV & attaches 1-based line numbers | `PASSED` |
| | `test_ingest_empty_file_zero_bytes` | 0-byte file handled safely without crashing | `PASSED` |
| | `test_ingest_header_only_file` | Header-only file returns empty DataFrame | `PASSED` |
| | `test_ingest_missing_file_raises_error` | Missing file raises `FileNotFoundError` | `PASSED` |
| | `test_validate_schema_columns_missing` | Detects missing required schema columns | `PASSED` |
| `test_validation.py` | `test_validation_happy_path` | Valid record passes all validation rules | `PASSED` |
| | `test_validation_missing_primary_key` | Null or empty `order_id` quarantined | `PASSED` |
| | `test_validation_missing_customer_id` | Null or empty `customer_id` quarantined | `PASSED` |
| | `test_validation_negative_order_amount` | Negative amount quarantined | `PASSED` |
| | `test_validation_zero_order_amount` | Zero amount quarantined | `PASSED` |
| | `test_validation_malformed_order_amount` | String amount quarantined | `PASSED` |
| | `test_validation_invalid_order_status` | Unapproved status quarantined | `PASSED` |
| | `test_validation_invalid_customer_reference` | Orphan customer foreign key quarantined | `PASSED` |
| | `test_validation_order_before_customer_creation` | Order dated before customer creation quarantined | `PASSED` |
| | `test_validation_invalid_timestamp` | Unparseable timestamp quarantined | `PASSED` |
| | `test_validation_boundary_minimum_amount` | Exact boundary minimum $0.01 validated | `PASSED` |
| | `test_validation_customer_email_format` | Malformed email quarantined | `PASSED` |
| `test_duplicates.py` | `test_exact_duplicates_kept_once` | Exact duplicate kept once, count recorded | `PASSED` |
| | `test_conflicting_duplicates_quarantined` | Conflicting duplicate key quarantined | `PASSED` |
| | `test_duplicate_customers_quarantined` | Duplicate customer ID quarantined | `PASSED` |
| `test_transformation.py`| `test_transform_high_value_flag` | Orders $\ge \$1,500$ flagged `is_high_value=1` | `PASSED` |
| | `test_transform_rounding` | Rounding to 2 decimal places & status upper | `PASSED` |
| `test_pipeline.py` | `test_pipeline_happy_path` | End-to-end execution & database persistence | `PASSED` |
| | `test_pipeline_empty_orders_file` | Empty file produces SUCCESS status with 0 rows | `PASSED` |
| | `test_pipeline_partial_failure` | 50% valid, 50% bad batch handled gracefully | `PASSED` |
| | `test_pipeline_corrupted_schema` | Missing columns batch rejected with FAILED | `PASSED` |
| | `test_pipeline_late_arriving_records` | Late-arriving historical order loads smoothly | `PASSED` |
| | `test_database_transaction_rollback` | Database transaction rolls back on mid-failure | `PASSED` |
| `test_idempotency.py` | `test_rerun_same_input_is_idempotent` | Rerunning identical batch leaves count unchanged | `PASSED` |
| | `test_order_lifecycle_update_and_history` | Status update in-place + history preserved | `PASSED` |
| | `test_retry_after_failure` | Failed run followed by corrected batch succeeds | `PASSED` |

---

## 9. Automated Reconciliation & Audit Report

Every pipeline execution automatically executes SQL reconciliation queries against `ecommerce.db` and writes results to `reports/quality_report.json`:

```json
{
  "report_generated_at": "2026-11-15T08:56:29.222223+00:00",
  "execution_metrics": {
    "run_id": "run_20261115_085629_6c8fce",
    "pipeline_status": "PARTIAL_SUCCESS",
    "input_count": 114,
    "valid_count": 99,
    "rejected_count": 14,
    "duplicate_count": 2,
    "output_count": 99,
    "execution_duration": 0.0492
  },
  "reconciliation": {
    "reconciliation_checks": [
      {
        "check": "No duplicate order IDs in clean_orders",
        "status": "PASSED",
        "details": "0 duplicates found"
      },
      {
        "check": "No null primary keys in clean_orders",
        "status": "PASSED",
        "details": "0 null primary keys found"
      },
      {
        "check": "No negative or zero order amounts in clean_orders",
        "status": "PASSED",
        "details": "0 non-positive amounts found"
      },
      {
        "check": "No orphaned customer foreign keys in clean_orders",
        "status": "PASSED",
        "details": "0 orphans found"
      },
      {
        "check": "No invalid order statuses in clean_orders",
        "status": "PASSED",
        "details": "0 invalid statuses found"
      }
    ],
    "all_passed": true,
    "target_aggregates": {
      "total_clean_orders": 99,
      "total_order_amount_usd": 26475.78,
      "total_quarantined_records": 16
    }
  }
}
```

---

## 10. Performance Benchmarks & Optimization

To execute performance measurement across 1,000, 10,000, and 100,000 records:
```powershell
python src/pipeline.py --benchmark
```

### Measured Benchmark Results
```
Record Count    | Duration (s)    | Throughput (rows/s)    | Status         
---------------------------------------------------------------------------
1,000           | 0.1690          | 5,917.2 rows/sec       | PARTIAL_SUCCESS
10,000          | 1.0331          | 9,679.6 rows/sec       | PARTIAL_SUCCESS
100,000         | 10.6762         | 9,366.6 rows/sec       | PARTIAL_SUCCESS
```

### Optimization Analysis
1. **Identified Bottleneck**:
   - Committing transactions per row incurs disk synchronization (`fsync`) overhead, limiting throughput to ~150–300 rows/second.
2. **Implemented Optimization**:
   - **Atomic Bulk Batching**: Clean orders and quarantine records are committed in single atomic transactions using `executemany()`.
   - **SQLite PRAGMA Tuning**: Utilizing `PRAGMA foreign_keys = ON;` and Write-Ahead Logging (`WAL`).
   - **Vectorized String Ingestion**: Reading inputs with `dtype=str` avoids premature Python object overhead.
3. **Net Speedup**:
   - Sustains **>9,300 rows/second**, processing **100,000 records in ~10 seconds** on single-core SQLite.

---

## 11. Idempotency, Recovery & Safe Reruns

- **Deterministic UPSERT**: Loading uses `ON CONFLICT(order_id) DO UPDATE SET ...`. Rerunning an identical batch produces identical target row counts with **zero duplicate records**.
- **State Audit Preservation**: Every update to an existing order automatically records a timestamped snapshot in `order_history`.
- **Atomic Rollback**: If an unhandled failure occurs mid-batch, SQLite rolls back the entire transaction, guaranteeing no half-written or corrupt states.
- **Safe Retries**: Corrected batches can be rerun safely; valid records are inserted or refreshed cleanly.

---

## 12. Assumptions, Known Limitations & Next Steps

### Assumptions
- Customer account creation occurs prior to or at the time of order placement.
- Order amounts are represented in USD with 2 decimal precision.

### Known Limitations
- SQLite is an embedded single-writer database suited for edge or vertical slice pipelines. High concurrent writers require an enterprise database like PostgreSQL.

### Next Steps for Production
- Integrate distributed orchestration (e.g. Apache Airflow or Prefect).
- Scale to multi-terabyte data lakes using PySpark or DuckDB.
