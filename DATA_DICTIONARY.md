# Data Dictionary: E-Commerce Data Quality & Order Pipeline

This document defines the physical and logical schemas, data types, constraints, nullability, keys, and business rules across all source, target, quarantine, and audit entities.

---

## 1. Source Entity: Customers (`customers.csv`)
- **Storage Format**: CSV (UTF-8)
- **Granularity**: One record per registered customer account.
- **File Location**: `data/raw/customers.csv`

| Column Name | Data Type | Nullable | Primary / Foreign Key | Validation Rules & Allowed Values | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `customer_id` | String | **No** | **Primary Key** | Must be unique, non-empty string (e.g. `CUST_0001`). Cannot contain whitespace. | Unique identifier for each customer. |
| `customer_name` | String | **No** | None | Must be a non-empty string. | Full name of the customer. |
| `email` | String | **No** | None | Must match standard email regex: `^[\w\.-]+@[\w\.-]+\.\w+$`. | Customer contact email address. |
| `city` | String | **No** | None | Must be a non-empty string. | Customer city of residence. |
| `created_at` | String (ISO-8601) | **No** | None | Standard UTC ISO-8601 string (`YYYY-MM-DDTHH:MM:SSZ` or offset). | Account registration timestamp. |

---

## 2. Source Entity: Orders (`orders.csv`)
- **Storage Format**: CSV (UTF-8)
- **Granularity**: One record per purchase order transaction.
- **File Location**: `data/raw/orders.csv`

| Column Name | Data Type | Nullable | Primary / Foreign Key | Validation Rules & Allowed Values | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `order_id` | String | **No** | **Primary Key** | Must be unique, non-empty string (e.g. `ORD_000001`). | Unique purchase order transaction ID. |
| `customer_id` | String | **No** | **Foreign Key** | Must reference an existing, valid `customer_id` in `customers`. | Customer account placing the order. |
| `order_amount` | Decimal / Float | **No** | None | Must be numeric, finite, and strictly $> 0.00$. Boundary: Min $0.01. | Total monetary purchase amount. |
| `order_status` | String | **No** | None | Enum whitelist: `PLACED`, `SHIPPED`, `DELIVERED`, `CANCELLED`. | Operational status of the order. |
| `order_date` | String (ISO-8601) | **No** | None | Valid ISO-8601 UTC timestamp. Must be $\ge$ referenced `customer.created_at`. | Date and time order was created. |
| `updated_at` | String (ISO-8601) | **No** | None | Valid ISO-8601 UTC timestamp. Must be $\ge order\_date$. | Timestamp of most recent state change. |

---

## 3. Target Entity: Clean Orders (`clean_orders` Table & CSV)
- **Storage Format**: SQLite Table & `output/clean_orders.csv`
- **Granularity**: One record per validated, accepted order.
- **Table Name**: `clean_orders`

| Column Name | SQLite Data Type | Nullable | Key / Index | Transformation & Derivation Logic | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `order_id` | TEXT | **No** | **Primary Key** | Direct ingestion; uniqueness enforced by database constraint. | Unique order identifier. |
| `customer_id` | TEXT | **No** | **Foreign Key**, Indexed | Foreign key referencing `customers(customer_id)`. | Verified customer identifier. |
| `order_amount` | REAL | **No** | None | Rounded to 2 decimal places. | Validated order amount in USD. |
| `order_status` | TEXT | **No** | None | Normalized to uppercase string. | Current order status. |
| `order_date` | TEXT | **No** | None | Standardized ISO-8601 UTC string. | Order placement timestamp. |
| `updated_at` | TEXT | **No** | None | Standardized ISO-8601 UTC string. | Status update timestamp. |
| `is_high_value` | INTEGER | **No** | None | **Derived Flag**: `1` if `order_amount >= 1500.00`, else `0`. | Business flag for analytical review. |
| `run_id` | TEXT | **No** | None | **Lineage**: Pipeline execution identifier (e.g. `run_20261115_...`). | Execution run that loaded the record. |
| `loaded_at` | TEXT | **No** | None | **Lineage**: ISO-8601 UTC timestamp of ingestion into database. | Timestamp record was written. |

---

## 4. Quarantine Entity: Rejected Records (`rejected_records` Table & CSV)
- **Storage Format**: SQLite Table & `output/rejected_records.csv`
- **Granularity**: One record per quarantined validation failure.
- **Table Name**: `rejected_records`

| Column Name | SQLite Data Type | Nullable | Key / Index | Value Format | Description |
| :--- | :--- | :---: | :---: | :--- | :--- |
| `rejection_id` | INTEGER | **No** | **Primary Key** | Auto-incrementing integer. | Unique rejection identifier. |
| `run_id` | TEXT | **No** | Indexed | String identifier of current pipeline run. | Traceability link to execution run. |
| `source_file` | TEXT | **No** | None | Origin file basename (`orders.csv` / `customers.csv`). | File where corrupt row originated. |
| `source_row_number`| INTEGER | **No** | None | 1-based integer corresponding to row in physical file. | Exact physical line number in file. |
| `raw_record` | TEXT | **No** | None | Verbatim row serialized as JSON string. | Full original row data for auditability. |
| `rejection_reason` | TEXT | **No** | None | Categorical rule prefix + detailed explanation. | Diagnostic explanation of rejection. |
| `rejected_at` | TEXT | **No** | None | ISO-8601 UTC timestamp. | Timestamp of quarantine event. |

### Quarantine Rule Categories
- `MISSING_PRIMARY_KEY`: `order_id` is blank or null.
- `MISSING_CUSTOMER_ID`: `customer_id` is blank or null.
- `MISSING_REQUIRED_FIELDS`: Customer row missing required fields.
- `ZERO_ORDER_AMOUNT`: Amount is exactly $0.00.
- `NEGATIVE_ORDER_AMOUNT`: Amount is less than $0.00.
- `MALFORMED_ORDER_AMOUNT`: Non-numeric string or NaN in amount.
- `INVALID_ORDER_STATUS`: Status not in allowed enum set.
- `MALFORMED_TIMESTAMP`: Date string fails ISO-8601 parsing.
- `INVALID_CUSTOMER_REFERENCE`: Referenced `customer_id` does not exist in `customers`.
- `ORDER_BEFORE_CUSTOMER_CREATION`: `order_date` is earlier than customer account `created_at`.
- `INVALID_EMAIL_FORMAT`: Customer email fails regex format validation.
- `DUPLICATE_CUSTOMER_ID`: Duplicate customer identifier in batch.
- `CONFLICTING_DUPLICATE_KEY`: Duplicate `order_id` with differing attributes.
- `SCHEMA_CORRUPTION`: File is missing required schema columns.

---

## 5. Operational Audit Entity: Pipeline Runs (`pipeline_runs`)
- **Storage Format**: SQLite Table
- **Granularity**: One record per pipeline execution run.
- **Table Name**: `pipeline_runs`

| Column Name | SQLite Data Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `run_id` | TEXT | **No** (PK) | Unique run identifier (`run_YYYYMMDD_HHMMSS_xxxxxx`). |
| `start_time` | TEXT | **No** | Execution start timestamp (ISO-8601 UTC). |
| `end_time` | TEXT | Yes | Execution completion timestamp (ISO-8601 UTC). |
| `execution_duration`| REAL | Yes | Total runtime elapsed in seconds. |
| `pipeline_status` | TEXT | **No** | Run outcome: `SUCCESS`, `PARTIAL_SUCCESS`, or `FAILED`. |
| `input_count` | INTEGER | **No** | Total raw rows ingested. |
| `valid_count` | INTEGER | **No** | Total clean rows passed to target. |
| `rejected_count` | INTEGER | **No** | Total invalid rows quarantined. |
| `duplicate_count` | INTEGER | **No** | Total duplicate records detected. |
| `output_count` | INTEGER | **No** | Total records published to `clean_orders`. |
| `failure_reason` | TEXT | Yes | Exception message if pipeline suffered unhandled failure. |

---

## 6. Audit Entity: Order Change State History (`order_history`)
- **Storage Format**: SQLite Table
- **Granularity**: One record per state update or insertion for an order.
- **Table Name**: `order_history`

| Column Name | SQLite Data Type | Nullable | Key / Index | Description |
| :--- | :--- | :---: | :---: | :--- |
| `history_id` | INTEGER | **No** | **Primary Key** | Auto-incrementing primary key. |
| `order_id` | TEXT | **No** | Indexed | Order identifier. |
| `customer_id` | TEXT | **No** | None | Customer identifier. |
| `order_amount` | REAL | **No** | None | Order amount at this point in time. |
| `order_status` | TEXT | **No** | None | Order status at this point in time. |
| `order_date` | TEXT | **No** | None | Order placement timestamp. |
| `updated_at` | TEXT | **No** | None | Status update timestamp. |
| `run_id` | TEXT | **No** | None | Pipeline run that recorded this state. |
| `recorded_at` | TEXT | **No** | None | Database insertion timestamp (UTC). |
