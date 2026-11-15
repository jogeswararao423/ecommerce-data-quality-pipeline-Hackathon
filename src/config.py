import os
from pathlib import Path

# Base Paths using pathlib for multi-platform support
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
GENERATED_DATA_DIR = DATA_DIR / "generated"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"

# Database Configuration (SQLite)
DEFAULT_DB_PATH = OUTPUT_DIR / "ecommerce.db"

# Artifact Paths
DEFAULT_LOG_PATH = LOGS_DIR / "pipeline.log"
QUALITY_REPORT_PATH = REPORTS_DIR / "quality_report.json"
OUTPUT_QUALITY_REPORT_PATH = OUTPUT_DIR / "quality_report.json"
PERFORMANCE_REPORT_PATH = REPORTS_DIR / "performance_report.txt"
CLEAN_ORDERS_CSV = OUTPUT_DIR / "clean_orders.csv"
REJECTED_RECORDS_CSV = OUTPUT_DIR / "rejected_records.csv"

# Source File Paths
DEFAULT_CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
DEFAULT_ORDERS_FILE = RAW_DATA_DIR / "orders.csv"

# Validation & Business Constraints
RANDOM_SEED = 42

ALLOWED_ORDER_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"}
HIGH_VALUE_THRESHOLD = 1500.00
MIN_ORDER_AMOUNT = 0.01

CUSTOMER_REQUIRED_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "city",
    "created_at"
]

ORDER_REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_amount",
    "order_status",
    "order_date",
    "updated_at"
]

EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"
