#!/usr/bin/env bash
set -e

echo "====================================================================="
echo " E-Commerce Data Quality Pipeline: Complete Automated Execution Run"
echo "====================================================================="
echo ""

echo "[1/3] Running Automated Test Suite (31 Tests)..."
python3 -m pytest -v
echo ""

echo "[2/3] Executing Main Ingestion, Validation and Loading Pipeline..."
python3 src/pipeline.py --generate --records 100
echo ""

echo "[3/3] Testing Idempotency (Safe Rerun)..."
python3 src/pipeline.py
echo ""

echo "====================================================================="
echo " EXECUTION COMPLETED SUCCESSFULLY!"
echo " - Clean Orders:    output/clean_orders.csv"
echo " - Quarantined:     output/rejected_records.csv"
echo " - SQLite Database: output/ecommerce.db"
echo " - Quality Report:  reports/quality_report.json"
echo " - Test Evidence:   reports/test_execution_report.txt"
echo "====================================================================="
