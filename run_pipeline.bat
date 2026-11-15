@echo off
echo =====================================================================
echo  E-Commerce Data Quality Pipeline: Complete Automated Execution Run
echo =====================================================================
echo.

echo [1/3] Running Automated Test Suite (31 Tests)...
python -m pytest -v
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Test suite failed! Exiting.
    exit /b %ERRORLEVEL%
)
echo.

echo [2/3] Executing Main Ingestion, Validation and Loading Pipeline...
python src/pipeline.py --generate --records 100
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Pipeline execution failed! Exiting.
    exit /b %ERRORLEVEL%
)
echo.

echo [3/3] Testing Idempotency (Safe Rerun)...
python src/pipeline.py
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Idempotent rerun failed! Exiting.
    exit /b %ERRORLEVEL%
)
echo.

echo =====================================================================
echo  EXECUTION COMPLETED SUCCESSFULLY!
echo  - Clean Orders:    output\clean_orders.csv
echo  - Quarantined:     output\rejected_records.csv
echo  - SQLite Database: output\ecommerce.db
echo  - Quality Report:  reports\quality_report.json
echo  - Test Evidence:   reports\test_execution_report.txt
echo =====================================================================
pause
