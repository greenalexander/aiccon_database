:: Must be created in the project's root folder. It automates the environment activation and runs the master pipeline.

@echo off
title AICCON Data Engine - Automated Refresh
echo ---------------------------------------------------------
echo AICCON INTERNAL DATABASE UPDATE
echo ---------------------------------------------------------

:: 1. Navigate to the project directory (Change this to your actual path)
cd /d "%~dp0"

:: 2. Check for Virtual Environment and activate
if exist .venv\Scripts\activate (
    call .venv\Scripts\activate
) else (
    echo [WARNING] Virtual environment (.venv) not found. Running with system Python.
)

:: 3. Run the Master Pipeline
echo [PROCESS] Starting full data refresh...
python run_pipeline.py

if %ERRORLEVEL% NEQ 0 (
    echo ---------------------------------------------------------
    echo [ERROR] The update failed. Check aiccon-data/pipeline_log.json
    echo ---------------------------------------------------------
    pause
    exit /b %ERRORLEVEL%
)

echo ---------------------------------------------------------
echo [SUCCESS] Database updated and verified. 
echo You can now refresh your PowerBI dashboard.
echo ---------------------------------------------------------
pause