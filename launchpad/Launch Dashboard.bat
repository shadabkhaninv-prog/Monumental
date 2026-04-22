@echo off
REM =========================================================
REM  Localhost Launchpad  ->  http://localhost:9000
REM  Single dashboard to start / stop / open your localhost
REM  apps. No application code is modified.
REM =========================================================

title Localhost Launchpad (port 9000)

cd /d "%~dp0"

set "LP_PORT=9000"

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found on PATH.
    pause
    exit /b 1
)

REM Ensure Flask is available (quiet install; no-op if already present)
python -c "import flask" >nul 2>&1
if errorlevel 1 (
    echo Installing Flask...
    python -m pip install flask --quiet
)

REM Open the dashboard in the default browser a moment after boot
start "" powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep -Seconds 2; Start-Process 'http://localhost:%LP_PORT%'"

REM Run the dashboard in this console (Ctrl+C to stop)
set "LAUNCHPAD_PORT=%LP_PORT%"
python launchpad_server.py

echo.
echo Launchpad stopped.
pause
