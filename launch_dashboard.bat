@echo off
cd /d "%~dp0"
echo.
echo  =========================================
echo   Institutional Picks - Fire Dashboard
echo  =========================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Please install Python 3.9+
    pause & exit /b 1
)

:: Install dependencies if missing
echo  Checking dependencies...
python -m pip install streamlit plotly kiteconnect pandas openpyxl --quiet

echo.
echo  Launching dashboard at http://localhost:8501
echo  Press Ctrl+C in this window to stop.
echo.

python -m streamlit run ip_dashboard.py --server.headless false --browser.gatherUsageStats false

pause
