@echo off
cd /d "%~dp0"
echo.
echo  =========================================
echo   Kite Current-Day Trade Downloader
echo  =========================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

echo  Running Kite same-day trade download...
echo.

python download_today_kite_trades.py %*

echo.
pause
