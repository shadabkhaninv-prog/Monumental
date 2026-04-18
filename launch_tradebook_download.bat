@echo off
cd /d "%~dp0"
echo.
echo  =========================================
echo   Zerodha Console Tradebook Downloader
echo  =========================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

echo  Checking dependencies...
python -m pip install selenium --quiet

echo.
echo  Launching browser automation...
echo  Complete Zerodha login if prompted.
echo.

python download_zerodha_tradebook.py %*

echo.
pause
