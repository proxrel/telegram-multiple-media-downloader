@echo off
setlocal

cd /d "%~dp0"

echo Starting Telegram Batch Media Downloader...
echo.

py -3 "tbmd_fixed.py"

echo.
pause
endlocal