@echo off
setlocal

cd /d "%~dp0"

echo Starting Ordered Mirror (sirali kanal aktarimi)...
echo.

py -3 "ordered_mirror.py"

echo.
pause
endlocal