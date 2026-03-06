@echo off
title DISCODATA Explorer
echo ========================================
echo   DISCODATA Explorer - Starting...
echo ========================================
echo.

cd /d "%~dp0"

echo Starting server on http://localhost:5000
echo.

:: Open browser after 2 seconds
start "" cmd /c "timeout /t 2 >nul && start http://localhost:5000"

:: Start server
python -m waitress --host=127.0.0.1 --port=5000 app:app

pause
