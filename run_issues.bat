@echo off
cd /d "%~dp0"
echo [Issue Runner] Starting...
python issue_runner.py %*
pause
