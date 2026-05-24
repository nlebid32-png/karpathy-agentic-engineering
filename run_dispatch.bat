@echo off
cd /d "%~dp0"
echo Starting Dispatch server...
python dispatch_server.py
pause
