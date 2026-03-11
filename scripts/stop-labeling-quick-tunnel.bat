@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%stop-labeling-quick-tunnel.ps1"

if not exist "%PS1%" (
  echo Stop script not found: "%PS1%"
  exit /b 1
)

powershell -ExecutionPolicy Bypass -File "%PS1%"
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo.
  echo Stop launcher failed with exit code %EC%.
  pause
)
exit /b %EC%
