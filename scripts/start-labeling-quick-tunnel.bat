@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%start-labeling-quick-tunnel.ps1"

if not exist "%PS1%" (
  echo Start script not found: "%PS1%"
  exit /b 1
)

if not exist "%USERPROFILE%\.labeling_postgres_dsn.txt" (
  echo Missing Postgres DSN.
  echo.
  echo Option 1 ^(recommended^): save your DSN in:
  echo   %USERPROFILE%\.labeling_postgres_dsn.txt
  echo.
  echo Example file content ^(single line^):
  echo postgresql://postgres:YOURPASSWORD@localhost:5432/RGN_Database
  echo.
  pause
  exit /b 1
)

powershell -ExecutionPolicy Bypass -File "%PS1%" -StorageBackend postgres %*
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo.
  echo Start launcher failed with exit code %EC%.
  pause
)
exit /b %EC%
