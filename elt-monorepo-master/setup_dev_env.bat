@echo off
setlocal ENABLEDELAYEDEXPANSION

echo =============================================
echo   ELT Monorepo Development Environment Setup
echo =============================================
echo.

set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"

echo [+] Script directory:
echo     %ROOT_DIR%
echo.

REM Capture Python version
for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v

echo Detected Python version: !PYVER!

REM Check if the version starts with 3.12.
echo !PYVER! | findstr /b "3.12." >nul
if errorlevel 1 (
    echo [!] ERROR: Python 3.12.x is required.
    pause
    ENDLOCAL
    exit /b 1
)

echo [+] Python version OK.
echo.

REM Jumps over function definitions and starts main logic
goto :main_start

:create_env
echo create_env
set TARGET=%1
set INSTALL_SHARED=%2
ENDLOCAL

if "%TARGET%"=="" (
    echo [!] No target directory passed to create_env.
    goto :eof
)

echo [+] Creating environment for: %TARGET%

if not exist "%ROOT_DIR%\%TARGET%\" (
    echo [!] ERROR: Folder not found: %ROOT_DIR%\%TARGET%
    goto :eof
)

pushd "%ROOT_DIR%\%TARGET%"

python -m venv .venv
if not exist ".venv\Scripts\python.exe" (
    echo [!] ERROR: Failed to create .venv in %TARGET%
    popd
    goto :eof
)

".venv\Scripts\python.exe" -m pip install --upgrade pip

if exist "requirements.txt" (
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
)

if "%INSTALL_SHARED%"=="yes" (
    ".venv\Scripts\python.exe" -m pip install -e "%ROOT_DIR%\shared_libs"
)

popd
echo Done: %TARGET%
echo.

goto :eof

:main_start
REM --- Main script execution starts here ---

REM Your variable setup (like the Python version check) should go here
REM ... (previous code you shared) ...

echo setup
call :create_env setup no
call :create_env pscsSourceLoader yes
call :create_env data-movers yes

REM The rest of your main script logic
call "%ROOT_DIR%\pscsSourceLoader\.venv\Scripts\activate.bat"

echo =============================================
echo [?] All virtual environments created correctly!
echo =============================================
pause
exit /b 0