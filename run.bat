@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ===== osu! Lost Scores Analyzer =====
echo Checking required components...

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed. Please install Python 3.8 or higher.
    echo Visit https://www.python.org/downloads/ to download.
    pause
    exit /b 1
)

for /f "tokens=2" %%I in ('python --version 2^>^&1') do set pyver=%%I
for /f "tokens=1,2 delims=." %%a in ("!pyver!") do (
    set major=%%a
    set minor=%%b
)

if !major! lss 3 (
    echo Python 3.8 or higher is required. You have Python !pyver!.
    pause
    exit /b 1
)

if !major! equ 3 if !minor! lss 8 (
    echo Python 3.8 or higher is required. You have Python !pyver!.
    pause
    exit /b 1
)

if not exist ".venv\Scripts\activate.bat" (
    echo Creating virtual environment...
    python -m venv .venv
    if %errorlevel% neq 0 (
        echo Error creating virtual environment.
        pause
        exit /b 1
    )
)

call .venv\Scripts\activate.bat

if exist "requirements.txt" (
    echo Installing dependencies...
    pip install -r requirements.txt -q
    if %errorlevel% neq 0 (
        echo Error installing dependencies.
        pause
        exit /b 1
    ) else (
        echo All dependencies installed successfully.
    )
) else (
    echo requirements.txt file not found. Cannot install dependencies.
    pause
    exit /b 1
)

mkdir src\cache 2>nul
mkdir src\csv 2>nul
mkdir src\maps 2>nul
mkdir src\results 2>nul
mkdir src\config 2>nul

if exist "src\.env" (
    del /f "src\.env"
    echo Deleted incorrect .env file in src\.env
)
if exist "src\project\.env" (
    del /f "src\project\.env"
    echo Deleted incorrect .env file in src\project\.env
)

set ENV_FILE=.env
if not exist "%ENV_FILE%" (
    echo CLIENT_ID=default_client_id> "%ENV_FILE%"
    echo CLIENT_SECRET=default_client_secret>> "%ENV_FILE%"
    echo DB_FILE=../cache/beatmap_info.db>> "%ENV_FILE%"
    echo CUTOFF_DATE=1719619200>> "%ENV_FILE%"
    echo Created .env file in root directory
)

set "DOTENV_PATH=%CD%\.env"
echo DOTENV_PATH set to: %DOTENV_PATH%

echo Launching osu! Lost Scores Analyzer...

echo API keys can be configured within the application

cd src\project
set "DOTENV_PATH=%DOTENV_PATH%"
python main.py
set EXIT_CODE=%errorlevel%
cd ..\..

call .venv\Scripts\deactivate.bat

if %EXIT_CODE% neq 0 (
    echo Program ended with an error (code %EXIT_CODE%).
) else (
    echo Program completed successfully.
)

pause
exit /b %EXIT_CODE%