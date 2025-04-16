@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ===== osu! Lost Scores Analyzer =====
echo Проверка необходимых компонентов...

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python не установлен. Пожалуйста, установите Python 3.8 или выше.
    echo Откройте https://www.python.org/downloads/ для загрузки.
    pause
    exit /b 1
)

for /f "tokens=2" %%I in ('python --version 2^>^&1') do set pyver=%%I
for /f "tokens=1,2 delims=." %%a in ("!pyver!") do (
    set major=%%a
    set minor=%%b
)

if !major! lss 3 (
    echo Требуется Python 3.8 или выше. У вас установлен Python !pyver!.
    pause
    exit /b 1
)

if !major! equ 3 if !minor! lss 8 (
    echo Требуется Python 3.8 или выше. У вас установлен Python !pyver!.
    pause
    exit /b 1
)

if not exist ".venv\Scripts\activate.bat" (
    echo Создание виртуального окружения...
    python -m venv .venv
    if %errorlevel% neq 0 (
        echo Ошибка при создании виртуального окружения.
        pause
        exit /b 1
    )
)

call .venv\Scripts\activate.bat

if exist "requirements.txt" (
    echo Установка зависимостей...
    pip install -r requirements.txt -q
    if %errorlevel% neq 0 (
        echo Ошибка при установке зависимостей.
        pause
        exit /b 1
    ) else (
        echo Все зависимости установлены успешно.
    )
) else (
    echo Файл requirements.txt не найден. Невозможно установить зависимости.
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
    echo Удален ошибочный .env файл в src\.env
)
if exist "src\project\.env" (
    del /f "src\project\.env"
    echo Удален ошибочный .env файл в src\project\.env
)

set ENV_FILE=.env
if not exist "%ENV_FILE%" (
    echo CLIENT_ID=default_client_id> "%ENV_FILE%"
    echo CLIENT_SECRET=default_client_secret>> "%ENV_FILE%"
    echo DB_FILE=../cache/beatmap_info.db>> "%ENV_FILE%"
    echo CUTOFF_DATE=1719619200>> "%ENV_FILE%"
    echo Создан .env файл в корневой директории
)

set "DOTENV_PATH=%CD%\.env"
echo DOTENV_PATH установлен в: %DOTENV_PATH%

echo Запуск osu! Lost Scores Analyzer...

if not exist "src\config\api_keys.json" (
    set /p OSU_CLIENT_ID=Введите Client ID osu!:
    set /p OSU_CLIENT_SECRET=Введите Client Secret osu!:
)

cd src\project
set "OSU_CLIENT_ID=%OSU_CLIENT_ID%"
set "OSU_CLIENT_SECRET=%OSU_CLIENT_SECRET%"
set "DOTENV_PATH=%DOTENV_PATH%"
python main.py
set EXIT_CODE=%errorlevel%
cd ..\..

call .venv\Scripts\deactivate.bat

if %EXIT_CODE% neq 0 (
    echo Программа завершилась с ошибкой (код %EXIT_CODE%).
) else (
    echo Программа успешно завершена.
)

pause
exit /b %EXIT_CODE%