@echo off
setlocal

set VENV_DIR=.venv
set PYTHON_VERSION=3.13.3

echo === Checking Environment ===

if not exist %VENV_DIR% (
    echo [INFO] Virtual environment not found. Creating with Python 3.13...
    py -3.13 -m venv %VENV_DIR%
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to create venv. Ensure Python 3.13 is installed.
        pause
        exit /b 1
    )
)

call %VENV_DIR%\Scripts\activate

for /f "tokens=2" %%v in ('python --version') do set CURRENT_VER=%%v
if "%CURRENT_VER%" neq "%PYTHON_VERSION%" (
    echo [WARNING] Version mismatch: Found %CURRENT_VER%, need %PYTHON_VERSION%.
    echo [INFO] Recreating environment...
    deactivate
    rmdir /s /q %VENV_DIR%
    py -3.13 -m venv %VENV_DIR%
    call %VENV_DIR%\Scripts\activate
)

echo === Setting up environment ===
pip install -r requirements.txt

if not exist "%LOCALAPPDATA%\camoufox" (
    echo Fetching Camoufox binaries...
    python -m camoufox fetch
    if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
) else (
    echo Camoufox binaries already present, skipping fetch.
)

echo === Creating and opening .env file ===
if not exist .env (
    echo YANDEX_IMAGES_PER_KEYWORD = 100> .env
    echo PINTEREST_IMAGES_PER_KEYWORD = 50>> .env
    echo.>> .env
    echo #Optinal>> .env
    echo BASE_DOWNLOAD_DIR = images>> .env
    echo DATASET_DIR = dataset>> .env
    echo HAMMING_THRESHOLD = 12>> .env
    echo MIN_WIDTH = 250>> .env
    echo MIN_HEIGHT = 250>> .env
    echo MAX_CONCURRENT_DOWNLOADS = 10>> .env      
    echo.>> .env
    echo #Extra>> .env
    echo #HEADLESS_MODE=True>> .env
    echo #MAX_RETRIES=3>> .env
    echo #MAX_SCROLLS=30>> .env
    echo #DELAY_BETWEEN_KEYWORDS=3>> .env
    echo #DELAY_BETWEEN_DOWNLOADS=0.5>> .env
)
start /wait notepad .env

echo === Navigating to src\ ===
cd /d src
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

echo === Creating and opening keywords.txt ===
start /wait notepad keywords.txt

echo === Starting Snakemake Pipeline ===
snakemake --cores 1 --rerun-incomplete
set SNAKEMAKE_STATUS=%ERRORLEVEL%

echo === Cleaning up temporary files ===
if exist keywords.txt del keywords.txt
cd ..
if exist .env del .env

if %SNAKEMAKE_STATUS% neq 0 (
    echo === Pipeline encountered an error! ===
    exit /b %SNAKEMAKE_STATUS%
)

echo === Pipeline Finished Successfully! ===
pause