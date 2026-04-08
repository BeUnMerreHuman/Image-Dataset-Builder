@echo off
setlocal

echo === Environment Check ===
where uv >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [FATAL] 'uv' package manager is not installed or not in PATH.
    echo To fix this, open PowerShell and run the following command:
    echo irm https://astral.sh/uv/install.ps1 ^| iex
    echo Once installed, restart this window and run the batch file again.
    pause
    exit /b 1
)

echo === Syncing Environment ===
uv sync
if %ERRORLEVEL% neq 0 (
    echo [ERROR] uv sync failed. Ensure Python 3.13+ is accessible.
    pause
    exit /b 1
)

echo === Fetching Camoufox Binaries ===
if not exist "%LOCALAPPDATA%\camoufox" (
    uv run python -m camoufox fetch
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to fetch Camoufox binaries.
        pause
        exit /b %ERRORLEVEL%
    )
) else (
    echo Camoufox binaries already present.
)

echo === Configuring Environment ===
if not exist .env (
    echo YANDEX_IMAGES_PER_KEYWORD = 100> .env
    echo PINTEREST_IMAGES_PER_KEYWORD = 50>> .env
    echo.>> .env
    echo #Optional>> .env
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

echo === Configuring Keywords ===
if not exist src mkdir src
pushd src
if not exist keywords.txt type nul > keywords.txt
start /wait notepad keywords.txt

echo === Starting Snakemake Pipeline ===
uv run snakemake --cores 1 --rerun-incomplete
set SNAKEMAKE_STATUS=%ERRORLEVEL%

echo === Executing Cleanup ===
if exist keywords.txt del keywords.txt
popd
if exist .env del .env

if %SNAKEMAKE_STATUS% neq 0 (
    echo === Pipeline encountered an error! ===
    pause
    exit /b %SNAKEMAKE_STATUS%
)

echo === Pipeline Finished Successfully! ===
pause