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
        echo Ensure you have Microsoft Visual C++ Redistributable installed and try again.
        pause
        exit /b %ERRORLEVEL%
    )
) else (
    echo Camoufox binaries already present.
)

echo === Configuring Environment and Keywords ===
start /wait notepad .env
cd src
start /wait notepad keywords.txt

echo === Starting Snakemake Pipeline ===
uv run snakemake --cores 1 --rerun-incomplete
set SNAKEMAKE_STATUS=%ERRORLEVEL%

if %SNAKEMAKE_STATUS% neq 0 (
    echo === Pipeline encountered an error! ===
    pause
    exit /b %SNAKEMAKE_STATUS%
)

echo === Pipeline Finished Successfully! ===
pause