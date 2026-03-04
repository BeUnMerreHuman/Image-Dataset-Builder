@echo off

echo === Setting up environment ===
pip install -r requirements.txt
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

echo === Opening .env file ===
start /wait notepad .env

echo === Navigating to src\ ===
cd /d src
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

echo === Opening keywords.txt ===
start /wait notepad keywords.txt

echo === Starting Snakemake Pipeline ===
snakemake --cores 1 --forceall
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

echo === Pipeline Finished Successfully! ===
pause