# Claude Transcript ETL - Windows Setup (PowerShell)
# Run once: .\setup.ps1
# Options:
#   -Backend duckdb     Use DuckDB instead of SQLite
#   -NoSchedule         Skip scheduler installation
#   -Interval 30        Set schedule interval in minutes (default: 30)

param(
    [string]$Backend = "sqlite",
    [switch]$NoSchedule,
    [int]$Interval = 30
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

Write-Host ""
Write-Host "Claude Transcript ETL Setup" -ForegroundColor Cyan
Write-Host "==========================="
Write-Host "  Backend:  $Backend"
Write-Host "  Schedule: $(if ($NoSchedule) { 'disabled' } else { "every ${Interval}min + on login" })"
Write-Host ""

# 1. Check Python
$Python = Get-Command python -ErrorAction SilentlyContinue
if (-not $Python) {
    $Python = Get-Command python3 -ErrorAction SilentlyContinue
}
if (-not $Python) {
    Write-Host "ERROR: Python 3 not found. Install Python 3.9+ first." -ForegroundColor Red
    Write-Host "Download from: https://www.python.org/downloads/"
    exit 1
}
$PythonPath = $Python.Source
Write-Host "1. Python: $PythonPath"

# 2. Install dependencies
Write-Host "2. Installing dependencies..."
if ($Backend -eq "duckdb") {
    & $PythonPath -m pip install duckdb --quiet 2>$null
    Write-Host "   duckdb installed"
}
try {
    & $PythonPath -m pip install pyyaml --quiet 2>$null
    Write-Host "   pyyaml installed (optional, for config.yaml)"
} catch {}

# 3. Create directories
Write-Host "3. Creating directories..."
New-Item -ItemType Directory -Path "$ScriptDir\logs" -Force | Out-Null
Write-Host "   logs\ created"

# 4. Run initial extraction
Write-Host "4. Running initial extraction..."
& $PythonPath "$ScriptDir\etl.py" --full --backend $Backend
Write-Host "   Initial extraction complete"

# 5. Install scheduler
if (-not $NoSchedule) {
    Write-Host "5. Installing scheduler..."

    $TaskName = "ClaudeTranscriptETL"

    # Test if we have admin privileges for Task Scheduler
    $IsAdmin = $false
    try {
        $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
        $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)
        $IsAdmin = $Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    } catch {}

    if ($IsAdmin) {
        # Admin path: use Task Scheduler (proper quoting for paths with spaces)
        Write-Host "   Admin detected - using Task Scheduler"

        $Template = Get-Content "$ScriptDir\schedulers\task-scheduler.xml.template" -Raw
        $Template = $Template -replace "{{PYTHON_PATH}}", [System.Security.SecurityElement]::Escape($PythonPath)
        $Template = $Template -replace "{{ETL_SCRIPT_PATH}}", [System.Security.SecurityElement]::Escape("$ScriptDir\etl.py")
        $Template = $Template -replace "{{INTERVAL_MINUTES}}", $Interval
        $Template = $Template -replace "{{RUN_AT_LOGIN}}", "true"
        $Template = $Template -replace "{{WORKING_DIR}}", [System.Security.SecurityElement]::Escape($ScriptDir)

        $TempXml = "$env:TEMP\claude-etl-task.xml"
        $Template | Out-File -FilePath $TempXml -Encoding unicode

        schtasks /Delete /TN "`"$TaskName`"" /F 2>$null
        schtasks /Create /TN "`"$TaskName`"" /XML "`"$TempXml`"" /F
        Remove-Item $TempXml -ErrorAction SilentlyContinue

        Write-Host "   Task Scheduler job installed ($TaskName)" -ForegroundColor Green
        Write-Host "   To stop:  schtasks /Delete /TN `"$TaskName`" /F"
        Write-Host "   To view:  schtasks /Query /TN `"$TaskName`""
    } else {
        # Non-admin path: use VBS startup loop (no elevation required)
        Write-Host "   No admin privileges - using startup folder instead"

        $StartupDir = [Environment]::GetFolderPath("Startup")
        $VbsSource = "$ScriptDir\etl-loop.vbs"
        $VbsLauncher = "$StartupDir\ClaudeTranscriptETL.vbs"

        if (-not (Test-Path $VbsSource)) {
            Write-Host "   ERROR: etl-loop.vbs not found in $ScriptDir" -ForegroundColor Red
            Write-Host "   The VBS loop script should be included in the repo."
            exit 1
        }

        # Write a launcher in the Startup folder that invokes the repo's VBS loop
        $LauncherContent = "' Startup launcher - runs Claude Transcript ETL loop silently`r`n"
        $LauncherContent += "CreateObject(""WScript.Shell"").Run """"""wscript.exe"""""" """"""$VbsSource""""""`", 0, False"
        Set-Content -Path $VbsLauncher -Value $LauncherContent -Encoding ASCII

        # Start it now for this session
        Start-Process wscript.exe -ArgumentList "`"$VbsSource`""

        Write-Host "   Startup script installed" -ForegroundColor Green
        Write-Host "   Loop: $VbsSource (runs every ${Interval}min)"
        Write-Host "   Startup launcher: $VbsLauncher"
        Write-Host "   To stop now:  taskkill /IM wscript.exe /F"
        Write-Host "   To disable:   Remove-Item `"$VbsLauncher`""
    }
} else {
    Write-Host "5. Scheduler: skipped (-NoSchedule)"
}

Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
Write-Host ""
$DbExt = if ($Backend -eq "duckdb") { "duckdb" } else { "db" }
Write-Host "  Database: $ScriptDir\transcripts.$DbExt"
Write-Host "  Logs:     $ScriptDir\logs\"
Write-Host ""
Write-Host "Commands:"
Write-Host "  $PythonPath `"$ScriptDir\etl.py`" --stats       # View stats"
Write-Host "  $PythonPath `"$ScriptDir\etl.py`"               # Manual incremental run"
Write-Host "  $PythonPath `"$ScriptDir\etl.py`" --full         # Full re-extraction"
Write-Host ""
