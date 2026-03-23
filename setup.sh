#!/bin/bash
# Claude Transcript ETL - Cross-Platform Setup (macOS/Linux)
# Run once: ./setup.sh
# Options:
#   --backend duckdb    Use DuckDB instead of SQLite
#   --no-schedule       Skip scheduler installation
#   --interval MINS     Set schedule interval (default: 30)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND="sqlite"
INSTALL_SCHEDULE=true
INTERVAL=30

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --backend) BACKEND="$2"; shift 2 ;;
        --no-schedule) INSTALL_SCHEDULE=false; shift ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo ""
echo "Claude Transcript ETL Setup"
echo "==========================="
echo "  Backend:  $BACKEND"
echo "  Schedule: $([ "$INSTALL_SCHEDULE" = true ] && echo "every ${INTERVAL}min + on login" || echo "disabled")"
echo ""

# 1. Check Python
PYTHON=$(command -v python3 || command -v python)
if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3 not found. Install Python 3.9+ first."
    exit 1
fi
echo "1. Python: $PYTHON ($($PYTHON --version 2>&1))"

# 2. Install dependencies
echo "2. Installing dependencies..."
if [ "$BACKEND" = "duckdb" ]; then
    $PYTHON -m pip install duckdb --break-system-packages --quiet 2>/dev/null || \
    $PYTHON -m pip install duckdb --quiet 2>/dev/null || \
    pip3 install duckdb --quiet
    echo "   duckdb installed"
fi
# PyYAML is optional but recommended
$PYTHON -m pip install pyyaml --break-system-packages --quiet 2>/dev/null || \
$PYTHON -m pip install pyyaml --quiet 2>/dev/null || true
echo "   pyyaml installed (optional, for config.yaml)"

# 3. Create directories
echo "3. Creating directories..."
mkdir -p "$SCRIPT_DIR/logs"
echo "   logs/ created"

# 4. Run initial extraction
echo "4. Running initial extraction..."
$PYTHON "$SCRIPT_DIR/etl.py" --full --backend "$BACKEND"
echo "   Initial extraction complete"

# 5. Install scheduler (macOS only for launchd, Linux uses cron)
if [ "$INSTALL_SCHEDULE" = true ]; then
    echo "5. Installing scheduler..."
    OS="$(uname -s)"

    if [ "$OS" = "Darwin" ]; then
        # macOS: launchd
        PLIST_NAME="com.claude-transcript-etl.plist"
        PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"
        INTERVAL_SEC=$((INTERVAL * 60))

        # Generate plist from template
        sed -e "s|{{PYTHON_PATH}}|$PYTHON|g" \
            -e "s|{{ETL_SCRIPT_PATH}}|$SCRIPT_DIR/etl.py|g" \
            -e "s|{{INTERVAL_SECONDS}}|$INTERVAL_SEC|g" \
            -e "s|{{RUN_AT_LOAD}}|true|g" \
            -e "s|{{LOG_DIR}}|$SCRIPT_DIR/logs|g" \
            -e "s|{{WORKING_DIR}}|$SCRIPT_DIR|g" \
            "$SCRIPT_DIR/schedulers/launchd.plist.template" > "/tmp/$PLIST_NAME"

        if [ -f "$PLIST_DST" ]; then
            launchctl unload "$PLIST_DST" 2>/dev/null || true
        fi
        cp "/tmp/$PLIST_NAME" "$PLIST_DST"
        launchctl load "$PLIST_DST"
        echo "   macOS LaunchAgent installed ($PLIST_DST)"
        echo "   To stop:    launchctl unload $PLIST_DST"
        echo "   To restart: launchctl load $PLIST_DST"

    elif [ "$OS" = "Linux" ]; then
        # Linux: cron
        CRON_CMD="*/$INTERVAL * * * * $PYTHON $SCRIPT_DIR/etl.py >> $SCRIPT_DIR/logs/etl-cron.log 2>&1"
        (crontab -l 2>/dev/null | grep -v "claude-transcript-etl"; echo "# claude-transcript-etl"; echo "$CRON_CMD") | crontab -
        echo "   Cron job installed (every ${INTERVAL}min)"
        echo "   To stop: crontab -e and remove the claude-transcript-etl lines"
    fi
else
    echo "5. Scheduler: skipped (--no-schedule)"
fi

echo ""
echo "Setup complete!"
echo ""
echo "  Database: $SCRIPT_DIR/transcripts.$([ "$BACKEND" = "duckdb" ] && echo "duckdb" || echo "db")"
echo "  Logs:     $SCRIPT_DIR/logs/"
echo ""
echo "Commands:"
echo "  $PYTHON $SCRIPT_DIR/etl.py --stats       # View stats"
echo "  $PYTHON $SCRIPT_DIR/etl.py               # Manual incremental run"
echo "  $PYTHON $SCRIPT_DIR/etl.py --full         # Full re-extraction"
echo ""
