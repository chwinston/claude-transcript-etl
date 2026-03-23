# Claude Transcript ETL

Extract, store, and analyze every Claude Code and Cowork conversation on your machine. Automatic, incremental, queryable.

## What It Does

Every time you use Claude Code or Cowork, a JSONL file is written to your local filesystem. This tool:

1. **Discovers** those JSONL files automatically (Claude Code + Cowork)
2. **Parses** each conversation into structured data: messages, tool calls, token usage, costs, commands, files touched
3. **Stores** everything in a local database (SQLite by default — zero install)
4. **Runs automatically** every 30 minutes via your OS scheduler + on login
5. **Provides a query tool** for ad-hoc analysis and CSV export

No API keys. No cloud. Everything stays on your machine.

---

## Quick Start

### macOS / Linux

```bash
git clone https://github.com/YOUR_ORG/claude-transcript-etl.git
cd claude-transcript-etl
chmod +x setup.sh
./setup.sh
```

### Windows (PowerShell)

```powershell
git clone https://github.com/YOUR_ORG/claude-transcript-etl.git
cd claude-transcript-etl
.\setup.ps1
```

That's it. The setup script will:
- Verify Python 3.9+ is installed
- Run a full initial extraction
- Install a scheduled job (launchd on macOS, Task Scheduler on Windows, cron on Linux)

---

## Requirements

- **Python 3.9+** (the only hard requirement)
- **Claude Code** and/or **Cowork** installed (otherwise there's nothing to extract)
- **SQLite** comes bundled with Python — zero extra install
- **DuckDB** (optional) — `pip install duckdb` if you want faster analytical queries on large datasets

---

## Setup Options

### macOS / Linux

```bash
./setup.sh                        # SQLite (default), with scheduler
./setup.sh --backend duckdb       # Use DuckDB instead
./setup.sh --no-schedule          # Skip scheduler, run manually
./setup.sh --interval 60          # Run every 60 minutes instead of 30
```

### Windows

```powershell
.\setup.ps1                        # SQLite (default), with Task Scheduler
.\setup.ps1 -Backend duckdb       # Use DuckDB instead
.\setup.ps1 -NoSchedule           # Skip scheduler
.\setup.ps1 -Interval 60          # Every 60 minutes
```

---

## Configuration

All settings live in `config.yaml`. Edit this file to customize behavior.

### Database Backend

```yaml
# "sqlite" = zero-install, good for most users
# "duckdb" = faster for large datasets (1000+ sessions), requires pip install duckdb
backend: sqlite
```

### Source Paths

Claude Code and Cowork store transcripts in OS-specific locations. The defaults are auto-detected, but you can override them:

```yaml
sources:
  claude_code:
    enabled: true
    path: ~/.claude               # macOS/Linux default
    # path: C:\Users\YOU\.claude  # Windows (if non-standard)

  cowork:
    enabled: true
    path: auto                    # Auto-detects based on OS
    # path: ~/Library/Application Support/Claude/local-agent-mode-sessions  # macOS explicit
    # path: C:\Users\YOU\AppData\Roaming\Claude\local-agent-mode-sessions   # Windows explicit
```

### Where Claude Stores Transcripts (Reference)

| Platform | Claude Code | Cowork |
|----------|------------|--------|
| macOS | `~/.claude/projects/` | `~/Library/Application Support/Claude/local-agent-mode-sessions/` |
| Windows | `%USERPROFILE%\.claude\projects\` | `%APPDATA%\Claude\local-agent-mode-sessions\` |
| Linux | `~/.claude/projects/` | `~/.config/Claude/local-agent-mode-sessions/` |

### Token Pricing

Cost estimates use published Anthropic pricing. Update when prices change:

```yaml
pricing:
  claude-opus-4-6:
    input: 15.00      # per 1M tokens
    output: 75.00
    cache_read: 1.50
    cache_create: 18.75
  claude-sonnet-4-6:
    input: 3.00
    output: 15.00
    cache_read: 0.30
    cache_create: 3.75
  claude-haiku-4-5-20251001:
    input: 0.80
    output: 4.00
    cache_read: 0.08
    cache_create: 1.00
  default:            # Fallback for unknown models
    input: 3.00
    output: 15.00
    cache_read: 0.30
    cache_create: 3.75
```

### Schedule

```yaml
schedule:
  enabled: true
  interval_minutes: 30    # How often the ETL runs
  run_on_login: true      # Also run when you log in
```

---

## Usage

### Manual Commands

```bash
python3 etl.py                     # Incremental extraction (only new/changed files)
python3 etl.py --full              # Full re-extraction (drops and rebuilds)
python3 etl.py --stats             # Print database statistics
python3 etl.py --code-only         # Only Claude Code (skip Cowork)
python3 etl.py --cowork-only       # Only Cowork (skip Claude Code)
python3 etl.py --backend duckdb    # Override backend for this run
```

### Query Tool

```bash
python3 query.py --today                    # Today's sessions
python3 query.py --costs                    # Cost by project
python3 query.py --tools                    # Tool usage ranking
python3 query.py --models                   # Model usage breakdown
python3 query.py --sessions 7               # Last 7 days summary
python3 query.py "SELECT COUNT(*) FROM sessions"   # Any SQL
python3 query.py --today --export today.csv         # Export to CSV
```

### Querying Directly

The database is a standard SQLite (or DuckDB) file. Use any tool you like:

```bash
# SQLite CLI
sqlite3 transcripts.db "SELECT COUNT(*) FROM sessions"

# Python
python3 -c "
import sqlite3
con = sqlite3.connect('transcripts.db')
for row in con.execute('SELECT project_path, COUNT(*) FROM sessions GROUP BY 1 ORDER BY 2 DESC'):
    print(row)
"

# DuckDB (if using that backend)
python3 -c "
import duckdb
con = duckdb.connect('transcripts.duckdb', read_only=True)
print(con.execute('SELECT * FROM sessions ORDER BY started_at DESC LIMIT 5').fetchdf())
"
```

---

## Database Schema

### Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `sessions` | One row per conversation | `session_id`, `project_path`, `model`, `started_at`, `duration_seconds`, `estimated_total_cost_usd`, `source` |
| `messages` | Every user/assistant turn | `content_text`, `role`, `sequence_number`, `input_tokens`, `output_tokens` |
| `tool_calls` | Every tool invocation | `tool_name`, `file_path`, `command`, `has_error` |
| `command_invocations` | Slash commands & skills | `command_name`, `is_skill` |
| `files_referenced` | Files touched per session | `file_path`, `operation`, `reference_count` |
| `model_usage` | Per-model token aggregates | `model`, `message_count`, token columns |
| `cowork_sessions` | Cowork-specific metadata | `title`, `initial_message`, `user_selected_folders` |
| `etl_state` | Incremental tracking | `file_path`, `file_size_bytes`, `file_modified_at` |

### Useful Queries

```sql
-- Sessions by day with cost
SELECT date(started_at) as day, COUNT(*) as sessions,
       ROUND(SUM(estimated_total_cost_usd), 2) as cost,
       ROUND(SUM(duration_seconds)/3600.0, 1) as hours
FROM sessions WHERE is_agent = 0
GROUP BY day ORDER BY day DESC LIMIT 14;

-- Most expensive sessions
SELECT session_id, project_path, model,
       ROUND(estimated_total_cost_usd, 2) as cost,
       duration_seconds/60 as minutes
FROM sessions ORDER BY estimated_total_cost_usd DESC LIMIT 10;

-- Tool error rates
SELECT tool_name, COUNT(*) as total,
       COUNT(CASE WHEN has_error THEN 1 END) as errors,
       ROUND(COUNT(CASE WHEN has_error THEN 1 END) * 100.0 / COUNT(*), 1) as error_pct
FROM tool_calls GROUP BY tool_name ORDER BY total DESC;

-- What commands/skills are actually used
SELECT command_name, COUNT(*) as times_used, is_skill
FROM command_invocations GROUP BY command_name, is_skill ORDER BY times_used DESC;

-- Cache efficiency by model
SELECT model, COUNT(*) as sessions,
       ROUND(AVG(cache_hit_rate) * 100, 1) as avg_cache_pct
FROM sessions WHERE cache_hit_rate IS NOT NULL
GROUP BY model;

-- Files most frequently edited
SELECT file_path, SUM(reference_count) as refs
FROM files_referenced WHERE operation = 'edit'
GROUP BY file_path ORDER BY refs DESC LIMIT 20;
```

---

## How It Works (Technical)

### Data Flow

```
Claude Code / Cowork writes JSONL files
        ↓
  etl.py discovers files (walks known directories)
        ↓
  Checks etl_state table (file size + mtime) — skip unchanged files
        ↓
  Parses each JSONL line → extracts messages, tools, tokens, commands
        ↓
  Inserts into normalized tables (sessions → messages → tool_calls, etc.)
        ↓
  Updates etl_state to mark file as processed
```

### Incremental Logic

The ETL never re-parses files that haven't changed. It tracks each file's size and modification time in `etl_state`. On each run:
- New file? → Extract it
- File grew (conversation continued)? → Re-extract it
- Same size + same mtime? → Skip

A `--full` run drops everything and rebuilds from scratch. Takes 1-2 minutes for ~500 sessions.

### Scheduling

| OS | Mechanism | Installed By |
|----|-----------|--------------|
| macOS | launchd (LaunchAgent) | `setup.sh` generates plist from template |
| Windows | Task Scheduler | `setup.ps1` generates XML from template |
| Linux | cron | `setup.sh` adds crontab entry |

The scheduler runs `python3 etl.py` (no args = incremental mode). Logs go to `logs/`.

---

## Scheduler Management

### macOS

```bash
# Stop
launchctl unload ~/Library/LaunchAgents/com.claude-transcript-etl.plist

# Start
launchctl load ~/Library/LaunchAgents/com.claude-transcript-etl.plist

# Check status
launchctl list | grep claude

# View logs
tail -f logs/etl-stdout.log
```

### Windows

```powershell
# Stop
schtasks /Delete /TN ClaudeTranscriptETL /F

# Run now
schtasks /Run /TN ClaudeTranscriptETL

# View status
schtasks /Query /TN ClaudeTranscriptETL
```

### Linux

```bash
# Edit/remove
crontab -e    # find and remove the claude-transcript-etl lines

# View logs
tail -f logs/etl-cron.log
```

---

## Team Setup

For tracking AI usage across a team (managers, R&D leads):

1. Each developer runs setup on their own machine
2. To aggregate, each person exports their data: `python3 query.py --sessions 30 --export my-sessions.csv`
3. Collect CSVs and import into a shared spreadsheet or database

For automated team collection, configure each person's ETL to write to a shared network path or use the analysis agent (see below).

### Team Config Example

```yaml
analysis:
  users:
    - alice
    - bob
    - carol
  teams:
    frontend:
      - alice
      - bob
    backend:
      - carol
```

---

## Troubleshooting

**"No transcript files found"** — Claude Code hasn't been used yet, or the path is wrong. Check that `~/.claude/projects/` exists and contains `.jsonl` files.

**"duckdb not installed"** — Run `pip install duckdb` or switch to SQLite (the default).

**Windows path issues** — Use forward slashes or raw strings in config.yaml: `path: C:/Users/tony/.claude`

**Scheduler not running** — Check logs in `logs/`. On macOS, `launchctl list | grep claude`. On Windows, `schtasks /Query /TN ClaudeTranscriptETL`.

**Stale data** — The ETL runs incrementally. If conversations seem missing, run `python3 etl.py` manually. For a clean slate: `python3 etl.py --full`.

---

## File Structure

```
claude-transcript-etl/
├── etl.py                    # Main ETL script (cross-platform)
├── query.py                  # Query tool with shortcuts + CSV export
├── config.yaml               # All settings (copy and customize)
├── schema_sqlite.sql         # SQLite schema (default)
├── schema_duckdb.sql         # DuckDB schema (optional backend)
├── setup.sh                  # macOS/Linux setup
├── setup.ps1                 # Windows setup (PowerShell)
├── schedulers/
│   ├── launchd.plist.template    # macOS scheduler template
│   └── task-scheduler.xml.template  # Windows scheduler template
├── logs/                     # ETL execution logs (created by setup)
└── README.md                 # This file
```

---

## License

MIT
