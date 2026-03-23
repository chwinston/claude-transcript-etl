-- Claude Code Transcript Database Schema v3.0 (SQLite)
-- Portable schema: sessions -> messages -> tool_calls + files_referenced + command_invocations
-- Supports both Claude Code and Cowork session sources

-- Sessions table: one row per conversation session
CREATE TABLE IF NOT EXISTS sessions (
    session_id          TEXT PRIMARY KEY,       -- UUID from the JSONL filename
    project_path        TEXT,                   -- Original project path
    project_dir_name    TEXT,                   -- Directory name in .claude/projects/
    started_at          TEXT,                   -- ISO8601 timestamp (earliest message)
    ended_at            TEXT,                   -- ISO8601 timestamp (latest message)
    duration_seconds    INTEGER,               -- ended_at - started_at
    git_branch          TEXT,                   -- Git branch at session start
    cwd                 TEXT,                   -- Working directory
    claude_version      TEXT,                   -- Claude Code version string
    model               TEXT,                   -- Primary model used (e.g., claude-opus-4-6)
    slug                TEXT,                   -- Session slug (e.g., "ticklish-waddling-music")
    agent_id            TEXT,                   -- For agent sessions: the agent ID
    user_message_count  INTEGER DEFAULT 0,
    assistant_message_count INTEGER DEFAULT 0,
    tool_call_count     INTEGER DEFAULT 0,
    command_count       INTEGER DEFAULT 0,
    is_agent            INTEGER DEFAULT 0,     -- 1 for agent-*.jsonl files
    source              TEXT DEFAULT 'claude-code', -- 'claude-code' or 'cowork'
    -- Token usage aggregates
    total_input_tokens      INTEGER DEFAULT 0,
    total_output_tokens     INTEGER DEFAULT 0,
    total_cache_creation_tokens INTEGER DEFAULT 0,
    total_cache_read_tokens INTEGER DEFAULT 0,
    cache_hit_rate          REAL,
    -- Cost estimates
    estimated_input_cost_usd  REAL DEFAULT 0,
    estimated_output_cost_usd REAL DEFAULT 0,
    estimated_total_cost_usd  REAL DEFAULT 0,
    -- Turn timing
    total_turn_duration_ms  INTEGER DEFAULT 0,
    avg_turn_duration_ms    REAL,
    turn_count              INTEGER DEFAULT 0,
    -- File metadata
    file_size_bytes     INTEGER,
    file_path           TEXT,
    extracted_at        TEXT DEFAULT (datetime('now')),
    jsonl_modified_at   TEXT
);

-- Messages table: one row per user or assistant turn
CREATE TABLE IF NOT EXISTS messages (
    message_id          TEXT PRIMARY KEY,
    session_id          TEXT REFERENCES sessions(session_id),
    parent_uuid         TEXT,
    role                TEXT NOT NULL,          -- 'user', 'assistant', 'system'
    timestamp           TEXT NOT NULL,          -- ISO8601
    sequence_number     INTEGER,
    content_text        TEXT,
    has_thinking        INTEGER DEFAULT 0,
    thinking_text       TEXT,
    stop_reason         TEXT,
    model               TEXT,
    api_message_id      TEXT,
    is_sidechain        INTEGER DEFAULT 0,
    cwd                 TEXT,
    git_branch          TEXT,
    -- Token usage (assistant messages only)
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    cache_creation_input_tokens INTEGER,
    cache_read_input_tokens INTEGER,
    service_tier        TEXT,
    inference_geo       TEXT
);

-- Tool calls table: one row per tool invocation
CREATE TABLE IF NOT EXISTS tool_calls (
    tool_call_id        TEXT PRIMARY KEY,
    message_id          TEXT REFERENCES messages(message_id),
    session_id          TEXT REFERENCES sessions(session_id),
    tool_name           TEXT NOT NULL,
    input_summary       TEXT,
    timestamp           TEXT,
    file_path           TEXT,
    command             TEXT,
    description         TEXT,
    result_message_id   TEXT,
    has_error           INTEGER DEFAULT 0
);

-- Command invocations: slash commands and skills used during sessions
CREATE TABLE IF NOT EXISTS command_invocations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT REFERENCES sessions(session_id),
    message_id          TEXT,
    timestamp           TEXT,
    command_name        TEXT NOT NULL,
    command_args        TEXT,
    is_skill            INTEGER DEFAULT 0,
    UNIQUE(session_id, message_id, command_name)
);

-- Files referenced: deduplicated list of files touched per session
CREATE TABLE IF NOT EXISTS files_referenced (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT REFERENCES sessions(session_id),
    file_path           TEXT NOT NULL,
    operation           TEXT,
    first_referenced_at TEXT,
    reference_count     INTEGER DEFAULT 1,
    UNIQUE(session_id, file_path, operation)
);

-- Model usage: aggregated per-model stats across sessions
CREATE TABLE IF NOT EXISTS model_usage (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT REFERENCES sessions(session_id),
    model               TEXT NOT NULL,
    message_count       INTEGER DEFAULT 0,
    total_input_tokens  INTEGER DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    total_cache_creation_tokens INTEGER DEFAULT 0,
    total_cache_read_tokens INTEGER DEFAULT 0,
    UNIQUE(session_id, model)
);

-- Extraction state: tracks which files have been processed (for incremental ETL)
CREATE TABLE IF NOT EXISTS etl_state (
    file_path           TEXT PRIMARY KEY,
    file_size_bytes     INTEGER,
    file_modified_at    TEXT,
    extracted_at        TEXT DEFAULT (datetime('now'))
);

-- Cowork session metadata
CREATE TABLE IF NOT EXISTS cowork_sessions (
    cowork_session_id   TEXT PRIMARY KEY,
    jsonl_session_id    TEXT,
    title               TEXT,
    initial_message     TEXT,
    model               TEXT,
    created_at          TEXT,
    last_activity_at    TEXT,
    vm_process_name     TEXT,
    user_selected_folders TEXT,
    is_archived         INTEGER DEFAULT 0
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project_path);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at);
CREATE INDEX IF NOT EXISTS idx_cowork_jsonl ON cowork_sessions(jsonl_session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);
CREATE INDEX IF NOT EXISTS idx_tool_calls_session ON tool_calls(session_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_name ON tool_calls(tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_calls_file ON tool_calls(file_path);
CREATE INDEX IF NOT EXISTS idx_files_session ON files_referenced(session_id);
CREATE INDEX IF NOT EXISTS idx_commands_session ON command_invocations(session_id);
CREATE INDEX IF NOT EXISTS idx_commands_name ON command_invocations(command_name);
CREATE INDEX IF NOT EXISTS idx_model_usage_session ON model_usage(session_id);
CREATE INDEX IF NOT EXISTS idx_model_usage_model ON model_usage(model);
