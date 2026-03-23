-- Claude Code Transcript Database Schema v2.1
-- DuckDB normalized schema: sessions → messages → tool_calls + files_referenced + command_invocations
-- Now supports both Claude Code and Cowork session sources

-- Sessions table: one row per conversation session
CREATE TABLE IF NOT EXISTS sessions (
    session_id          VARCHAR PRIMARY KEY,   -- UUID from the JSONL filename
    project_path        VARCHAR,               -- Original project path (e.g., /Users/chasewinston/Projects/M-A-tool)
    project_dir_name    VARCHAR,               -- Directory name in .claude/projects/
    started_at          TIMESTAMP,             -- Earliest message timestamp
    ended_at            TIMESTAMP,             -- Latest message timestamp
    duration_seconds    INTEGER,               -- ended_at - started_at
    git_branch          VARCHAR,               -- Git branch at session start
    cwd                 VARCHAR,               -- Working directory
    claude_version      VARCHAR,               -- Claude Code version string
    model               VARCHAR,               -- Primary model used (e.g., claude-opus-4-6)
    slug                VARCHAR,               -- Session slug (e.g., "ticklish-waddling-music")
    agent_id            VARCHAR,               -- For agent sessions: the agent ID
    user_message_count  INTEGER DEFAULT 0,
    assistant_message_count INTEGER DEFAULT 0,
    tool_call_count     INTEGER DEFAULT 0,
    command_count       INTEGER DEFAULT 0,     -- Number of slash commands invoked
    is_agent            BOOLEAN DEFAULT FALSE, -- True for agent-*.jsonl files
    source              VARCHAR DEFAULT 'claude-code', -- 'claude-code' or 'cowork'
    -- Token usage aggregates
    total_input_tokens      BIGINT DEFAULT 0,
    total_output_tokens     BIGINT DEFAULT 0,
    total_cache_creation_tokens BIGINT DEFAULT 0,
    total_cache_read_tokens BIGINT DEFAULT 0,
    cache_hit_rate          DOUBLE,            -- cache_read / (cache_read + cache_creation + input)
    -- Cost estimates (based on published pricing, may need updating)
    estimated_input_cost_usd  DOUBLE DEFAULT 0,
    estimated_output_cost_usd DOUBLE DEFAULT 0,
    estimated_total_cost_usd  DOUBLE DEFAULT 0,
    -- Turn timing
    total_turn_duration_ms  BIGINT DEFAULT 0,  -- Sum of all turn durations
    avg_turn_duration_ms    DOUBLE,
    turn_count              INTEGER DEFAULT 0,
    -- File metadata
    file_size_bytes     BIGINT,                -- Size of the source JSONL file
    file_path           VARCHAR,               -- Full path to source JSONL file
    extracted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    jsonl_modified_at   TIMESTAMP              -- File mtime, used for incremental updates
);

-- Messages table: one row per user or assistant turn
CREATE TABLE IF NOT EXISTS messages (
    message_id          VARCHAR PRIMARY KEY,   -- uuid from the JSONL line
    session_id          VARCHAR REFERENCES sessions(session_id),
    parent_uuid         VARCHAR,               -- Parent message uuid (for threading)
    role                VARCHAR NOT NULL,       -- 'user', 'assistant', 'system'
    timestamp           TIMESTAMP NOT NULL,
    sequence_number     INTEGER,               -- Order within the session (0-indexed)
    content_text        TEXT,                   -- Plain text content (user message or assistant text blocks concatenated)
    has_thinking        BOOLEAN DEFAULT FALSE,  -- Whether assistant response included thinking
    thinking_text       TEXT,                   -- Extended thinking content (optional, can be large)
    stop_reason         VARCHAR,               -- Assistant stop reason (end_turn, tool_use, etc.)
    model               VARCHAR,               -- Model for this specific response
    api_message_id      VARCHAR,               -- Anthropic API message ID (msg_...)
    is_sidechain        BOOLEAN DEFAULT FALSE,
    cwd                 VARCHAR,
    git_branch          VARCHAR,
    -- Token usage (assistant messages only)
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    cache_creation_input_tokens INTEGER,
    cache_read_input_tokens INTEGER,
    service_tier        VARCHAR,               -- e.g., "standard"
    inference_geo       VARCHAR                -- e.g., "us", "not_available"
);

-- Tool calls table: one row per tool invocation
CREATE TABLE IF NOT EXISTS tool_calls (
    tool_call_id        VARCHAR PRIMARY KEY,   -- tool_use id from content block
    message_id          VARCHAR REFERENCES messages(message_id),  -- The assistant message containing this tool call
    session_id          VARCHAR REFERENCES sessions(session_id),
    tool_name           VARCHAR NOT NULL,      -- e.g., Bash, Read, Write, Edit, Glob, Grep, etc.
    input_summary       TEXT,                  -- Key input parameters (truncated for storage)
    timestamp           TIMESTAMP,
    -- Specific common fields extracted for easy querying:
    file_path           VARCHAR,               -- For Read/Write/Edit: the file being operated on
    command             VARCHAR,               -- For Bash: the command executed
    description         VARCHAR,               -- For Bash: the description field
    -- Result tracking
    result_message_id   VARCHAR,               -- The user message containing the tool_result
    has_error           BOOLEAN DEFAULT FALSE
);

-- Command invocations: slash commands and skills used during sessions
CREATE TABLE IF NOT EXISTS command_invocations (
    id                  INTEGER PRIMARY KEY,
    session_id          VARCHAR REFERENCES sessions(session_id),
    message_id          VARCHAR,               -- The user message that invoked the command
    timestamp           TIMESTAMP,
    command_name        VARCHAR NOT NULL,       -- e.g., "infrastructure:extract", "daily-summary"
    command_args        TEXT,                   -- Arguments passed to the command
    is_skill            BOOLEAN DEFAULT FALSE,  -- Whether this was a skill vs a slash command
    UNIQUE(session_id, message_id, command_name)
);

-- Files referenced: deduplicated list of files touched per session
CREATE TABLE IF NOT EXISTS files_referenced (
    id                  INTEGER PRIMARY KEY,   -- Auto-increment via sequence
    session_id          VARCHAR REFERENCES sessions(session_id),
    file_path           VARCHAR NOT NULL,
    operation           VARCHAR,               -- 'read', 'write', 'edit', 'glob', 'grep', 'bash'
    first_referenced_at TIMESTAMP,
    reference_count     INTEGER DEFAULT 1,
    UNIQUE(session_id, file_path, operation)
);

-- Model usage: aggregated per-model stats across sessions
CREATE TABLE IF NOT EXISTS model_usage (
    id                  INTEGER PRIMARY KEY,
    session_id          VARCHAR REFERENCES sessions(session_id),
    model               VARCHAR NOT NULL,
    message_count       INTEGER DEFAULT 0,
    total_input_tokens  BIGINT DEFAULT 0,
    total_output_tokens BIGINT DEFAULT 0,
    total_cache_creation_tokens BIGINT DEFAULT 0,
    total_cache_read_tokens BIGINT DEFAULT 0,
    UNIQUE(session_id, model)
);

-- Extraction state: tracks which files have been processed (for incremental ETL)
CREATE TABLE IF NOT EXISTS etl_state (
    file_path           VARCHAR PRIMARY KEY,
    file_size_bytes     BIGINT,
    file_modified_at    TIMESTAMP,
    extracted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cowork session metadata: rich metadata from the companion .json files
CREATE TABLE IF NOT EXISTS cowork_sessions (
    cowork_session_id   VARCHAR PRIMARY KEY,   -- e.g., "local_e8daeba4-..."
    jsonl_session_id    VARCHAR,               -- Links to sessions.session_id (the CLI session UUID)
    title               VARCHAR,               -- Human-readable session title
    initial_message     TEXT,                   -- The user's first message (often a detailed prompt)
    model               VARCHAR,               -- Model used (e.g., claude-opus-4-6)
    created_at          TIMESTAMP,
    last_activity_at    TIMESTAMP,
    vm_process_name     VARCHAR,               -- VM process name (e.g., "zen-relaxed-clarke")
    user_selected_folders VARCHAR,             -- JSON array of mounted folders
    is_archived         BOOLEAN DEFAULT FALSE
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_cowork_jsonl ON cowork_sessions(jsonl_session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);
CREATE INDEX IF NOT EXISTS idx_tool_calls_session ON tool_calls(session_id);
CREATE INDEX IF NOT EXISTS idx_tool_calls_name ON tool_calls(tool_name);
CREATE INDEX IF NOT EXISTS idx_tool_calls_file ON tool_calls(file_path);
CREATE INDEX IF NOT EXISTS idx_files_session ON files_referenced(session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project_path);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at);
CREATE INDEX IF NOT EXISTS idx_commands_session ON command_invocations(session_id);
CREATE INDEX IF NOT EXISTS idx_commands_name ON command_invocations(command_name);
CREATE INDEX IF NOT EXISTS idx_model_usage_session ON model_usage(session_id);
CREATE INDEX IF NOT EXISTS idx_model_usage_model ON model_usage(model);
