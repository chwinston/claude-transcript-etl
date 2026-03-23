#!/usr/bin/env python3
"""
Claude Code Transcript ETL v3.0 (Portable)
=============================================
Extracts conversations from Claude Code and Cowork JSONL files into a
local database (SQLite by default, DuckDB optional).

Cross-platform: macOS, Windows, Linux.
Zero mandatory dependencies beyond Python 3.9+.

Usage:
    python3 etl.py                    # Incremental extraction
    python3 etl.py --full             # Full re-extraction (drops and recreates)
    python3 etl.py --stats            # Print database statistics
    python3 etl.py --backend duckdb   # Use DuckDB instead of SQLite
    python3 etl.py --config path.yaml # Use a custom config file
    python3 etl.py --claude-dir PATH  # Override ~/.claude location
    python3 etl.py --cowork-dir PATH  # Override Cowork sessions location
    python3 etl.py --code-only        # Only extract Claude Code transcripts
    python3 etl.py --cowork-only      # Only extract Cowork transcripts
"""

import argparse
import json
import os
import platform
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Optional imports
# ---------------------------------------------------------------------------

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

# ---------------------------------------------------------------------------
# Cross-platform path defaults
# ---------------------------------------------------------------------------

SYSTEM = platform.system()  # 'Darwin', 'Windows', 'Linux'
SCRIPT_DIR = Path(__file__).parent.resolve()


def default_claude_dir() -> Path:
    """Where Claude Code stores project transcripts."""
    if SYSTEM == "Windows":
        appdata = os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming")
        candidate = Path(appdata) / "Claude"
        if candidate.exists():
            return candidate
        # Fallback: some Windows installs use ~/.claude
        return Path.home() / ".claude"
    return Path.home() / ".claude"


def default_cowork_dir() -> Path:
    """Where Cowork stores local agent mode sessions."""
    if SYSTEM == "Darwin":
        return Path.home() / "Library" / "Application Support" / "Claude" / "local-agent-mode-sessions"
    elif SYSTEM == "Windows":
        appdata = os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming")
        return Path(appdata) / "Claude" / "local-agent-mode-sessions"
    else:  # Linux
        return Path.home() / ".config" / "Claude" / "local-agent-mode-sessions"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_PRICING = {
    "claude-opus-4-6":              {"input": 15.00, "output": 75.00, "cache_read": 1.50, "cache_create": 18.75},
    "claude-opus-4-5-20250620":     {"input": 15.00, "output": 75.00, "cache_read": 1.50, "cache_create": 18.75},
    "claude-sonnet-4-5-20250929":   {"input":  3.00, "output": 15.00, "cache_read": 0.30, "cache_create":  3.75},
    "claude-sonnet-4-6":            {"input":  3.00, "output": 15.00, "cache_read": 0.30, "cache_create":  3.75},
    "claude-haiku-4-5-20251001":    {"input":  0.80, "output":  4.00, "cache_read": 0.08, "cache_create":  1.00},
}
FALLBACK_PRICING = {"input": 3.00, "output": 15.00, "cache_read": 0.30, "cache_create": 3.75}


def load_config(config_path: Path | None = None) -> dict:
    """Load config from YAML or return defaults."""
    defaults = {
        "backend": "sqlite",
        "db_path": str(SCRIPT_DIR / "transcripts.db"),
        "sources": {
            "claude_code": {"enabled": True, "path": str(default_claude_dir())},
            "cowork": {"enabled": True, "path": "auto"},
        },
        "pricing": DEFAULT_PRICING,
        "analysis": {"users": [], "teams": {}},
    }

    if config_path and config_path.exists() and HAS_YAML:
        with open(config_path) as f:
            user_cfg = yaml.safe_load(f) or {}
        # Merge pricing
        if "pricing" in user_cfg:
            for model, prices in user_cfg["pricing"].items():
                if model == "default":
                    continue
                defaults["pricing"][model] = prices
        defaults["backend"] = user_cfg.get("backend", defaults["backend"])
        if "db_path" in user_cfg:
            defaults["db_path"] = str(Path(user_cfg["db_path"]).expanduser())
        if "sources" in user_cfg:
            for src_key in ("claude_code", "cowork"):
                if src_key in user_cfg["sources"]:
                    defaults["sources"][src_key].update(user_cfg["sources"][src_key])
        if "analysis" in user_cfg:
            defaults["analysis"].update(user_cfg["analysis"])

    return defaults


# ---------------------------------------------------------------------------
# Database abstraction layer
# ---------------------------------------------------------------------------

class DatabaseBackend:
    """Thin wrapper so ETL code works with both SQLite and DuckDB."""

    def __init__(self, backend: str, db_path: str):
        self.backend = backend
        self.db_path = db_path
        self.con = None

    def connect(self, read_only: bool = False):
        if self.backend == "duckdb":
            if not HAS_DUCKDB:
                print("Error: duckdb not installed. Run: pip install duckdb")
                print("Or switch to SQLite backend (default, zero-install).")
                sys.exit(1)
            self.con = duckdb.connect(self.db_path, read_only=read_only)
        else:
            self.con = sqlite3.connect(self.db_path)
            try:
                self.con.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass  # WAL not supported on some filesystems (network mounts, etc.)
            self.con.execute("PRAGMA synchronous=NORMAL")
            self.con.execute("PRAGMA foreign_keys=ON")
        return self

    def execute(self, sql: str, params=None):
        if params:
            return self.con.execute(sql, params)
        return self.con.execute(sql)

    def fetchone(self, sql: str, params=None):
        if params:
            return self.con.execute(sql, params).fetchone()
        return self.con.execute(sql).fetchone()

    def fetchall(self, sql: str, params=None):
        if params:
            return self.con.execute(sql, params).fetchall()
        return self.con.execute(sql).fetchall()

    def commit(self):
        if self.backend == "sqlite":
            self.con.commit()
        # DuckDB auto-commits

    def close(self):
        if self.con:
            self.con.close()

    def init_schema(self):
        if self.backend == "duckdb":
            schema_file = SCRIPT_DIR / "schema_duckdb.sql"
        else:
            schema_file = SCRIPT_DIR / "schema_sqlite.sql"
        if not schema_file.exists():
            print(f"Error: Schema file not found: {schema_file}")
            sys.exit(1)
        schema_sql = schema_file.read_text()
        lines = [l for l in schema_sql.splitlines() if not l.strip().startswith("--")]
        clean_sql = "\n".join(lines)
        for stmt in clean_sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    self.execute(stmt)
                except Exception as e:
                    print(f"  Warning: schema statement failed: {e}")
        self.commit()

    def drop_all_tables(self):
        tables = ["cowork_sessions", "etl_state", "model_usage", "command_invocations",
                   "files_referenced", "tool_calls", "messages", "sessions"]
        for table in tables:
            try:
                self.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass
        self.commit()

    @property
    def insert_or_replace(self) -> str:
        """DuckDB uses INSERT OR REPLACE, SQLite uses INSERT OR REPLACE."""
        return "INSERT OR REPLACE"

    @property
    def on_conflict_ignore(self) -> str:
        """Conflict handling for both backends."""
        if self.backend == "duckdb":
            return "ON CONFLICT DO NOTHING"
        return "ON CONFLICT DO NOTHING"


# ---------------------------------------------------------------------------
# Helpers (unchanged from v2.1, just Python-stdlib-only)
# ---------------------------------------------------------------------------

def project_dir_to_path(dirname: str) -> str:
    """Convert a .claude/projects directory name back to the original path."""
    if not dirname.startswith("-"):
        return dirname
    parts = dirname[1:].split("-")
    resolved = "/" if SYSTEM != "Windows" else "C:\\"
    i = 0
    while i < len(parts):
        best_match = None
        for j in range(i + 1, len(parts) + 1):
            candidate = "-".join(parts[i:j])
            test_path = os.path.join(resolved, candidate)
            if os.path.exists(test_path):
                best_match = (j, candidate)
        if best_match:
            j, candidate = best_match
            resolved = os.path.join(resolved, candidate)
            i = j
        else:
            resolved = os.path.join(resolved, parts[i])
            i += 1
    return resolved


def parse_timestamp(ts) -> datetime | None:
    """Parse a timestamp from JSONL — could be ISO string or epoch millis."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    if isinstance(ts, str):
        ts = ts.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None
    return None


def ts_to_iso(dt: datetime | None) -> str | None:
    """Convert datetime to ISO8601 string for SQLite storage."""
    if dt is None:
        return None
    return dt.isoformat()


def extract_text_content(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    texts.append(block.get("text", ""))
                elif block.get("type") == "tool_result":
                    result = block.get("content", "")
                    if isinstance(result, str):
                        texts.append(f"[tool_result: {result[:200]}]")
                    elif isinstance(result, list):
                        for rb in result:
                            if isinstance(rb, dict) and rb.get("type") == "text":
                                texts.append(f"[tool_result: {rb.get('text', '')[:200]}]")
        return "\n".join(texts)
    return ""


def extract_thinking(content) -> str | None:
    if not isinstance(content, list):
        return None
    thoughts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "thinking":
            thoughts.append(block.get("thinking", ""))
    return "\n---\n".join(thoughts) if thoughts else None


def extract_tool_uses(content) -> list[dict]:
    if not isinstance(content, list):
        return []
    tools = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_use":
            inp = block.get("input", {})
            tool = {
                "id": block.get("id"),
                "name": block.get("name"),
                "input": inp,
            }
            if isinstance(inp, dict):
                tool["file_path"] = (
                    inp.get("file_path") or inp.get("path") or inp.get("notebook_path")
                )
                tool["command"] = inp.get("command")
                tool["description"] = inp.get("description")
            tools.append(tool)
    return tools


def extract_tool_results(content) -> dict:
    if not isinstance(content, list):
        return {}
    results = {}
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_result":
            tool_use_id = block.get("tool_use_id")
            if tool_use_id:
                results[tool_use_id] = {"is_error": block.get("is_error", False)}
    return results


def extract_command_invocation(content) -> dict | None:
    if not isinstance(content, str):
        return None
    cmd_match = re.search(r'<command-message>([^<]+)</command-message>', content)
    if cmd_match:
        cmd_name = cmd_match.group(1).strip()
        args_match = re.search(r'<command-args>([^<]*)</command-args>', content)
        args = args_match.group(1).strip() if args_match else None
        return {"command_name": cmd_name, "command_args": args, "is_skill": ":" in cmd_name}
    cmd_match = re.search(r'<command-name>/([^<]+)</command-name>', content)
    if cmd_match:
        cmd_name = cmd_match.group(1).strip()
        return {"command_name": cmd_name, "command_args": None, "is_skill": ":" in cmd_name}
    return None


def estimate_cost(model: str, input_tokens: int, output_tokens: int,
                  cache_create: int, cache_read: int, pricing: dict = None) -> dict:
    model_prices = (pricing or DEFAULT_PRICING).get(model, FALLBACK_PRICING)
    input_cost = (input_tokens / 1_000_000) * model_prices["input"]
    output_cost = (output_tokens / 1_000_000) * model_prices["output"]
    cache_create_cost = (cache_create / 1_000_000) * model_prices["cache_create"]
    cache_read_cost = (cache_read / 1_000_000) * model_prices["cache_read"]
    return {
        "input_cost": input_cost + cache_create_cost + cache_read_cost,
        "output_cost": output_cost,
        "total_cost": input_cost + output_cost + cache_create_cost + cache_read_cost,
    }


def truncate(s: str | None, max_len: int = 2000) -> str | None:
    if s is None:
        return None
    return s[:max_len] + "... [truncated]" if len(s) > max_len else s


# ---------------------------------------------------------------------------
# File Discovery
# ---------------------------------------------------------------------------

def find_jsonl_files(claude_dir: Path) -> list[dict]:
    """Find all JSONL transcript files under .claude/projects/."""
    projects_dir = claude_dir / "projects"
    if not projects_dir.exists():
        print(f"  Note: {projects_dir} does not exist (no Claude Code sessions yet?)")
        return []

    files = []
    for proj_dir in sorted(projects_dir.iterdir()):
        if not proj_dir.is_dir():
            continue
        for jsonl_file in sorted(proj_dir.glob("*.jsonl")):
            stat = jsonl_file.stat()
            is_agent = jsonl_file.name.startswith("agent-")
            files.append({
                "file_path": str(jsonl_file),
                "project_dir_name": proj_dir.name,
                "project_path": project_dir_to_path(proj_dir.name),
                "session_id": jsonl_file.stem,
                "is_agent": is_agent,
                "file_size": stat.st_size,
                "file_mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                "source": "claude-code",
            })
    return files


def find_cowork_jsonl_files(cowork_dir: Path) -> list[dict]:
    """Find all JSONL transcript files from Cowork sessions."""
    if not cowork_dir.exists():
        print(f"  Note: Cowork dir not found: {cowork_dir} (Cowork not installed?)")
        return []

    files = []
    for root, dirs, filenames in os.walk(str(cowork_dir)):
        root_path = Path(root)
        if "cowork_plugins" in root_path.parts:
            continue
        for fname in filenames:
            if not fname.endswith(".jsonl") or fname == "audit.jsonl":
                continue
            full_path = root_path / fname
            stat = full_path.stat()
            session_id = full_path.stem
            is_agent = session_id.startswith("agent-")

            cowork_session_id = None
            cowork_meta = None
            title = None
            initial_message = None
            for parent in full_path.parents:
                if parent.name.startswith("local_"):
                    cowork_session_id = parent.name
                    meta_json = parent.parent / f"{parent.name}.json"
                    if meta_json.exists():
                        try:
                            with open(meta_json) as mf:
                                cowork_meta = json.load(mf)
                                title = cowork_meta.get("title")
                                initial_message = cowork_meta.get("initialMessage")
                        except (json.JSONDecodeError, OSError):
                            pass
                    break

            project_dir_name = None
            if ".claude" in root_path.parts:
                claude_idx = list(root_path.parts).index(".claude")
                if claude_idx + 2 < len(root_path.parts) and root_path.parts[claude_idx + 1] == "projects":
                    project_dir_name = root_path.parts[claude_idx + 2]

            files.append({
                "file_path": str(full_path),
                "project_dir_name": project_dir_name or "cowork",
                "project_path": f"cowork://{title or cowork_session_id or 'unknown'}",
                "session_id": session_id,
                "is_agent": is_agent,
                "file_size": stat.st_size,
                "file_mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                "source": "cowork",
                "cowork_session_id": cowork_session_id,
                "cowork_title": title,
                "cowork_initial_message": initial_message,
                "cowork_meta": cowork_meta,
            })
    return files


# ---------------------------------------------------------------------------
# Extraction Core
# ---------------------------------------------------------------------------

def upsert_cowork_metadata(db: DatabaseBackend, file_info: dict):
    meta = file_info.get("cowork_meta")
    if not meta:
        return
    cowork_session_id = meta.get("sessionId", file_info.get("cowork_session_id"))
    if not cowork_session_id:
        return

    db.execute(f"""
        {db.insert_or_replace} INTO cowork_sessions (
            cowork_session_id, jsonl_session_id, title, initial_message,
            model, created_at, last_activity_at, vm_process_name,
            user_selected_folders, is_archived
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        cowork_session_id,
        file_info["session_id"],
        meta.get("title"),
        (meta.get("initialMessage", "") or "")[:5000],
        meta.get("model"),
        ts_to_iso(parse_timestamp(meta.get("createdAt"))),
        ts_to_iso(parse_timestamp(meta.get("lastActivityAt"))),
        meta.get("vmProcessName"),
        json.dumps(meta.get("userSelectedFolders", [])),
        1 if meta.get("isArchived", False) else 0,
    ])


def needs_extraction(db: DatabaseBackend, file_info: dict) -> bool:
    result = db.fetchone(
        "SELECT file_size_bytes, file_modified_at FROM etl_state WHERE file_path = ?",
        [file_info["file_path"]],
    )
    if result is None:
        return True
    prev_size, prev_mtime = result
    return prev_size != file_info["file_size"] or prev_mtime != ts_to_iso(file_info["file_mtime"])


def extract_session(db: DatabaseBackend, file_info: dict, pricing: dict = None) -> dict:
    """Parse a single JSONL file and insert into the database."""
    fp = file_info["file_path"]
    session_id = file_info["session_id"]

    # Delete existing data for this session (idempotent re-extraction)
    for table in ["model_usage", "command_invocations", "files_referenced", "tool_calls", "messages", "sessions"]:
        db.execute(f"DELETE FROM {table} WHERE session_id = ?", [session_id])

    messages = []
    tool_calls = []
    files_ref = {}
    tool_results_map = {}
    commands = []
    turn_durations = []
    model_token_agg = {}

    first_ts = None
    last_ts = None
    git_branch = None
    cwd = None
    version = None
    model = None
    slug = None
    agent_id = None
    seq = 0

    sess_input_tokens = 0
    sess_output_tokens = 0
    sess_cache_creation = 0
    sess_cache_read = 0

    with open(fp, "r", encoding="utf-8") as f:
        for line_num, raw_line in enumerate(f):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                entry = json.loads(raw_line)
            except json.JSONDecodeError:
                continue

            entry_type = entry.get("type")
            ts = parse_timestamp(entry.get("timestamp"))

            if ts:
                if first_ts is None or ts < first_ts:
                    first_ts = ts
                if last_ts is None or ts > last_ts:
                    last_ts = ts

            if git_branch is None and entry.get("gitBranch"):
                git_branch = entry["gitBranch"]
            if cwd is None and entry.get("cwd"):
                cwd = entry["cwd"]
            if version is None and entry.get("version"):
                version = entry["version"]
            if slug is None and entry.get("slug"):
                slug = entry["slug"]
            if agent_id is None and entry.get("agentId"):
                agent_id = entry["agentId"]

            if entry_type == "system":
                subtype = entry.get("subtype")
                if subtype == "turn_duration":
                    dur = entry.get("durationMs")
                    if dur is not None:
                        turn_durations.append(dur)
                continue

            if entry_type not in ("user", "assistant"):
                continue

            msg = entry.get("message", {})
            role = msg.get("role") or entry_type
            content = msg.get("content", "")
            uuid = entry.get("uuid", f"{session_id}-{line_num}")

            text = extract_text_content(content)
            msg_model = msg.get("model")
            if msg_model and model is None:
                model = msg_model

            usage = msg.get("usage", {})
            input_tokens = usage.get("input_tokens", 0) if usage else 0
            output_tokens = usage.get("output_tokens", 0) if usage else 0
            cache_creation_input_tokens = usage.get("cache_creation_input_tokens", 0) if usage else 0
            cache_read_input_tokens = usage.get("cache_read_input_tokens", 0) if usage else 0
            service_tier = usage.get("service_tier") if usage else None
            inference_geo = usage.get("inference_geo") if usage else None
            api_message_id = msg.get("id")

            if role == "assistant" and usage:
                sess_input_tokens += input_tokens
                sess_output_tokens += output_tokens
                sess_cache_creation += cache_creation_input_tokens
                sess_cache_read += cache_read_input_tokens

                m_key = msg_model or "unknown"
                if m_key not in model_token_agg:
                    model_token_agg[m_key] = {"input": 0, "output": 0, "cache_create": 0, "cache_read": 0, "count": 0}
                model_token_agg[m_key]["input"] += input_tokens
                model_token_agg[m_key]["output"] += output_tokens
                model_token_agg[m_key]["cache_create"] += cache_creation_input_tokens
                model_token_agg[m_key]["cache_read"] += cache_read_input_tokens
                model_token_agg[m_key]["count"] += 1

            has_thinking = False
            thinking_text = None
            if role == "assistant":
                thinking_text = extract_thinking(content)
                has_thinking = thinking_text is not None

            messages.append({
                "message_id": uuid,
                "session_id": session_id,
                "parent_uuid": entry.get("parentUuid"),
                "role": role,
                "timestamp": ts,
                "sequence_number": seq,
                "content_text": truncate(text, 50000),
                "has_thinking": has_thinking,
                "thinking_text": truncate(thinking_text, 50000),
                "stop_reason": msg.get("stop_reason"),
                "model": msg_model,
                "api_message_id": api_message_id,
                "is_sidechain": entry.get("isSidechain", False),
                "cwd": entry.get("cwd"),
                "git_branch": entry.get("gitBranch"),
                "input_tokens": input_tokens if usage else None,
                "output_tokens": output_tokens if usage else None,
                "cache_creation_input_tokens": cache_creation_input_tokens if usage else None,
                "cache_read_input_tokens": cache_read_input_tokens if usage else None,
                "service_tier": service_tier,
                "inference_geo": inference_geo,
            })
            seq += 1

            if role == "user":
                cmd = extract_command_invocation(text if isinstance(content, str) else extract_text_content(content))
                if cmd is None and isinstance(content, str):
                    cmd = extract_command_invocation(content)
                if cmd:
                    commands.append({
                        "message_id": uuid,
                        "timestamp": ts,
                        "command_name": cmd["command_name"],
                        "command_args": truncate(cmd.get("command_args"), 500),
                        "is_skill": cmd["is_skill"],
                    })
                results = extract_tool_results(content)
                tool_results_map.update(results)

            if role == "assistant":
                for tu in extract_tool_uses(content):
                    tool_calls.append({
                        "tool_call_id": tu["id"],
                        "message_id": uuid,
                        "session_id": session_id,
                        "tool_name": tu["name"],
                        "input_summary": truncate(json.dumps(tu["input"], default=str), 2000),
                        "timestamp": ts,
                        "file_path": tu.get("file_path"),
                        "command": truncate(tu.get("command"), 1000),
                        "description": tu.get("description"),
                        "result_message_id": None,
                        "has_error": False,
                    })
                    if tu.get("file_path"):
                        op = tu["name"].lower()
                        if op in ("read", "write", "edit", "glob", "grep", "notebookedit"):
                            key = (tu["file_path"], op)
                        else:
                            key = (tu["file_path"], "other")
                        files_ref[key] = files_ref.get(key, 0) + 1

                    if tu["name"] == "Bash" and tu.get("command"):
                        for match in re.findall(r'(?:^|\s)(/[^\s;|&>]+)', tu["command"]):
                            if "." in match.split("/")[-1]:
                                key = (match, "bash")
                                files_ref[key] = files_ref.get(key, 0) + 1

    # Link tool results
    for tc in tool_calls:
        if tc["tool_call_id"] in tool_results_map:
            tc["has_error"] = tool_results_map[tc["tool_call_id"]].get("is_error", False)

    user_count = sum(1 for m in messages if m["role"] == "user")
    assistant_count = sum(1 for m in messages if m["role"] == "assistant")
    duration = int((last_ts - first_ts).total_seconds()) if first_ts and last_ts else 0

    total_all_input = sess_cache_read + sess_cache_creation + sess_input_tokens
    cache_hit_rate = (sess_cache_read / total_all_input) if total_all_input > 0 else None

    cost = estimate_cost(model or "unknown", sess_input_tokens, sess_output_tokens,
                         sess_cache_creation, sess_cache_read, pricing)

    total_turn_ms = sum(turn_durations)
    avg_turn_ms = (total_turn_ms / len(turn_durations)) if turn_durations else None

    source = file_info.get("source", "claude-code")
    is_agent_val = 1 if file_info["is_agent"] else 0

    db.execute("""
        INSERT INTO sessions (
            session_id, project_path, project_dir_name, started_at, ended_at,
            duration_seconds, git_branch, cwd, claude_version, model,
            slug, agent_id,
            user_message_count, assistant_message_count, tool_call_count, command_count,
            is_agent, source,
            total_input_tokens, total_output_tokens,
            total_cache_creation_tokens, total_cache_read_tokens, cache_hit_rate,
            estimated_input_cost_usd, estimated_output_cost_usd, estimated_total_cost_usd,
            total_turn_duration_ms, avg_turn_duration_ms, turn_count,
            file_size_bytes, file_path, jsonl_modified_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        session_id, file_info["project_path"], file_info["project_dir_name"],
        ts_to_iso(first_ts), ts_to_iso(last_ts), duration, git_branch, cwd, version, model,
        slug, agent_id,
        user_count, assistant_count, len(tool_calls), len(commands),
        is_agent_val, source,
        sess_input_tokens, sess_output_tokens,
        sess_cache_creation, sess_cache_read, cache_hit_rate,
        cost["input_cost"], cost["output_cost"], cost["total_cost"],
        total_turn_ms, avg_turn_ms, len(turn_durations),
        file_info["file_size"], fp, ts_to_iso(file_info["file_mtime"]),
    ])

    if source == "cowork":
        upsert_cowork_metadata(db, file_info)

    for m in messages:
        db.execute("""
            INSERT INTO messages (
                message_id, session_id, parent_uuid, role, timestamp,
                sequence_number, content_text, has_thinking, thinking_text,
                stop_reason, model, api_message_id, is_sidechain, cwd, git_branch,
                input_tokens, output_tokens,
                cache_creation_input_tokens, cache_read_input_tokens,
                service_tier, inference_geo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            m["message_id"], m["session_id"], m["parent_uuid"], m["role"],
            ts_to_iso(m["timestamp"]), m["sequence_number"], m["content_text"],
            1 if m["has_thinking"] else 0, m["thinking_text"], m["stop_reason"],
            m["model"], m["api_message_id"], 1 if m["is_sidechain"] else 0,
            m["cwd"], m["git_branch"],
            m["input_tokens"], m["output_tokens"],
            m["cache_creation_input_tokens"], m["cache_read_input_tokens"],
            m["service_tier"], m["inference_geo"],
        ])

    for tc in tool_calls:
        if tc["tool_call_id"] is None:
            continue
        try:
            db.execute(f"""
                INSERT INTO tool_calls (
                    tool_call_id, message_id, session_id, tool_name, input_summary,
                    timestamp, file_path, command, description, result_message_id, has_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                {db.on_conflict_ignore}
            """, [
                tc["tool_call_id"], tc["message_id"], session_id, tc["tool_name"],
                tc["input_summary"], ts_to_iso(tc["timestamp"]), tc["file_path"],
                tc["command"], tc["description"], tc["result_message_id"],
                1 if tc["has_error"] else 0,
            ])
        except Exception:
            pass  # Skip duplicates

    for cmd in commands:
        try:
            db.execute(f"""
                INSERT INTO command_invocations (session_id, message_id, timestamp, command_name, command_args, is_skill)
                VALUES (?, ?, ?, ?, ?, ?)
                {db.on_conflict_ignore}
            """, [
                session_id, cmd["message_id"], ts_to_iso(cmd["timestamp"]),
                cmd["command_name"], cmd["command_args"], 1 if cmd["is_skill"] else 0,
            ])
        except Exception:
            pass

    for (fpath, op), count in files_ref.items():
        try:
            db.execute(f"""
                INSERT INTO files_referenced (session_id, file_path, operation, first_referenced_at, reference_count)
                VALUES (?, ?, ?, ?, ?)
                {db.on_conflict_ignore}
            """, [session_id, fpath, op, ts_to_iso(first_ts), count])
        except Exception:
            pass

    for m_name, m_stats in model_token_agg.items():
        try:
            db.execute(f"""
                INSERT INTO model_usage (session_id, model, message_count,
                    total_input_tokens, total_output_tokens,
                    total_cache_creation_tokens, total_cache_read_tokens)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                {db.on_conflict_ignore}
            """, [
                session_id, m_name, m_stats["count"],
                m_stats["input"], m_stats["output"],
                m_stats["cache_create"], m_stats["cache_read"],
            ])
        except Exception:
            pass

    db.execute(f"""
        {db.insert_or_replace} INTO etl_state (file_path, file_size_bytes, file_modified_at, extracted_at)
        VALUES (?, ?, ?, datetime('now'))
    """, [fp, file_info["file_size"], ts_to_iso(file_info["file_mtime"])])

    db.commit()

    return {
        "messages": len(messages),
        "tool_calls": len(tool_calls),
        "files_referenced": len(files_ref),
        "commands": len(commands),
        "tokens_in": sess_input_tokens,
        "tokens_out": sess_output_tokens,
        "cost_usd": cost["total_cost"],
    }


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def print_stats(db: DatabaseBackend):
    print("\n  Database Statistics")
    print("=" * 60)

    row = db.fetchone("SELECT COUNT(*) FROM sessions")
    print(f"  Sessions:          {row[0]}")

    try:
        rows = db.fetchall("""
            SELECT COALESCE(source, 'claude-code') as src, COUNT(*) as cnt
            FROM sessions GROUP BY src ORDER BY cnt DESC
        """)
        for src, cnt in rows:
            print(f"    {src}: {cnt}")
    except Exception:
        pass

    row = db.fetchone("SELECT COUNT(*) FROM sessions WHERE is_agent = 1")
    print(f"  Agent sessions:    {row[0]}")
    row = db.fetchone("SELECT COUNT(*) FROM messages")
    print(f"  Messages:          {row[0]}")
    row = db.fetchone("SELECT COUNT(*) FROM tool_calls")
    print(f"  Tool calls:        {row[0]}")
    row = db.fetchone("SELECT COUNT(*) FROM command_invocations")
    print(f"  Command invocs:    {row[0]}")
    row = db.fetchone("SELECT COUNT(DISTINCT file_path) FROM files_referenced")
    print(f"  Unique files:      {row[0]}")

    print("\n  Token Usage (all sessions):")
    row = db.fetchone("""
        SELECT SUM(total_input_tokens), SUM(total_output_tokens),
               SUM(total_cache_creation_tokens), SUM(total_cache_read_tokens),
               SUM(estimated_total_cost_usd)
        FROM sessions
    """)
    if row and row[0]:
        total_all = (row[0] or 0) + (row[2] or 0) + (row[3] or 0)
        print(f"  Input tokens:      {row[0]:>12,}")
        print(f"  Output tokens:     {row[1]:>12,}")
        print(f"  Cache create:      {row[2]:>12,}")
        print(f"  Cache read:        {row[3]:>12,}")
        if total_all > 0:
            print(f"  Cache hit rate:    {(row[3] or 0) / total_all * 100:>11.1f}%")
        print(f"  Est. total cost:   ${row[4]:>10.2f}")

    print("\n  Model Usage:")
    rows = db.fetchall("""
        SELECT model, SUM(message_count) as msgs,
               SUM(total_input_tokens) + SUM(total_cache_creation_tokens) + SUM(total_cache_read_tokens) as all_input,
               SUM(total_output_tokens) as out_tokens
        FROM model_usage
        GROUP BY model ORDER BY msgs DESC
    """)
    for m, msgs, inp, out in rows:
        print(f"  {m}: {msgs} msgs, {(inp or 0) + (out or 0):,} total tokens")

    print("\n  Commands & Skills Used:")
    rows = db.fetchall("""
        SELECT command_name, COUNT(*) as cnt, is_skill
        FROM command_invocations
        GROUP BY command_name, is_skill
        ORDER BY cnt DESC LIMIT 10
    """)
    if rows:
        for name, cnt, is_skill in rows:
            label = "skill" if is_skill else "cmd"
            print(f"  /{name} [{label}]: {cnt}x")
    else:
        print("  (none detected)")

    print("\n  Top Tool Usage:")
    rows = db.fetchall("""
        SELECT tool_name, COUNT(*) as cnt,
               COUNT(CASE WHEN has_error THEN 1 END) as errors
        FROM tool_calls
        GROUP BY tool_name ORDER BY cnt DESC LIMIT 10
    """)
    for name, cnt, errs in rows:
        err_str = f" ({errs} errors)" if errs > 0 else ""
        print(f"  {name}: {cnt}{err_str}")

    print("\n  Sessions by Project:")
    rows = db.fetchall("""
        SELECT project_path, COUNT(*) as cnt,
               SUM(estimated_total_cost_usd) as cost
        FROM sessions WHERE is_agent = 0
        GROUP BY project_path ORDER BY cnt DESC
    """)
    for project, cnt, cost in rows:
        print(f"  {project}: {cnt} sessions (${cost:.2f})")

    try:
        cowork_count = db.fetchone("SELECT COUNT(*) FROM cowork_sessions")[0]
        if cowork_count > 0:
            print(f"\n  Cowork Sessions: {cowork_count}")
            rows = db.fetchall("""
                SELECT title, created_at, model
                FROM cowork_sessions
                ORDER BY created_at DESC LIMIT 10
            """)
            for title, created, model_name in rows:
                print(f"  {created or '?'} | {title or '(untitled)'} [{model_name or '?'}]")
    except Exception:
        pass

    db_file = Path(db.db_path)
    if db_file.exists():
        size_mb = db_file.stat().st_size / (1024 * 1024)
        print(f"\n  Database size: {size_mb:.1f} MB")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Claude Transcript ETL v3.0 (Portable)")
    parser.add_argument("--full", action="store_true", help="Full re-extraction (drops existing data)")
    parser.add_argument("--stats", action="store_true", help="Print database statistics and exit")
    parser.add_argument("--backend", choices=["sqlite", "duckdb"], default=None, help="Database backend")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    parser.add_argument("--claude-dir", type=str, default=None, help="Path to .claude directory")
    parser.add_argument("--cowork-dir", type=str, default=None, help="Path to Cowork sessions directory")
    parser.add_argument("--db", type=str, default=None, help="Path to database file")
    parser.add_argument("--code-only", action="store_true", help="Only extract Claude Code transcripts")
    parser.add_argument("--cowork-only", action="store_true", help="Only extract Cowork transcripts")
    args = parser.parse_args()

    # Load config
    config_path = Path(args.config) if args.config else SCRIPT_DIR / "config.yaml"
    config = load_config(config_path if config_path.exists() else None)

    # CLI overrides
    backend = args.backend or config["backend"]
    db_path = args.db or config["db_path"]
    claude_dir = Path(args.claude_dir).expanduser() if args.claude_dir else Path(config["sources"]["claude_code"]["path"]).expanduser()
    cowork_path = config["sources"]["cowork"]["path"]
    if args.cowork_dir:
        cowork_dir = Path(args.cowork_dir).expanduser()
    elif cowork_path == "auto":
        cowork_dir = default_cowork_dir()
    else:
        cowork_dir = Path(cowork_path).expanduser()

    # Connect
    db = DatabaseBackend(backend, db_path)
    db.connect(read_only=args.stats)

    if args.stats:
        print_stats(db)
        db.close()
        return

    if args.full:
        print("  Full re-extraction: dropping existing tables...")
        db.drop_all_tables()

    db.init_schema()

    all_files = []

    if not args.cowork_only:
        if claude_dir.exists():
            code_files = find_jsonl_files(claude_dir)
            print(f"  Found {len(code_files)} Claude Code JSONL files in {claude_dir / 'projects'}")
            all_files.extend(code_files)
        else:
            print(f"  Claude Code dir not found: {claude_dir}")

    if not args.code_only:
        if cowork_dir.exists():
            cowork_files = find_cowork_jsonl_files(cowork_dir)
            print(f"  Found {len(cowork_files)} Cowork JSONL files in {cowork_dir}")
            all_files.extend(cowork_files)
        else:
            print(f"  Cowork sessions dir not found: {cowork_dir}")

    if not all_files:
        print("  No transcript files found in any source.")
        db.close()
        return

    to_extract = all_files if args.full else [f for f in all_files if needs_extraction(db, f)]

    if not to_extract:
        print("  All files up to date. Nothing to extract.")
        print_stats(db)
        db.close()
        return

    print(f"  Extracting {len(to_extract)} files...")

    total_messages = 0
    total_tools = 0
    total_cost = 0.0
    errors = 0

    for i, file_info in enumerate(to_extract, 1):
        source_tag = f"[{file_info.get('source', '?')}]"
        if file_info.get("source") == "cowork" and file_info.get("cowork_title"):
            short_name = file_info["cowork_title"]
        else:
            short_name = f"{file_info['project_dir_name']}/{Path(file_info['file_path']).name}"
        try:
            stats = extract_session(db, file_info, config.get("pricing"))
            total_messages += stats["messages"]
            total_tools += stats["tool_calls"]
            total_cost += stats["cost_usd"]
            cost_str = f"${stats['cost_usd']:.3f}" if stats["cost_usd"] > 0 else "$0"
            print(f"  [{i}/{len(to_extract)}] {source_tag} {short_name}: "
                  f"{stats['messages']} msgs, {stats['tool_calls']} tools, "
                  f"{stats['commands']} cmds, {cost_str}")
        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(to_extract)}] {source_tag} ERROR {short_name}: {e}")

    print(f"\n  Extraction complete:")
    print(f"   Sessions: {len(to_extract) - errors}")
    print(f"   Messages: {total_messages}")
    print(f"   Tool calls: {total_tools}")
    print(f"   Est. cost: ${total_cost:.2f}")
    if errors:
        print(f"   Errors: {errors}")

    print_stats(db)
    db.close()


if __name__ == "__main__":
    main()
