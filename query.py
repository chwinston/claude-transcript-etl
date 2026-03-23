#!/usr/bin/env python3
"""
Quick query tool for Claude transcript database.

Usage:
    python3 query.py "SELECT COUNT(*) FROM sessions"
    python3 query.py --today                           # Today's sessions
    python3 query.py --costs                           # Cost breakdown by project
    python3 query.py --tools                           # Tool usage ranking
    python3 query.py --models                          # Model usage breakdown
    python3 query.py --sessions 7                      # Last 7 days of sessions
    python3 query.py --export sessions.csv             # Export query to CSV
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()


def get_db(backend: str = None, db_path: str = None):
    """Auto-detect and connect to the database."""
    if db_path:
        p = Path(db_path)
        if p.suffix == ".duckdb" or backend == "duckdb":
            import duckdb
            return duckdb.connect(str(p), read_only=True), "duckdb"
        import sqlite3
        return sqlite3.connect(str(p)), "sqlite"

    if backend == "duckdb" or (SCRIPT_DIR / "transcripts.duckdb").exists():
        try:
            import duckdb
            return duckdb.connect(str(SCRIPT_DIR / "transcripts.duckdb"), read_only=True), "duckdb"
        except ImportError:
            pass

    import sqlite3
    db_file = SCRIPT_DIR / "transcripts.db"
    if not db_file.exists():
        print(f"No database found. Run etl.py first.")
        sys.exit(1)
    return sqlite3.connect(str(db_file)), "sqlite"


def run_query(con, sql: str, export_path: str = None):
    """Execute a query and print results."""
    try:
        cursor = con.execute(sql)
        rows = cursor.fetchall()
        if not rows:
            print("(no results)")
            return

        # Get column names
        cols = [desc[0] for desc in cursor.description]

        if export_path:
            with open(export_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"Exported {len(rows)} rows to {export_path}")
            return

        # Print as table
        col_widths = [len(c) for c in cols]
        for row in rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)[:60]))

        header = " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cols))
        print(header)
        print("-" * len(header))
        for row in rows:
            print(" | ".join(str(v)[:60].ljust(col_widths[i]) for i, v in enumerate(row)))
        print(f"\n({len(rows)} rows)")

    except Exception as e:
        print(f"Query error: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Query Claude transcript database")
    parser.add_argument("sql", nargs="?", help="SQL query to execute")
    parser.add_argument("--today", action="store_true", help="Today's sessions")
    parser.add_argument("--costs", action="store_true", help="Cost breakdown by project")
    parser.add_argument("--tools", action="store_true", help="Tool usage ranking")
    parser.add_argument("--models", action="store_true", help="Model usage")
    parser.add_argument("--sessions", type=int, metavar="DAYS", help="Sessions from last N days")
    parser.add_argument("--export", type=str, metavar="FILE", help="Export results to CSV")
    parser.add_argument("--backend", choices=["sqlite", "duckdb"], default=None)
    parser.add_argument("--db", type=str, default=None, help="Path to database file")
    args = parser.parse_args()

    con, backend = get_db(args.backend, args.db)
    today = datetime.now().strftime("%Y-%m-%d")

    if args.today:
        sql = f"""
            SELECT session_id, project_path, model, started_at,
                   duration_seconds/60 as minutes, estimated_total_cost_usd as cost,
                   user_message_count as msgs, tool_call_count as tools
            FROM sessions
            WHERE started_at >= '{today}'
              AND is_agent = 0
            ORDER BY started_at
        """
    elif args.costs:
        sql = """
            SELECT project_path,
                   COUNT(*) as sessions,
                   ROUND(SUM(estimated_total_cost_usd), 2) as total_cost,
                   ROUND(SUM(duration_seconds)/3600.0, 1) as total_hours
            FROM sessions
            WHERE is_agent = 0
            GROUP BY project_path
            ORDER BY total_cost DESC
        """
    elif args.tools:
        sql = """
            SELECT tool_name,
                   COUNT(*) as uses,
                   COUNT(CASE WHEN has_error THEN 1 END) as errors,
                   ROUND(COUNT(CASE WHEN has_error THEN 1 END) * 100.0 / COUNT(*), 1) as error_pct
            FROM tool_calls
            GROUP BY tool_name
            ORDER BY uses DESC
        """
    elif args.models:
        sql = """
            SELECT model,
                   SUM(message_count) as messages,
                   SUM(total_input_tokens + total_cache_creation_tokens + total_cache_read_tokens) as input_tokens,
                   SUM(total_output_tokens) as output_tokens
            FROM model_usage
            GROUP BY model
            ORDER BY messages DESC
        """
    elif args.sessions:
        cutoff = (datetime.now() - timedelta(days=args.sessions)).strftime("%Y-%m-%d")
        sql = f"""
            SELECT date(started_at) as day,
                   COUNT(*) as sessions,
                   ROUND(SUM(duration_seconds)/3600.0, 1) as hours,
                   ROUND(SUM(estimated_total_cost_usd), 2) as cost,
                   SUM(user_message_count) as messages
            FROM sessions
            WHERE started_at >= '{cutoff}' AND is_agent = 0
            GROUP BY date(started_at)
            ORDER BY day DESC
        """
    elif args.sql:
        sql = args.sql
    else:
        parser.print_help()
        return

    run_query(con, sql, args.export)
    con.close()


if __name__ == "__main__":
    main()
