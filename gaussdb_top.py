#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

INSTANCE_INFO_SQL = """
SELECT
  current_database() AS db_name,
  inet_server_addr()::text AS host,
  inet_server_port()::text AS port,
  current_setting('server_version') AS version,
  pg_postmaster_start_time()::timestamp(0)::text AS start_time;
""".strip()

DB_STATS_SQL = """
SELECT
  datname,
  numbackends::text,
  xact_commit::text,
  xact_rollback::text,
  blks_read::text,
  blks_hit::text,
  tup_returned::text,
  tup_fetched::text,
  tup_inserted::text,
  tup_updated::text,
  tup_deleted::text
FROM pg_stat_database
WHERE datname = current_database();
""".strip()

SESSION_SUMMARY_SQL = """
SELECT
  state,
  COALESCE(wait_event_type, 'none') AS wait_type,
  COUNT(*) AS sessions
FROM pg_stat_activity
GROUP BY state, COALESCE(wait_event_type, 'none')
ORDER BY sessions DESC;
""".strip()

TOP_SQL_SQL = """
SELECT
  LEFT(REPLACE(query, '\n', ' '), 120) AS sql_text,
  COUNT(*) AS executions,
  MAX(EXTRACT(EPOCH FROM (now() - query_start)))::INT AS max_runtime_s,
  COALESCE(MAX(wait_event_type), 'none') AS wait_type
FROM pg_stat_activity
WHERE state = 'active'
  AND pid <> pg_backend_pid()
  AND query NOT ILIKE '%pg_stat_activity%'
GROUP BY LEFT(REPLACE(query, '\n', ' '), 120)
ORDER BY executions DESC
LIMIT 10;
""".strip()

TOP_SESSION_SQL = """
SELECT
  pid::text,
  usename,
  application_name,
  client_addr::text,
  state,
  COALESCE(wait_event_type, 'none') AS wait_type,
  EXTRACT(EPOCH FROM (now() - query_start))::INT AS runtime_s,
  LEFT(REPLACE(query, '\n', ' '), 80) AS sql_text
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
ORDER BY runtime_s DESC NULLS LAST
LIMIT 10;
""".strip()

BGWRITER_SQL = """
SELECT
  checkpoints_timed::text,
  checkpoints_req::text,
  buffers_checkpoint::text,
  buffers_clean::text,
  maxwritten_clean::text,
  buffers_backend::text,
  buffers_alloc::text
FROM pg_stat_bgwriter;
""".strip()


def build_gsql_command(args, sql):
    cmd = [
        "gsql",
        "-X",
        "-t",
        "-A",
        "-F",
        "|",
        "-c",
        sql,
    ]
    if args.database:
        cmd.extend(["-d", args.database])
    if args.user:
        cmd.extend(["-U", args.user])
    if args.host:
        cmd.extend(["-h", args.host])
    if args.port:
        cmd.extend(["-p", str(args.port)])
    return cmd


def run_query(args, sql):
    env = os.environ.copy()
    if args.password:
        env["PGPASSWORD"] = args.password
    elif os.environ.get("GAUSSDB_PASSWORD"):
        env["PGPASSWORD"] = os.environ["GAUSSDB_PASSWORD"]

    cmd = build_gsql_command(args, sql)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gsql failed")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    rows = [line.split("|") for line in lines]
    return rows


def format_table(headers, rows):
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    header_line = "  ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    separator = "  ".join("-" * width for width in widths)
    body = ["  ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row)) for row in rows]
    return "\n".join([header_line, separator] + body)


def render(args):
    instance_info = run_query(args, INSTANCE_INFO_SQL)
    db_stats = run_query(args, DB_STATS_SQL)
    summary_rows = run_query(args, SESSION_SUMMARY_SQL)
    top_rows = run_query(args, TOP_SQL_SQL)
    top_sessions = run_query(args, TOP_SESSION_SQL)
    bgwriter = run_query(args, BGWRITER_SQL)

    header = "gaussdb-top (oratop-alike)"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    info_table = format_table(
        ["db", "host", "port", "version", "start_time"],
        instance_info or [["unknown", "-", "-", "-", "-"]],
    )
    stats_table = format_table(
        [
            "db",
            "sessions",
            "commit",
            "rollback",
            "read",
            "hit",
            "return",
            "fetch",
            "insert",
            "update",
            "delete",
        ],
        db_stats or [["unknown", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"]],
    )
    bgwriter_table = format_table(
        [
            "chk_timed",
            "chk_req",
            "buf_chk",
            "buf_clean",
            "max_clean",
            "buf_backend",
            "buf_alloc",
        ],
        bgwriter or [["0", "0", "0", "0", "0", "0", "0"]],
    )

    output = [
        f"{header}  |  {timestamp}",
        "",
        "Instance",
        info_table,
        "",
        "Database Stats",
        stats_table,
        "",
        "Session Summary",
        format_table(["state", "wait_type", "sessions"], summary_rows or [["none", "none", "0"]]),
        "",
        "Top SQL (active)",
        format_table(
            ["sql_text", "executions", "max_runtime_s", "wait_type"],
            top_rows or [["none", "0", "0", "none"]],
        ),
        "",
        "Top Sessions",
        format_table(
            ["pid", "user", "app", "client", "state", "wait", "runtime_s", "sql_text"],
            top_sessions or [["-", "-", "-", "-", "-", "-", "0", "-"]],
        ),
        "",
        "Background Writer",
        bgwriter_table,
    ]
    return "\n".join(output)


def clear_screen():
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="GaussDB oratop-style monitor")
    parser.add_argument("--database", "-d", help="Database name")
    parser.add_argument("--user", "-U", help="Database user")
    parser.add_argument("--host", "-h", help="Database host")
    parser.add_argument("--port", "-p", type=int, help="Database port")
    parser.add_argument("--password", "-W", help="Database password (or set GAUSSDB_PASSWORD)")
    parser.add_argument("--interval", "-i", type=int, default=5, help="Refresh interval in seconds")
    parser.add_argument("--iterations", "-n", type=int, default=0, help="Number of refreshes (0=continuous)")
    return parser.parse_args()


def main():
    args = parse_args()
    iterations = args.iterations
    count = 0
    while True:
        clear_screen()
        try:
            sys.stdout.write(render(args) + "\n")
        except RuntimeError as exc:
            sys.stdout.write(f"Error: {exc}\n")
            sys.stdout.write("Check gsql availability and connection parameters.\n")
            return 1
        sys.stdout.flush()
        count += 1
        if iterations and count >= iterations:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
