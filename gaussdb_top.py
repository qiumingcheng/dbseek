#!/usr/bin/env python3
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from getpass import getpass

try:
    import curses
except ImportError:  # pragma: no cover
    curses = None

VERSION = "0.1.0"

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

WAIT_EVENT_SQL = """
SELECT
  COALESCE(wait_event, 'none') AS wait_event,
  COUNT(*) AS waits,
  SUM(EXTRACT(EPOCH FROM (now() - query_start)))::BIGINT AS time_s,
  COALESCE(wait_event_type, 'none') AS wait_class
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND state <> 'idle'
GROUP BY COALESCE(wait_event, 'none'), COALESCE(wait_event_type, 'none')
ORDER BY time_s DESC
LIMIT 5;
""".strip()

TOP_SESSION_SQL = """
SELECT
  pid::text,
  usename,
  application_name,
  COALESCE(wait_event_type, 'none') AS wait_type,
  COALESCE(wait_event, 'none') AS wait_event,
  EXTRACT(EPOCH FROM (now() - query_start))::INT AS runtime_s,
  state,
  LEFT(REPLACE(query, '\n', ' '), 80) AS sql_text
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND state <> 'idle'
ORDER BY runtime_s DESC NULLS LAST
LIMIT 5;
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
LIMIT 5;
""".strip()

LOCK_SUMMARY_SQL = """
SELECT
  locktype,
  mode,
  COUNT(*) AS locks
FROM pg_locks
GROUP BY locktype, mode
ORDER BY locks DESC
LIMIT 5;
""".strip()

HELP_TEXT = """
Help

1: Section 1 - DATABASE
2: Section 2 - INSTANCE
3: Section 3 - DB WAIT EVENTS
4: Section 4 - SESSION/PROCESS
q/Q: Exit help

Interactive Keys
f: standard <-> detailed
r: Cumulative <-> Real-Time (Section 3)
s: SQL mode (Section 4)
p: session/process mode (Section 4)
m: USERNAME/PROGRAM <-> MODULE/ACTION (Section 4)
t: tablespace
x: SQL plan table
I: change refresh interval
q: quit
Esc: pause
""".strip()


class Config:
    def __init__(self):
        self.batch = False
        self.iterations = 0
        self.output = None
        self.interval = 5
        self.realtime_wait = False
        self.show_module = False
        self.sql_mode = False
        self.detailed = False
        self.logon = None
        self.password = None
        self.section = 0
        self.previous_wait = {}


def parse_args(argv):
    config = Config()
    args = list(argv)
    logon = None
    idx = 0
    while idx < len(args):
        arg = args[idx]
        if arg == "-b":
            config.batch = True
        elif arg == "-n":
            idx += 1
            config.iterations = int(args[idx])
        elif arg == "-o":
            idx += 1
            config.output = args[idx]
        elif arg == "-i":
            idx += 1
            config.interval = int(args[idx])
        elif arg == "-r":
            config.realtime_wait = True
        elif arg == "-m":
            config.show_module = True
        elif arg == "-s":
            config.sql_mode = True
        elif arg == "-f":
            config.detailed = True
        elif arg == "-v":
            print(VERSION)
            raise SystemExit(0)
        elif arg == "-h":
            print_usage()
            raise SystemExit(0)
        elif arg.startswith("-"):
            raise SystemExit(f"Unknown option: {arg}")
        else:
            logon = arg
        idx += 1
    config.logon = logon
    return config


def print_usage():
    print("Usage: gtop [ [Options] [Logon] ]")
    print("Logon: {username[@connect_identifier] | / } [AS SYSDBA]")
    print("Options:")
    print("  -b        batch mode")
    print("  -n <num>  max iterations")
    print("  -o <file> output file (batch)")
    print("  -i <sec>  refresh interval (default 5)")
    print("  -r        real-time wait events")
    print("  -m        toggle USERNAME/PROGRAM vs MODULE/ACTION")
    print("  -s        SQL mode")
    print("  -f        detailed format (132 cols)")
    print("  -v        version")
    print("  -h        help")


def parse_logon(logon):
    if not logon or logon == "/":
        return {}
    cleaned = re.sub(r"\s+AS\s+SYSDBA\s*$", "", logon, flags=re.IGNORECASE)
    parts = cleaned.split("@", 1)
    return {"user": parts[0], "connect": parts[1] if len(parts) > 1 else None}


def build_gsql_command(config, sql):
    cmd = ["gsql", "-X", "-t", "-A", "-F", "|", "-c", sql]
    logon = parse_logon(config.logon)
    if logon.get("connect"):
        cmd.extend(["-d", logon["connect"]])
    if logon.get("user"):
        cmd.extend(["-U", logon["user"]])
    return cmd


def run_query(config, sql):
    env = os.environ.copy()
    if config.password:
        env["PGPASSWORD"] = config.password
    cmd = build_gsql_command(config, sql)
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "gsql failed")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return [line.split("|") for line in lines]


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def read_loadavg():
    try:
        with open("/proc/loadavg", "r", encoding="utf-8") as handle:
            return handle.read().strip().split()[:3]
    except OSError:
        return ["0", "0", "0"]


def read_cpu_busy():
    try:
        with open("/proc/stat", "r", encoding="utf-8") as handle:
            line = handle.readline()
    except OSError:
        return 0.0
    parts = line.split()
    if len(parts) < 5:
        return 0.0
    values = [int(v) for v in parts[1:8]]
    idle = values[3] + values[4]
    total = sum(values)
    return 100.0 * (total - idle) / total if total else 0.0


def format_number(value, width):
    if value is None:
        text = "N/A"
    else:
        text = str(value)
    return text[:width].ljust(width)


def section_header(title, width):
    return f"{title}".ljust(width, " ")


def build_sections(config):
    instance_info = run_query(config, INSTANCE_INFO_SQL)
    db_stats = run_query(config, DB_STATS_SQL)
    summary_rows = run_query(config, SESSION_SUMMARY_SQL)
    wait_rows = run_query(config, WAIT_EVENT_SQL)
    top_sessions = run_query(config, TOP_SESSION_SQL)
    top_sql = run_query(config, TOP_SQL_SQL)
    locks = run_query(config, LOCK_SUMMARY_SQL)

    width = 132 if config.detailed else 80
    loadavg = read_loadavg()
    cpu_busy = read_cpu_busy()

    inst = instance_info[0] if instance_info else ["-", "-", "-", "-", "-"]
    db = db_stats[0] if db_stats else ["-", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"]

    section1 = [
        section_header("Section 1 - DATABASE", width),
        f"Version {inst[3]}  role N/A  db {inst[0]}  time {datetime.now().strftime('%H:%M:%S')}  up {inst[4]}",
        f"ins 1  sn {db[1]}  us N/A  mt N/A  fra N/A  er N/A  %db N/A",
    ]

    section2 = [
        section_header("Section 2 - INSTANCE", width),
        "ID  %CPU  LOAD  %DCU  AAS  ASC  ASI  ASW  ASP  AST  UST  MBPS  IOPS  IORL  LOGR  PHYR  PHYW  %FR  PGA  TEMP  UTPS  UCPS  SSRT  DCTR  DWTR  %DBT",
        f"1  {cpu_busy:5.1f}  {loadavg[0]:4}  N/A  N/A  N/A  N/A  N/A  0  {db[1]}  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A  N/A",
    ]

    wait_section = [
        section_header("Section 3 - DB WAIT EVENTS", width),
        "EVENT  WAITS  TIME(s)  AVG_MS  PCT  WAIT_CLASS",
    ]
    realtime_rows = []
    for row in wait_rows or [["none", "0", "0", "none"]]:
        waits = safe_int(row[1])
        time_s = safe_int(row[2])
        prev = config.previous_wait.get(row[0], (0, 0))
        if config.realtime_wait:
            waits = max(waits - prev[0], 0)
            time_s = max(time_s - prev[1], 0)
        realtime_rows.append((row[0], waits, time_s, row[3]))
        config.previous_wait[row[0]] = (safe_int(row[1]), safe_int(row[2]))
    for event, waits, time_s, wait_class in realtime_rows:
        avg_ms = f"{(time_s * 1000 / waits) if waits else 0:.1f}"
        wait_section.append(f"{event}  {waits}  {time_s}  {avg_ms}  N/A  {wait_class}")

    if config.sql_mode:
        section4 = [section_header("Section 4 - SQL MODE", width)]
        section4.append("SQL_ID  EXEC  ELAPSED  CPU  I/O  WAIT  SQL_TEXT")
        for row in top_sql or [["none", "0", "0", "none"]]:
            section4.append(f"N/A  {row[1]}  {row[2]}  N/A  N/A  {row[3]}  {row[0]}")
    else:
        section4 = [section_header("Section 4 - SESSION/PROCESS", width)]
        if config.show_module:
            section4.append("ID  SID  SPID  USERNAME  MODULE  ACTION  SQL  E/T  STA  WAIT_CLASS  EVENT")
        else:
            section4.append("ID  SID  SPID  USERNAME  PROGRAM  SQL  E/T  STA  WAIT_CLASS  EVENT")
        for row in top_sessions or [["-", "-", "-", "none", "none", "0", "idle", "none"]]:
            program = row[2]
            if config.show_module:
                section4.append(
                    f"1  {row[0]}  {row[0]}  {row[1]}  N/A  N/A  {row[7]}  {row[5]}  {row[6]}  {row[3]}  {row[4]}"
                )
            else:
                section4.append(
                    f"1  {row[0]}  {row[0]}  {row[1]}  {program}  {row[7]}  {row[5]}  {row[6]}  {row[3]}  {row[4]}"
                )

    extra = ["", "Locks (Top)" ]
    for row in locks or [["none", "none", "0"]]:
        extra.append(f"{row[0]}  {row[1]}  {row[2]}")

    return section1, section2, wait_section, section4, extra


def render_text(config):
    sections = build_sections(config)
    lines = []
    if config.section in (1, 2, 3, 4):
        target = sections[config.section - 1]
        lines.extend(target)
    else:
        for section in sections[:4]:
            lines.extend(section)
            lines.append("")
        lines.extend(sections[4])
    return "\n".join(lines)


def batch_loop(config):
    output_handle = open(config.output, "w", encoding="utf-8") if config.output else sys.stdout
    cycle = 1
    while True:
        output_handle.write(f"Cycle {cycle} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output_handle.write(render_text(config))
        output_handle.write("\n\n")
        output_handle.flush()
        cycle += 1
        if config.iterations and cycle > config.iterations:
            break
        time.sleep(config.interval)
    if output_handle is not sys.stdout:
        output_handle.close()


def handle_password(config):
    if config.logon and "/" not in config.logon and "@" in config.logon:
        config.password = getpass("Password: ")


def interactive_loop(config):
    if not curses:
        print("Interactive mode requires curses.")
        return

    def run(screen):
        curses.curs_set(0)
        screen.nodelay(True)
        screen.timeout(200)
        paused = False

        def show_message(lines):
            screen.erase()
            for idx, line in enumerate(lines):
                screen.addstr(idx, 0, line)
            screen.refresh()
            screen.nodelay(False)
            screen.getch()
            screen.nodelay(True)

        while True:
            if not paused:
                content = render_text(config)
                screen.erase()
                for idx, line in enumerate(content.splitlines()):
                    try:
                        screen.addstr(idx, 0, line[: (132 if config.detailed else 80)])
                    except curses.error:
                        pass
                screen.refresh()
            key = screen.getch()
            if key == -1:
                time.sleep(config.interval)
                continue
            if key in (ord("q"), ord("Q")):
                break
            if key == ord("h"):
                show_message(HELP_TEXT.splitlines())
                continue
            if key == ord("f"):
                config.detailed = not config.detailed
            elif key == ord("r"):
                config.realtime_wait = not config.realtime_wait
            elif key == ord("s"):
                config.sql_mode = True
            elif key == ord("p"):
                config.sql_mode = False
            elif key == ord("m"):
                config.show_module = not config.show_module
            elif key == ord("I"):
                screen.nodelay(False)
                screen.addstr(0, 0, "New interval (sec): ")
                screen.refresh()
                curses.echo()
                value = screen.getstr().decode("utf-8")
                curses.noecho()
                screen.nodelay(True)
                if value.isdigit():
                    config.interval = int(value)
            elif key == ord("1"):
                config.section = 1
            elif key == ord("2"):
                config.section = 2
            elif key == ord("3"):
                config.section = 3
            elif key == ord("4"):
                config.section = 4
            elif key == ord("t"):
                show_message(["Tablespace information", "N/A (standby or not available)."])
            elif key == ord("x"):
                screen.nodelay(False)
                screen.addstr(0, 0, "Enter SQL_ID: ")
                screen.refresh()
                curses.echo()
                _ = screen.getstr().decode("utf-8")
                curses.noecho()
                screen.nodelay(True)
                show_message(["SQL Plan", "N/A (plan table not available)."])
            elif key == 27:  # Esc
                paused = not paused

    curses.wrapper(run)


def main():
    config = parse_args(sys.argv[1:])
    handle_password(config)
    if config.batch:
        batch_loop(config)
    else:
        interactive_loop(config)


if __name__ == "__main__":
    main()
