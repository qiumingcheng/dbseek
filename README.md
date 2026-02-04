# gaussdb-top

`gaussdb-top` provides an oratop-like, terminal-friendly monitor for GaussDB
instances. It runs snapshot queries against GaussDB system views to surface
instance details, workload metrics, and hot sessions/SQL for fast triage.

## Requirements

- GaussDB client tools (`gsql`) available on `PATH`.
- Access to a GaussDB instance with privileges to read:
  - `pg_stat_activity`
  - `pg_stat_database`
  - `pg_stat_bgwriter`

## Usage

```bash
# Basic usage (uses default gsql environment / .pgpass)
./gaussdb_top.py

# Explicit connection settings
./gaussdb_top.py -h 127.0.0.1 -p 5432 -U dbadmin -d postgres

# Provide password via env var
GAUSSDB_PASSWORD=secret ./gaussdb_top.py -h 127.0.0.1 -U dbadmin -d postgres

# Run 3 refresh cycles with a 2-second interval
./gaussdb_top.py -i 2 -n 3
```

## Output sections

- **Instance**: database name, host/port, version, and start time.
- **Database Stats**: commit/rollback counts, block read/hit metrics, and tuple activity.
- **Database Delta**: per-interval deltas for commits/rollbacks and tuple activity.
- **Session Summary**: sessions grouped by state and wait type.
- **Wait Events**: top wait event types and events.
- **Top SQL (active)**: active SQL snippets with execution counts, max runtime, and wait type.
- **Top Sessions**: longest-running sessions with wait classification and SQL snippet.
- **Lock Summary**: top lock types and modes.
- **Background Writer**: checkpoint and buffer statistics.
- **Background Writer Delta**: per-interval buffer and checkpoint deltas.

## Status

GaussDB oratop-style monitoring implementation.
