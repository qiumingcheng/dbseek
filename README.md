# dbseek oratop-like monitor

This repository provides a lightweight, Oracle `oratop`-inspired monitor written in Python.
It relies only on `/proc` so it runs without extra dependencies on Linux.

## Usage

```bash
python3 oratop.py
```

Options:

- `--interval SECONDS` refresh interval (default: 1.0)
- `--count N` number of iterations before exit
- `--top N` number of processes to show
- `--sort {cpu,mem}` sort by CPU or memory (default: cpu)
- `--no-clear` disable ANSI clear between refreshes

Example:

```bash
python3 oratop.py --interval 2 --top 5 --sort mem
```
