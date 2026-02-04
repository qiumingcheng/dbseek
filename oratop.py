#!/usr/bin/env python3
"""Lightweight oratop-like terminal monitor.

This script provides a quick, Oracle-oratop-inspired snapshot of system
CPU/memory/load and top processes using only /proc (no external deps).
"""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


HZ = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
CPU_COUNT = os.cpu_count() or 1


@dataclass
class ProcSample:
    pid: int
    name: str
    state: str
    cpu_time: int
    rss_kb: int


@dataclass
class Snapshot:
    total_cpu: List[int]
    proc: Dict[int, ProcSample]


def read_cpu_times() -> List[int]:
    with open("/proc/stat", "r", encoding="utf-8") as handle:
        line = handle.readline()
    parts = line.split()[1:]
    return [int(value) for value in parts]


def cpu_usage(prev: List[int], curr: List[int]) -> float:
    prev_total = sum(prev)
    curr_total = sum(curr)
    total_delta = curr_total - prev_total
    if total_delta <= 0:
        return 0.0
    prev_idle = prev[3] + prev[4]
    curr_idle = curr[3] + curr[4]
    idle_delta = curr_idle - prev_idle
    return max(0.0, min(100.0, (total_delta - idle_delta) / total_delta * 100.0))


def read_meminfo() -> Tuple[int, int]:
    total = 0
    available = 0
    with open("/proc/meminfo", "r", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("MemTotal:"):
                total = int(line.split()[1])
            elif line.startswith("MemAvailable:"):
                available = int(line.split()[1])
            if total and available:
                break
    return total, available


def read_loadavg() -> Tuple[str, str, str]:
    with open("/proc/loadavg", "r", encoding="utf-8") as handle:
        parts = handle.read().split()
    return parts[0], parts[1], parts[2]


def read_uptime() -> float:
    with open("/proc/uptime", "r", encoding="utf-8") as handle:
        uptime_seconds = float(handle.read().split()[0])
    return uptime_seconds


def read_proc_counts() -> Tuple[int, int]:
    running = 0
    blocked = 0
    with open("/proc/stat", "r", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("procs_running"):
                running = int(line.split()[1])
            elif line.startswith("procs_blocked"):
                blocked = int(line.split()[1])
            if running and blocked:
                break
    return running, blocked


def iter_process_samples() -> Iterable[ProcSample]:
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        stat_path = os.path.join("/proc", entry, "stat")
        try:
            with open(stat_path, "r", encoding="utf-8") as handle:
                stat = handle.read()
        except (FileNotFoundError, PermissionError):
            continue

        try:
            start = stat.index("(")
            end = stat.rindex(")")
            name = stat[start + 1 : end]
            fields = stat[end + 2 :].split()
            state = fields[0]
            utime = int(fields[11])
            stime = int(fields[12])
            rss_pages = int(fields[21])
        except (ValueError, IndexError):
            continue

        rss_kb = rss_pages * (os.sysconf("SC_PAGE_SIZE") // 1024)
        yield ProcSample(
            pid=pid,
            name=name,
            state=state,
            cpu_time=utime + stime,
            rss_kb=rss_kb,
        )


def snapshot() -> Snapshot:
    cpu = read_cpu_times()
    proc = {sample.pid: sample for sample in iter_process_samples()}
    return Snapshot(total_cpu=cpu, proc=proc)


def format_header(cpu_pct: float, mem_total: int, mem_avail: int) -> str:
    mem_used = mem_total - mem_avail
    mem_pct = (mem_used / mem_total * 100.0) if mem_total else 0.0
    load1, load5, load15 = read_loadavg()
    uptime = read_uptime()
    running, blocked = read_proc_counts()
    return (
        f"CPU {cpu_pct:5.1f}% | "
        f"MEM {mem_used/1024:,.0f}M/{mem_total/1024:,.0f}M "
        f"({mem_pct:4.1f}%) | "
        f"LOAD {load1} {load5} {load15} | "
        f"UP {uptime/3600:,.1f}h | "
        f"PROCS {running} run/{blocked} blk"
    )


def proc_delta(prev: Snapshot, curr: Snapshot) -> List[Tuple[ProcSample, float]]:
    total_delta = sum(curr.total_cpu) - sum(prev.total_cpu)
    results: List[Tuple[ProcSample, float]] = []
    for pid, sample in curr.proc.items():
        prev_sample = prev.proc.get(pid)
        if not prev_sample or total_delta <= 0:
            cpu_pct = 0.0
        else:
            cpu_delta = sample.cpu_time - prev_sample.cpu_time
            cpu_pct = max(0.0, cpu_delta / total_delta * 100.0 * CPU_COUNT)
        results.append((sample, cpu_pct))
    return results


def print_screen(
    header: str,
    rows: List[Tuple[ProcSample, float]],
    top_n: int,
    mem_total: int,
    use_clear: bool,
) -> None:
    if use_clear:
        print("\033[H\033[J", end="")
    print(header)
    print("PID     CPU%   MEM%   RSS(MB)  STATE  NAME")
    for sample, cpu_pct in rows[:top_n]:
        mem_pct = (sample.rss_kb / mem_total * 100.0) if mem_total else 0.0
        print(
            f"{sample.pid:<7}"
            f"{cpu_pct:6.1f}  "
            f"{mem_pct:5.1f}  "
            f"{sample.rss_kb/1024:7.1f}  "
            f"{sample.state:^5}  "
            f"{sample.name}"
        )


def run(interval: float, count: int | None, top_n: int, sort_key: str, use_clear: bool) -> None:
    prev = snapshot()
    iterations = 0
    while True:
        time.sleep(interval)
        curr = snapshot()
        cpu_pct = cpu_usage(prev.total_cpu, curr.total_cpu)
        mem_total, mem_avail = read_meminfo()
        header = format_header(cpu_pct, mem_total, mem_avail)
        rows = proc_delta(prev, curr)
        if sort_key == "mem":
            rows = sorted(rows, key=lambda item: item[0].rss_kb, reverse=True)
        else:
            rows = sorted(rows, key=lambda item: item[1], reverse=True)
        print_screen(header, rows, top_n, mem_total, use_clear)
        prev = curr
        iterations += 1
        if count is not None and iterations >= count:
            break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Oratop-like system monitor")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval seconds")
    parser.add_argument("--count", type=int, default=None, help="Number of iterations")
    parser.add_argument("--top", type=int, default=10, help="Top process count")
    parser.add_argument(
        "--sort",
        choices=("cpu", "mem"),
        default="cpu",
        help="Sort by cpu or memory",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Disable ANSI clear between refreshes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        run(
            interval=args.interval,
            count=args.count,
            top_n=args.top,
            sort_key=args.sort,
            use_clear=not args.no_clear,
        )
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
