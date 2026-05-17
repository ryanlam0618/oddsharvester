#!/usr/bin/env python3
"""Orchestrator v1 for large historic football scraping batches.

This tool plans and runs many `oddsharvester historic` jobs safely with:
- dry-run planning
- resumable state tracking
- market grouping
- conservative job-level concurrency
- predictable repo-local artifacts

It intentionally shells out to the existing OddsHarvester CLI rather than changing
core scraper behavior.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN = REPO_ROOT / "orchestrator" / "plans" / "football_historic_v1.json"
ARTIFACTS_ROOT = REPO_ROOT / "orchestrator" / "artifacts"
STATE_ROOT = ARTIFACTS_ROOT / "state"
LOG_ROOT = ARTIFACTS_ROOT / "logs"
RUNS_ROOT = ARTIFACTS_ROOT / "runs"
DEFAULT_STATE_FILE = STATE_ROOT / "football_historic_v1_state.json"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "orchestrator_v1"
DEFAULT_JOB_CONCURRENCY = 1
DEFAULT_INNER_CONCURRENCY = 2
DEFAULT_REQUEST_DELAY = 3.0  # Increased for less aggressive scraping
DEFAULT_PROXY_LIST = REPO_ROOT / "data" / "proxy_webshare.txt"

# Proxy pool for round-robin rotation
_proxy_pool: list[tuple[str, str, str]] = []
_proxy_index = 0
_proxy_lock = threading.Lock()


def load_proxy_pool(path: Path) -> list[tuple[str, str, str]]:
    """Parse proxy file (IP:PORT:USER:PASS per line) into (server, user, passwd) tuples."""
    if not path.exists():
        return []
    pool = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) != 4:
                continue
            ip, port, user, pwd = parts
            server = f"http://{ip}:{port}"
            pool.append((server, user, pwd))
    return pool


def next_proxy(pool: list[tuple[str, str, str]]) -> tuple[str, str, str] | None:
    global _proxy_index
    if not pool:
        return None
    with _proxy_lock:
        p = pool[_proxy_index % len(pool)]
        _proxy_index += 1
        return p


@dataclass(frozen=True)
class Job:
    job_id: str
    sport: str
    league: str
    season: str
    market_group: str
    markets: list[str]
    output_path: Path
    log_path: Path
    run_dir: Path
    cli_args: list[str]


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        # Inter-process lock: open state file itself as lock file
        self._lock_path = path.parent / (path.name + ".lock")
        self._lock_fd = None
        self.data = self._load()

    def _acquire(self) -> None:
        """Acquire exclusive inter-process lock."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_fd = self._lock_path.open("w")
        fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX)

    def _release(self) -> None:
        """Release inter-process lock."""
        if self._lock_fd:
            fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
            self._lock_fd.close()
            self._lock_fd = None

    def _default(self) -> dict[str, Any]:
        return {
            "version": 1,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "runs": [],
            "jobs": {},
        }

    def _load(self) -> dict[str, Any]:
        self._acquire()
        try:
            if not self.path.exists():
                self.path.parent.mkdir(parents=True, exist_ok=True)
                data = self._default()
                self._atomic_write(data)
                return data

            with self.path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        finally:
            self._release()

    def _atomic_write(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        import uuid
        temp_path = self.path.parent / (self.path.name + f".{uuid.uuid4().hex[:8]}.tmp")
        try:
            with temp_path.open("w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, sort_keys=True)
                handle.write("\n")
            temp_path.replace(self.path)
        except FileNotFoundError:
            if self.path.exists():
                pass
            else:
                raise

    def save(self) -> None:
        with self._lock:
            self._acquire()
            try:
                self.data["updated_at"] = utc_now()
                self._atomic_write(self.data)
            finally:
                self._release()

    def add_run(self, run_info: dict[str, Any]) -> None:
        with self._lock:
            self._acquire()
            try:
                self.data.setdefault("runs", []).append(run_info)
                self.data["updated_at"] = utc_now()
                self._atomic_write(self.data)
            finally:
                self._release()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self.data.get("jobs", {}).get(job_id)

    def update_job(self, job_id: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._acquire()
            try:
                # Read fresh from disk to avoid read-modify-write race
                with self.path.open("r", encoding="utf-8") as handle:
                    disk_data = json.load(handle)
                jobs = disk_data.setdefault("jobs", {})
                current = jobs.get(job_id, {})
                current.update(payload)
                current["updated_at"] = utc_now()
                jobs[job_id] = current
                disk_data["updated_at"] = utc_now()
                self._atomic_write(disk_data)
            finally:
                self._release()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OddsHarvester historic football orchestrator v1")
    parser.add_argument("--plan", default=str(DEFAULT_PLAN), help="Path to plan JSON file")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="Path to resumable state JSON file")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Root directory for scraped outputs")
    parser.add_argument("--job-concurrency", type=int, default=DEFAULT_JOB_CONCURRENCY, help="Concurrent CLI jobs")
    parser.add_argument("--inner-concurrency", type=int, default=DEFAULT_INNER_CONCURRENCY, help="Per-job oddsharvester concurrency")
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY, help="Per-job request delay")
    parser.add_argument("--headless", action="store_true", help="Pass --headless to oddsharvester historic")
    parser.add_argument("--odds-history", action="store_true", help="Pass --odds-history to oddsharvester historic")
    parser.add_argument("--max-pages", type=int, help="Optional max pages per job for cautious runs")
    parser.add_argument("--dry-run", action="store_true", help="Render plan and commands without executing")
    parser.add_argument("--resume", action="store_true", help="Skip successful jobs already present in state file")
    parser.add_argument("--fail-fast", action="store_true", help="Stop scheduling new work after first failed job")
    parser.add_argument("--limit", type=int, help="Limit queued jobs after filtering")
    parser.add_argument("--market-group", action="append", dest="market_groups", help="Only include named market group(s)")
    parser.add_argument("--league", action="append", dest="leagues", help="Only include league slug(s)")
    parser.add_argument("--season", action="append", dest="seasons", help="Only include season(s)")
    parser.add_argument("--sample-job", action="store_true", help="Only queue the first matching job")
    parser.add_argument("--show-plan", action="store_true", help="Print detailed job list")
    parser.add_argument("--proxy-list", type=Path, default=DEFAULT_PROXY_LIST, help="Path to proxy pool file (IP:PORT:USER:PASS per line)")
    parser.add_argument("--retry-empty", action="store_true", help="Jobs returning 0 matches are treated as success (not failure)")
    return parser.parse_args()


def load_plan(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        plan = json.load(handle)

    if plan.get("sport") != "football":
        raise ValueError("This v1 orchestrator currently expects a football plan")

    return plan


def build_jobs(plan: dict[str, Any], args: argparse.Namespace, run_id: str) -> list[Job]:
    output_root = Path(args.output_root)
    jobs: list[Job] = []
    selected_groups = set(args.market_groups or [])
    selected_leagues = set(args.leagues or [])
    selected_seasons = set(args.seasons or [])

    for league in plan["leagues"]:
        league_slug = league["slug"]
        if selected_leagues and league_slug not in selected_leagues:
            continue

        for season in plan["seasons"]:
            if selected_seasons and season not in selected_seasons:
                continue

            for group in plan["market_groups"]:
                group_name = group["name"]
                if selected_groups and group_name not in selected_groups:
                    continue

                job_id = f"{league_slug}__{season}__{group_name}"
                run_dir = RUNS_ROOT / run_id
                output_path = output_root / league_slug / season / f"{group_name}.json"
                log_path = LOG_ROOT / run_id / f"{job_id}.log"
                cli_args = [
                    sys.executable,
                    "-m",
                    "oddsharvester.cli.cli",
                    "historic",
                    "-s",
                    plan["sport"],
                    "-l",
                    league_slug,
                    "--season",
                    season,
                    "-m",
                    ",".join(group["markets"]),
                    "-f",
                    "json",
                    "-o",
                    str(output_path),
                    "-c",
                    str(args.inner_concurrency),
                    "--request-delay",
                    str(args.request_delay),
                ]
                if args.headless:
                    cli_args.append("--headless")
                if args.odds_history:
                    cli_args.append("--odds-history")
                if args.max_pages is not None:
                    cli_args.extend(["--max-pages", str(args.max_pages)])
                # Round-robin proxy for this job
                pool = load_proxy_pool(args.proxy_list)
                if pool:
                    p = next_proxy(pool)
                    if p:
                        cli_args.extend(["--proxy-url", p[0], "--proxy-user", p[1], "--proxy-pass", p[2]])

                jobs.append(
                    Job(
                        job_id=job_id,
                        sport=plan["sport"],
                        league=league_slug,
                        season=season,
                        market_group=group_name,
                        markets=list(group["markets"]),
                        output_path=output_path,
                        log_path=log_path,
                        run_dir=run_dir,
                        cli_args=cli_args,
                    )
                )

    if args.sample_job and jobs:
        jobs = jobs[:1]
    if args.limit is not None:
        jobs = jobs[: args.limit]

    return jobs


def filter_resume_jobs(jobs: list[Job], state: StateStore) -> list[Job]:
    filtered: list[Job] = []
    for job in jobs:
        existing = state.get_job(job.job_id)
        if existing and existing.get("status") == "success":
            continue
        filtered.append(job)
    return filtered


def ensure_layout(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def summarize_plan(plan: dict[str, Any], jobs: list[Job]) -> dict[str, Any]:
    unique_leagues = sorted({job.league for job in jobs})
    unique_seasons = sorted({job.season for job in jobs})
    unique_groups = sorted({job.market_group for job in jobs})
    total_market_invocations = sum(len(job.markets) for job in jobs)
    return {
        "plan_name": plan.get("name"),
        "sport": plan.get("sport"),
        "jobs": len(jobs),
        "unique_leagues": len(unique_leagues),
        "unique_seasons": len(unique_seasons),
        "unique_market_groups": len(unique_groups),
        "total_market_invocations": total_market_invocations,
        "job_concurrency": None,
    }


def print_plan(plan: dict[str, Any], jobs: list[Job], detailed: bool) -> None:
    summary = summarize_plan(plan, jobs)
    print(json.dumps(summary, indent=2))
    if detailed:
        for job in jobs:
            print(
                json.dumps(
                    {
                        "job_id": job.job_id,
                        "league": job.league,
                        "season": job.season,
                        "market_group": job.market_group,
                        "markets": job.markets,
                        "output": str(job.output_path.relative_to(REPO_ROOT)),
                    }
                )
            )


def run_job(job: Job, state: StateStore, args: argparse.Namespace) -> dict[str, Any]:
    ensure_layout([job.output_path.parent, job.log_path.parent, job.run_dir])
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")

    command_str = shell_join(job.cli_args)
    state.update_job(
        job.job_id,
        {
            "job_id": job.job_id,
            "league": job.league,
            "season": job.season,
            "market_group": job.market_group,
            "markets": job.markets,
            "status": "running",
            "started_at": utc_now(),
            "output_path": str(job.output_path),
            "log_path": str(job.log_path),
            "command": command_str,
        },
    )

    started = time.time()
    with job.log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"# job_id: {job.job_id}\n")
        log_handle.write(f"# started_at: {utc_now()}\n")
        log_handle.write(f"# command: {command_str}\n\n")
        log_handle.flush()
        process = subprocess.run(
            job.cli_args,
            cwd=REPO_ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

    elapsed = round(time.time() - started, 2)

    # Check if job failed due to 0 matches (empty result)
    job_failed = process.returncode != 0
    treat_as_success = False
    if job_failed and args.retry_empty:
        # Scan log for "0 event rows" pattern
        try:
            log_text = job.log_path.read_text(encoding="utf-8", errors="replace")
            if "Found 0 event rows on page" in log_text or "Extracted 0 unique matches from event rows" in log_text:
                treat_as_success = True
                print(f"  → 0 matches (empty page): treating as success (--retry-empty)")
        except Exception:
            pass

    status = "success" if (not job_failed or treat_as_success) else "failed"
    result = {
        "job_id": job.job_id,
        "status": status,
        "return_code": process.returncode,
        "finished_at": utc_now(),
        "duration_seconds": elapsed,
        "output_exists": job.output_path.exists(),
        "output_size_bytes": job.output_path.stat().st_size if job.output_path.exists() else 0,
        "log_path": str(job.log_path),
    }
    state.update_job(job.job_id, result)
    return result


def execute_jobs(jobs: list[Job], state: StateStore, args: argparse.Namespace) -> dict[str, Any]:
    if not jobs:
        return {"scheduled": 0, "success": 0, "failed": 0, "results": []}

    results: list[dict[str, Any]] = []
    stop_scheduling = False
    pending_iter = iter(jobs)

    with ThreadPoolExecutor(max_workers=max(1, args.job_concurrency)) as pool:
        futures = {}
        while True:
            while not stop_scheduling and len(futures) < max(1, args.job_concurrency):
                try:
                    job = next(pending_iter)
                except StopIteration:
                    break
                futures[pool.submit(run_job, job, state, args)] = job

            if not futures:
                break

            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                job = futures.pop(future)
                result = future.result()
                results.append(result)
                print(f"[{result['status']}] {job.job_id} rc={result['return_code']} log={job.log_path}")
                if args.fail_fast and result["status"] != "success":
                    stop_scheduling = True

    success = sum(1 for item in results if item["status"] == "success")
    failed = sum(1 for item in results if item["status"] != "success")
    return {"scheduled": len(jobs), "success": success, "failed": failed, "results": results}


def shell_join(args: list[str]) -> str:
    return shlex.join(args)


def main() -> int:
    args = parse_args()
    plan_path = Path(args.plan).resolve()
    state_file = Path(args.state_file).resolve()
    ensure_layout([ARTIFACTS_ROOT, STATE_ROOT, LOG_ROOT, RUNS_ROOT, Path(args.output_root)])

    plan = load_plan(plan_path)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state = StateStore(state_file)

    jobs = build_jobs(plan, args, run_id)
    if args.resume:
        jobs = filter_resume_jobs(jobs, state)
    summary = summarize_plan(plan, jobs)
    summary["job_concurrency"] = args.job_concurrency

    state.add_run(
        {
            "run_id": run_id,
            "started_at": utc_now(),
            "dry_run": args.dry_run,
            "resume": args.resume,
            "job_concurrency": args.job_concurrency,
            "inner_concurrency": args.inner_concurrency,
            "request_delay": args.request_delay,
            "plan_path": str(plan_path),
            "state_file": str(state_file),
            "jobs_scheduled": len(jobs),
        }
    )

    if args.dry_run or args.show_plan:
        print_plan(plan, jobs, detailed=args.show_plan)

    if args.dry_run:
        return 0

    result = execute_jobs(jobs, state, args)
    print(json.dumps(result, indent=2))
    return 0 if result["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
