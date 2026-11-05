#!/usr/bin/env python3
"""Layer-2 afb-ODDSH-COVERAGE Phase 2: 24+ league OddsPortal support verification.

Per competition (from the T5 coverage inventory):
1. Try candidate OddsPortal slugs (primary + fallbacks) with a LINKS-ONLY
   probe for one sampled season (staggered across 2015-16..2024-25).
2. First slug/season with matches -> sample up to N matches (default 10) via
   --match-links-file full 1x2 odds scrape.
3. Validate: bookmaker count per match, odds range 1.01-100.
4. Status: PASS (>=5 valid matches) / PARTIAL (1-4) / NO-DATA (0 matches,
   slug exists) / UNSUPPORTED (no slug worked).
5. Checkpoint state after every step (data/coverage_verify/state.json) —
   fully resumable; completed leagues are skipped on re-run.

Sequential by design (persistent context = single profile); wall-clock cap
via --total-hours (default 8h). Data only to repo data/ — no DB writes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
INVENTORY = Path("/root/.openclaw/workspace-coding/aifootballbets/reports/odds_coverage_inventory.json")
STATE_PATH = REPO_ROOT / "data" / "coverage_verify" / "state.json"
OUT_DIR = REPO_ROOT / "data" / "coverage_verify"
VENV_PY = REPO_ROOT / ".venv" / "bin" / "python"

# DB competition name -> candidate OddsPortal slugs (primary first)
SLUG_CANDIDATES: Dict[str, List[str]] = {
    "Premier League": ["england-premier-league"],
    "La Liga": ["spain-laliga", "spain-la-liga"],
    "Serie A": ["italy-serie-a"],
    "Bundesliga": ["germany-bundesliga"],
    "Ligue 1": ["france-ligue-1"],
    "J1 League": ["japan-j1-league"],
    "K League 1": ["south-korea-k-league-1", "korea-republic-k-league-1"],
    "A-League Men": ["australia-a-league", "australia-a-league-men"],
    "Chinese Super League": ["china-super-league"],
    "FA Cup": ["england-fa-cup"],
    "Copa del Rey": ["spain-copa-del-rey"],
    "Coppa Italia": ["italy-coppa-italia"],
    "Coupe de France": ["france-coupe-de-france"],
    "DFB Pokal": ["germany-dfb-pokal"],
    "EFL Cup": ["england-efl-cup"],
    "Emperor's Cup": ["japan-emperors-cup"],
    "J.League Cup": ["japan-j-league-cup"],
    "Australia Cup": ["australia-australia-cup"],
    "Chinese FA Cup": ["china-chinese-fa-cup"],
    "UCL": ["champions-league"],
    "UEL": ["europa-league"],
    "UECL": ["conference-league"],
    "AFC Champions League": ["afc-champions-league", "afc-champions-league-elite"],
    "AFC Champions League Two": ["afc-champions-league-2", "afc-cup"],
}

# Two-year (league seasons) vs calendar-year (most cups) season formats
# Verified 2026-11-05 via links-only probes (data/slugfix/):
#   CSL, AFC CL (pre-2024), AFC Cup / ACL Two (pre-2024) use calendar years.
CALENDAR_YEAR_CUPS = {
    "Australia Cup", "Emperor's Cup", "Chinese FA Cup",
    "Chinese Super League", "AFC Champions League", "AFC Champions League Two",
    "J.League Cup",
}


def season_formats(sample_year: int, league: str) -> List[str]:
    """Candidate season strings for a league, best guess first."""
    if league in CALENDAR_YEAR_CUPS:
        return [str(sample_year), f"{sample_year - 1}-{sample_year}"]
    return [f"{sample_year - 1}-{sample_year}", str(sample_year)]


def load_inventory() -> List[dict]:
    return json.loads(INVENTORY.read_text(encoding="utf-8"))


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def run_cli(args: List[str], timeout_s: int, log_path: Path, env_overrides: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    import os

    cmd = [str(VENV_PY), "-m", "oddsharvester.cli.cli"] + args
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    with log_path.open("w", encoding="utf-8") as fh:
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=fh, stderr=subprocess.STDOUT, timeout=timeout_s, env=env)
    return proc.returncode, log_path.read_text(encoding="utf-8", errors="replace")


def links_probe(sport: str, slug: str, season: str, out_json: Path, log: Path, timeout_s: int) -> int:
    """Links-only probe; returns the number of match links found (0 = none)."""
    args = [
        "historic", "-s", sport, "-l", slug, "--season", season,
        "-m", "1x2", "--links-only", "--headless",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "--locale", "en-GB", "--timezone", "Europe/London",
        "-f", "json", "-o", str(out_json),
    ]
    rc, _log_text = run_cli(args, timeout_s, log)
    if not out_json.exists():
        return 0
    try:
        data = json.loads(out_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 0
    links = []
    for rec in data if isinstance(data, list) else []:
        link = rec.get("match_link") if isinstance(rec, dict) else None
        if link:
            links.append(link)
    print(f"    [links] {slug} {season}: rc={rc} links={len(links)}")
    return len(links)


def sample_odds(sport: str, season: str, links_file: Path, out_json: Path, log: Path, timeout_s: int, max_matches: int) -> Optional[dict]:
    """Full 1x2 scrape for up to max_matches links; returns the parsed output."""
    links = [ln.strip() for ln in links_file.read_text(encoding="utf-8").splitlines() if ln.strip()][:max_matches]
    if not links:
        return None
    links_sample = links_file.with_suffix(".sample.txt")
    links_sample.write_text("\n".join(links), encoding="utf-8")
    args = [
        "historic", "-s", sport,
        "--season", season,
        "--match-links-file", str(links_sample),
        "-m", "1x2", "--headless",
        # NOTE: checkpoint save is controlled by the OH_CHECKPOINT_SAVE env
        # var (no CLI flag) — set to 0 for this subprocess so store_data runs
        # (with checkpoint+file_path set, the CLI skips store_data assuming
        # per-match checkpoints wrote the data, but checkpoints don't run in
        # match-links mode -> data was lost).
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "--locale", "en-GB", "--timezone", "Europe/London",
        "-f", "json", "-o", str(out_json),
    ]
    run_cli(args, timeout_s, log, env_overrides={"OH_CHECKPOINT_SAVE": "0"})
    if not out_json.exists():
        return None
    try:
        return json.loads(out_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def validate_sample(data: Optional[dict]) -> dict:
    if not data or not isinstance(data, list):
        return {"matches": 0, "valid_matches": 0, "bookmakers_max": 0, "odds_min": None, "odds_max": None, "out_of_range": 0}
    valid = 0
    bookies_max = 0
    all_odds: List[float] = []
    out_of_range = 0
    for rec in data:
        m = rec.get("1x2_market") or []
        if isinstance(m, list) and m:
            valid += 1
        bookies_max = max(bookies_max, len(m) if isinstance(m, list) else 0)
        for entry in m if isinstance(m, list) else []:
            for k in ("1", "X", "2"):
                try:
                    v = float(entry[k])
                    all_odds.append(v)
                    if not (1.01 <= v <= 100):
                        out_of_range += 1
                except (KeyError, TypeError, ValueError):
                    pass
    return {
        "matches": len(data),
        "valid_matches": valid,
        "bookmakers_max": bookies_max,
        "odds_min": round(min(all_odds), 3) if all_odds else None,
        "odds_max": round(max(all_odds), 3) if all_odds else None,
        "out_of_range": out_of_range,
    }


def verify_league(league: dict, idx: int, args, state: dict) -> dict:
    name = league["competition"]
    sport = "football"
    # stagger seasons: league idx -> sample year (2016..2025)
    sample_year = 2016 + (idx % 10)
    formats = season_formats(sample_year, name)
    candidates = SLUG_CANDIDATES.get(name, [])
    league_state = state.setdefault(name, {"status": "pending"})

    if league_state.get("status") in ("done", "unsupported", "no-data"):
        print(f"[skip] {name}: already {league_state.get('status')}")
        return league_state

    # ---- stage 1: slug probe (links-only) ----
    if not league_state.get("slug"):
        for slug in candidates:
            for season in formats:
                if time.time() > args.deadline_ts:
                    print("[deadline] wall clock cap reached mid-probe")
                    league_state["status"] = "pending"
                    state[name] = league_state
                    save_state(state)
                    return league_state
                out_json = OUT_DIR / f"{slug}_{season}_links.json"
                log = OUT_DIR / f"{slug}_{season}_links.log"
                n = links_probe(sport, slug, season, out_json, log, args.links_timeout)
                if n > 0:
                    league_state.update({"slug": slug, "season": season, "match_count": n,
                                         "links_json": str(out_json), "status": "links-ok"})
                    save_state(state)
                    break
                time.sleep(2)
            if league_state.get("slug"):
                break
        if not league_state.get("slug"):
            league_state.update({"status": "unsupported",
                                 "tried": [f"{s} x {fmt}" for s in candidates for fmt in formats]})
            state[name] = league_state
            save_state(state)
            print(f"[UNSUPPORTED] {name}: no slug/season produced matches")
            return league_state

    # ---- stage 2: sampled odds verification (<= max-matches) ----
    if league_state.get("status") != "done":
        slug = league_state["slug"]
        season = league_state["season"]
        links_json = Path(league_state["links_json"])
        links_file = links_json.with_suffix(".links.txt")
        data = json.loads(links_json.read_text(encoding="utf-8"))
        links = [rec.get("match_link") for rec in data if isinstance(rec, dict) and rec.get("match_link")]
        links_file.write_text("\n".join(links), encoding="utf-8")
        sample_out = OUT_DIR / f"{slug}_{season}_odds.json"
        sample_log = OUT_DIR / f"{slug}_{season}_odds.log"
        sample = sample_odds(sport, season, links_file, sample_out, sample_log, args.sample_timeout, args.max_matches)
        validation = validate_sample(sample)
        status = "PASS" if validation["valid_matches"] >= 5 else ("PARTIAL" if validation["valid_matches"] >= 1 else "NO-DATA")
        league_state.update({
            "status": "done",
            "result": status,
            "validation": validation,
            "sample_json": str(sample_out),
        })
        state[name] = league_state
        save_state(state)
        print(f"[{status}] {name} ({slug} {season}): {validation}")
    return league_state


def main() -> int:
    p = argparse.ArgumentParser(description="24+ league OddsPortal support verification (afb-ODDSH-COVERAGE Phase 2)")
    p.add_argument("--max-matches", type=int, default=10)
    p.add_argument("--links-timeout", type=int, default=240)
    p.add_argument("--sample-timeout", type=int, default=900)
    p.add_argument("--total-hours", type=float, default=8.0)
    p.add_argument("--only", type=str, default=None, help="verify a single competition name (debug)")
    args = p.parse_args()

    args.deadline_ts = time.time() + args.total_hours * 3600
    inventory = load_inventory()
    state = load_state()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    todo = [lg for lg in inventory if (not args.only or lg["competition"] == args.only)]
    print(f"[coverage] {len(todo)} competitions to verify (cap {args.total_hours}h)")

    for idx, league in enumerate(todo):
        name = league["competition"]
        print(f"\n=== [{idx + 1}/{len(todo)}] {name} (finished: {league['finished_matches']}) ===")
        try:
            verify_league(league, idx, args, state)
        except Exception as exc:
            print(f"[ERROR] {name}: {exc}")
            state.setdefault(name, {})["status"] = "error"
            state.setdefault(name, {})["error"] = str(exc)
            save_state(state)

    # final summary (partial: only leagues processed so far)
    summary = {name: st.get("result") or st.get("status") for name, st in state.items()}
    counts: Dict[str, int] = {}
    for v in summary.values():
        counts[v] = counts.get(v, 0) + 1
    report = {
        "meta": {"generated": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
                 "counts": counts, "deadline_respected": time.time() <= args.deadline_ts},
        "leagues": state,
    }
    out_json = REPO_ROOT / "reports" / "odds_coverage_verify_report.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n[done] summary counts: {counts}")
    print(f"[done] wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
