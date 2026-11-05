#!/usr/bin/env python3
"""Layer-2 afb-CLV-BACKFILL Batch 1: 1x2 historic odds scrape + MySQL ingest.

Scope (Kris approved 2026-11-05 11:10): 11 PASS leagues x 2 recent seasons
(2024-2025 + 2023-2024), 1x2 FullTime, all bookmakers (closing/current).

Pipeline per league-season (checkpoint/resume via state.json):
  Stage A scrape   : oddsharvester historic (Patchright persistent context,
                     OH_PROFILE_DIR per worker for parallel safety) -> JSON
                     in data/clv_backfill/
  Stage B mapping  : scraped matches -> aifootballbets matches.match_id
                     (league + date +/-1d + normalized team names; fuzzy
                     fallback via difflib). Unmatched recorded, never guessed.
  Stage C insert   : afb_odds_snapshots INSERT IGNORE (T5 idempotency) via
                     afbdb.ledger.db_write_snapshots; counts scraped/
                     inserted/skipped/no-match logged.

Usage:
  python scripts/clv_backfill.py                # run all pending
  python scripts/clv_backfill.py --league "Premier League" --season 2024-2025
  python scripts/clv_backfill.py --worker 1     # parallel worker (own profile)
  python scripts/clv_backfill.py --list         # show state summary

Data only to repo data/ + appdb afb_odds_snapshots (no other DB writes).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]          # oddsharvester repo
AIFB_ROOT = Path("/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, str(AIFB_ROOT))                        # afbdb module

OUT_DIR = REPO_ROOT / "data" / "clv_backfill"
STATE_PATH = OUT_DIR / "state.json"

LEAGUE_SLUGS: Dict[str, str] = {
    "Premier League": "england-premier-league",
    "La Liga": "spain-laliga",
    "Serie A": "italy-serie-a",
    "Bundesliga": "germany-bundesliga",
    "Ligue 1": "france-ligue-1",
    "J1 League": "japan-j1-league",
    "K League 1": "south-korea-k-league-1",
    "A-League Men": "australia-a-league",
    "FA Cup": "england-fa-cup",
    "Copa del Rey": "spain-copa-del-rey",
    "Coppa Italia": "italy-coppa-italia",
}
SEASONS = ["2024-2025", "2023-2024"]

# Calendar-year leagues (verified via coverage probes 2026-11-05): OddsPortal
# seasons for J1/K League are single years ("2024"), not two-year labels.
# Map DB season label -> scrape season for these.
CALENDAR_YEAR_LEAGUES = {"J1 League", "K League 1"}


def scrape_season_for(league: str, season: str) -> str:
    """DB season label -> OddsPortal season string (calendar-year leagues use the start year)."""
    return season[:4] if league in CALENDAR_YEAR_LEAGUES else season
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
# A full league-season yields 100+ matches; anything below this is a partial
# scrape (CLI timeout / Node driver crash) and must not be trusted (2026-11-05).
MIN_RECORDS = 20
# Full-season 1x2 scrape with all bookmakers measured ~13s/match (~380 matches
# = ~85 min); 25 min previously killed jobs mid-run. 2h gives ample headroom.
SCRAPE_TIMEOUT_S = 7200


def norm_name(s: str) -> str:
    return " ".join((s or "").strip().lower().replace(".", "").replace("-", " ").split())


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def scrape_league_season(league: str, slug: str, season: str, worker: int, timeout_s: int) -> Optional[Path]:
    """Stage A: oddsharvester historic scrape -> JSON. Skips if output exists."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{slug}_{season}.json"
    if out.exists() and out.stat().st_size > 100:
        try:
            if len(json.loads(out.read_text(encoding="utf-8"))) >= MIN_RECORDS:
                print(f"  [scrape] skip (exists, {len(json.loads(out.read_text(encoding='utf-8')))} records): {out.name}")
                return out
        except (json.JSONDecodeError, ValueError):
            pass
        print(f"  [scrape] existing output is partial (<{MIN_RECORDS} records) - re-scraping")
    profile = REPO_ROOT / "data" / f".patchright-profile-w{worker}"
    cmd = [
        str(REPO_ROOT / ".venv" / "bin" / "python"), "-m", "oddsharvester.cli.cli",
        "historic", "-s", "football", "-l", slug, "--season", season,
        "-m", "1x2", "--headless",
        "--user-agent", UA, "--locale", "en-GB", "--timezone", "Europe/London",
        "-f", "json", "-o", str(out),
    ]
    import os

    env = os.environ.copy()
    env["OH_PROFILE_DIR"] = str(profile)
    # CRITICAL (2026-11-05): with checkpoints enabled the CLI wires
    # checkpoint_file_path = -o output path and rewrites the file with a
    # SINGLE record after every match (store_data(data=[data]) overwrites,
    # no append), then skips the final aggregate write. Result: output
    # always contains only the last scraped match. coverage_verify hit the
    # same bug earlier (commit 2824eeb) — disable checkpoints so the final
    # aggregate store_data writes the full result list.
    env["OH_CHECKPOINT_SAVE"] = "0"
    log = OUT_DIR / f"{slug}_{season}.log"
    print(f"  [scrape] {slug} {season} (worker {worker}, profile {profile.name}) ...")
    rc = None
    with log.open("w", encoding="utf-8") as fh:
        try:
            proc = subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=fh, stderr=subprocess.STDOUT,
                                  timeout=timeout_s, env=env)
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            print(f"  [scrape] TIMEOUT after {timeout_s}s")
            rc = -9
    if out.exists() and out.stat().st_size > 100:
        try:
            n = len(json.loads(out.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, ValueError):
            n = 0
        if n >= MIN_RECORDS:
            if rc != 0:
                # CLI exits non-zero on partial listing discovery (e.g. 1
                # listing page failed) even when 200+ match records were
                # scraped fine — measured 2026-11-05: PL 24-25 rc=1 with 230
                # valid records. Accept the data; log the coverage caveat.
                print(f"  [scrape] OK with rc={rc}: {out.name} ({n} records; some listing pages failed)")
            else:
                print(f"  [scrape] OK: {out.name} ({n} records)")
            return out
        print(f"  [scrape] FAILED: rc={rc}, only {n} records (<{MIN_RECORDS}) - see {log.name}")
        # quarantine partial output so the skip-check doesn't trust it
        out.rename(out.with_suffix(".partial.json"))
        return None
    print(f"  [scrape] FAILED or empty: rc={rc} - see {log.name}")
    return None


def load_aifb_matches(conn, league: str) -> Dict[str, dict]:
    """aifootballbets finished matches for the league, keyed by normalized teams+date."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.match_id, m.match_date, m.home_score, m.away_score,
               th.name AS home_team, ta.name AS away_team
        FROM matches m
        JOIN competitions c ON c.competition_id = m.competition_id
        JOIN teams th ON th.team_id = m.home_team_id
        JOIN teams ta ON ta.team_id = m.away_team_id
        WHERE c.name = %s AND m.status = 'finished'
        """,
        (league,),
    )
    out: Dict[str, dict] = {}
    for match_id, match_date, hs, as_, home, away in cur.fetchall():
        key = f"{norm_name(home)}|{norm_name(away)}|{match_date.isoformat()}"
        out[key] = {
            "match_id": str(match_id),
            "match_date": match_date.isoformat(),
            "home": home,
            "away": away,
        }
    return out


def _tokens(s: str) -> set:
    return set(norm_name(s).split())


def _name_match(a: str, b: str) -> bool:
    """Token-subset match: OddsPortal uses SHORT names ('Newcastle') vs DB full
    names ('Newcastle United') — a token subset in either direction matches."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return False
    return ta <= tb or tb <= ta


def map_match(rec: dict, aifb_index: Dict[str, dict]) -> Tuple[Optional[str], str]:
    """Map a scraped match record to an aifootballbets match_id.

    Tolerance: match_date +/- 1 day + token-subset team name matching on BOTH
    teams (the pair constraint disambiguates short forms like 'Newcastle' vs
    'Newcastle United' / 'Manchester City'). Returns (match_id or None, method).
    """
    try:
        # scraped match_date can be '2025-02-15 15:00:00 UTC' — take the date part
        scraped_date = date.fromisoformat(str(rec.get("match_date")).strip().split(" ")[0])
    except Exception:
        return None, "unparseable-date"
    home = norm_name(rec.get("home_team") or "")
    away = norm_name(rec.get("away_team") or "")
    if not home or not away:
        return None, "missing-teams"
    home_t, away_t = _tokens(home), _tokens(away)
    for day_offset in (0, -1, 1):
        d = (scraped_date + timedelta(days=day_offset)).isoformat()
        for key, hit in aifb_index.items():
            hh, aa, dd = key.split("|", 2)
            if dd != d:
                continue
            if (_tokens(hh) <= home_t or home_t <= _tokens(hh)) and (
                _tokens(aa) <= away_t or away_t <= _tokens(aa)
            ):
                if day_offset == 0:
                    return hit["match_id"], "exact"
                return hit["match_id"], "date-tolerance"
    return None, "no-match"


def insert_snapshots(conn, match_id: str, scraped_date: str, market: list) -> Tuple[int, int]:
    """Insert one match's 1x2 bookmaker odds into afb_odds_snapshots (INSERT IGNORE).

    Returns (rows_attempted, rows_inserted_by_db_estimate).
    """
    from afbdb.ledger import db_write_snapshots

    rows = []
    for entry in market:
        bookie = str(entry.get("bookmaker_name") or "unknown")[:64]
        for sel in ("1", "X", "2"):
            try:
                odds = float(entry[sel])
            except (KeyError, TypeError, ValueError):
                continue
            if not (1.01 <= odds <= 100):
                continue
            rows.append(
                {
                    "match_id": match_id,
                    "source": "oddsharvester_backfill",
                    "league_key": None,
                    "league_name": None,
                    "home_team": str(entry.get("home_team") or "")[:100] if entry.get("home_team") else None,
                    "away_team": None,
                    "commence_time_utc": None,
                    "market_key": "1X2",
                    "selection": sel,
                    "odds": odds,
                    "bookmaker": bookie,
                    "captured_at_utc": scraped_date,
                    "run_id": "clv_backfill_batch1",
                }
            )
    if not rows:
        return 0, 0
    written = db_write_snapshots(rows)
    return len(rows), written


def process_league_season(league: str, season: str, state: dict, worker: int, conn) -> dict:
    key = f"{league}|{season}"
    st = state.setdefault(key, {"stage": "pending"})
    stats = {"scraped": 0, "mapped": 0, "inserted": 0, "no_match": 0, "skipped_odds": 0}

    if st.get("stage") == "done":
        print(f"  [skip] {key}: done")
        return stats

    slug = LEAGUE_SLUGS[league]
    season = scrape_season_for(league, season)
    # ---- Stage A: scrape ----
    scraped_path = scrape_league_season(league, slug, season, worker, timeout_s=SCRAPE_TIMEOUT_S)
    if not scraped_path:
        st["stage"] = "scrape-failed"
        state[key] = st
        save_state(state)
        return stats

    # ---- Stage B: mapping ----
    data = json.loads(scraped_path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        st["stage"] = "done"
        st["note"] = "0 matches scraped"
        state[key] = st
        save_state(state)
        return stats
    stats["scraped"] = len(data)

    aifb_index = load_aifb_matches(conn, league)
    matched: Dict[str, dict] = {}
    for rec in data:
        match_id, method = map_match(rec, aifb_index)
        if match_id:
            stats["mapped"] += 1
            matched[match_id] = rec
        else:
            stats["no_match"] += 1
    print(f"  [map] {len(data)} scraped -> {stats['mapped']} matched, {stats['no_match']} no-match")

    # ---- Stage C: insert ----
    total_rows = inserted = 0
    for match_id, rec in matched.items():
        market = rec.get("1x2_market") or []
        if not isinstance(market, list) or not market:
            stats["skipped_odds"] += 1
            continue
        scraped_date = str(rec.get("scraped_date") or "")
        attempted, written = insert_snapshots(conn, match_id, scraped_date, market)
        total_rows += attempted
        inserted += written
    conn.commit()
    stats["inserted"] = inserted
    stats["snapshot_rows"] = total_rows
    print(f"  [insert] rows attempted {total_rows}, db accepted {inserted}")

    st.update({"stage": "done", "stats": stats})
    state[key] = st
    save_state(state)
    return stats


def main() -> int:
    p = argparse.ArgumentParser(description="afb-CLV-BACKFILL Batch 1: 1x2 odds scrape + MySQL ingest")
    p.add_argument("--league", type=str, action="append", default=None)
    p.add_argument("--season", type=str, default=None)
    p.add_argument("--worker", type=int, default=1, help="worker id (own Patchright profile)")
    p.add_argument("--list", action="store_true")
    args = p.parse_args()

    # Per-worker state file: two workers sharing one state.json clobber each
    # other's checkpoints (observed 2026-11-05; worker 1 entries lost).
    global STATE_PATH
    if args.worker > 1:
        STATE_PATH = OUT_DIR / f"state-w{args.worker}.json"

    state = load_state()
    if args.list:
        for k, v in sorted(state.items()):
            print(f"  {k}: {v.get('stage')} {v.get('stats', '')}")
        return 0

    from afbdb.connection import afb_mysql_connect

    conn = afb_mysql_connect()

    jobs: List[Tuple[str, str]] = []
    for league in LEAGUE_SLUGS:
        for season in SEASONS:
            if (not args.league or league in args.league) and (not args.season or season == args.season):
                jobs.append((league, season))

    totals: Dict[str, int] = {}
    started = time.time()
    for i, (league, season) in enumerate(jobs):
        print(f"\n=== [{i + 1}/{len(jobs)}] {league} | {season} ===")
        try:
            stats = process_league_season(league, season, state, args.worker, conn)
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + int(v)
        except Exception as exc:
            print(f"  [ERROR] {league} {season}: {exc}")
            state.setdefault(f"{league}|{season}", {})["stage"] = "error"
            state[f"{league}|{season}"]["error"] = str(exc)[:300]
            save_state(state)
    elapsed = time.time() - started

    print(f"\n=== BATCH TOTALS: {totals} | elapsed {elapsed / 60:.1f} min ===")
    save_state(state)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
