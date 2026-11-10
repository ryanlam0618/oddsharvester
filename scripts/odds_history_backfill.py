#!/usr/bin/env python3
"""afb-T13 Phase 0: odds-history backfill runner (1x2 → AH, tiered near→far).

Runs INSIDE the afb-ah-scraper-tmp container (chromium + venv + pymysql there).
Matches the T12c-oh smoke findings:
- 1x2 odds-history: active per-bookmaker hover, ~6 min/match
- AH odds-history: umbrella full-market mode, ~14.6 min/match (the proven
  878s path; single-line clicks are fragile)

Pipeline per match:
  1. oddsharvester CLI historic --match-link -m <market> --odds-history → JSON
  2. parse odds_history_data → per-bookmaker timestamped points + opening odds
  3. block→selection mapping by cell order (1x2: 1/X/2; AH: home/away) —
     KNOWN RISK: a failed mid-row modal shifts the mapping for the remaining
     cells (documented; the per-Tier quality spot-check cross-checks the last
     point vs the closing snapshot odds)
  4. INSERT IGNORE into afb_odds_history_points (the ONLY writable table)

Worker checkpoint/resume: per-worker state file (T10 lesson). Partial JSON
quarantine. Rate limit: the CLI's internal waits are the rate pattern; 5s
inter-match sleep.

Zero writes to any other table.
"""
from __future__ import annotations

import argparse
import glob
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
AIFB_ROOT = Path("/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, str(AIFB_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import pymysql  # noqa: E402

from afbdb.connection import afb_mysql_connect  # noqa: E402

# clv_backfill provides LEAGUE_SLUGS / map_match / norm helpers
_spec = importlib.util.spec_from_file_location("clv_backfill", REPO_ROOT / "scripts" / "clv_backfill.py")
clv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(clv)

OUT_DIR = REPO_ROOT / "data" / "oh_backfill"
QUARANTINE = OUT_DIR / "quarantine"
STATE_DIR = OUT_DIR
RUN_ID = "oh_backfill_t13"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
SCRAPE_TIMEOUT_S = 1500        # 25 min per match (AH umbrella can take ~15)
INTER_MATCH_SLEEP_S = 5        # rate-limit courtesy between matches


# --------------------------------------------------------------- match list

def load_match_list(tier_seasons: List[str]) -> List[dict]:
    """Tier-1 match list from the EXISTING T10/T13 AH scrape JSONs (they carry
    match_link + date + names for the 20 comps x these seasons). Zero new
    listing scrapes needed for Tier 1."""
    out: Dict[str, dict] = {}
    for season in tier_seasons:
        for f in glob.glob(str(REPO_ROOT / "data" / "ah_backfill" / f"*_{season}.json")):
            try:
                d = json.load(open(f, encoding="utf-8"))
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(d, list):
                continue
            for rec in d:
                ml = rec.get("match_link") or ""
                if not ml:
                    continue
                out[ml] = {
                    "match_link": ml,
                    "date": str(rec.get("match_date", ""))[:10],
                    "home": rec.get("home_team") or "",
                    "away": rec.get("away_team") or "",
                    "league": rec.get("league_name") or "",
                    "season": season,
                }
    return sorted(out.values(), key=lambda r: (r["date"], r["match_link"]))


# ------------------------------------------------------- match_id resolution

def build_resolver(conn) -> Dict[str, Dict[str, dict]]:
    """{date_iso: [(match_id, home, away, league)]} for the 20 comps."""
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_id, m.match_date, th.name, ta.name, c.name
        FROM matches m
        JOIN teams th ON th.team_id = m.home_team_id
        JOIN teams ta ON ta.team_id = m.away_team_id
        JOIN competitions c ON c.competition_id = m.competition_id
        WHERE m.status='finished' AND m.home_score IS NOT NULL
    """)
    idx: Dict[str, List[dict]] = {}
    for mid, d, h, a, lg in cur.fetchall():
        idx.setdefault(str(d)[:10], []).append(
            {"match_id": str(mid), "home": h, "away": a, "league": lg})
    return idx


def resolve_match_id(rec: dict, idx: Dict[str, List[dict]]) -> Optional[str]:
    d = rec["date"]
    home_t = set(clv.norm_name(rec["home"]).split())
    away_t = set(clv.norm_name(rec["away"]).split())
    for off in (0, -1, 1):
        dd = (date.fromisoformat(d) + timedelta(days=off)).isoformat()
        for cand in idx.get(dd, []):
            ch = set(clv.norm_name(cand["home"]).split())
            ca = set(clv.norm_name(cand["away"]).split())
            if (home_t <= ch or ch <= home_t) and (away_t <= ca or ca <= away_t):
                return cand["match_id"]
    return None


# ------------------------------------------------------------- odds parsing

def parse_history_rows(rec: dict, match_id: str, market: str, run_id: str) -> List[dict]:
    """odds_history_data → afb_odds_history_points rows.

    Block→selection mapping by cell order: 1x2 = 1/X/2; AH = home/away.
    KNOWN RISK: a failed mid-row modal shifts the mapping (documented)."""
    rows: List[dict] = []
    selections = ["1", "X", "2"] if market == "1X2" else ["home", "away"]
    mk_key = [k for k in rec if k.endswith("_market")]
    for k in mk_key:
        for entry in rec.get(k) or []:
            bookmaker = str(entry.get("bookmaker_name") or "unknown")[:64]
            blocks = entry.get("odds_history_data") or []
            line = ""
            if market == "AH":
                m = rec.get("match_info") or {}
                line = _line_from_submarket(entry.get("submarket_name"))
            for bi, block in enumerate(blocks):
                if bi >= len(selections):
                    break
                selection = selections[bi]
                opening = block.get("opening_odds") or {}
                if opening.get("odds") is not None and opening.get("timestamp"):
                    rows.append(_row(match_id, market, bookmaker, selection, line,
                                     opening["timestamp"], float(opening["odds"]),
                                     1, run_id))
                for pnt in block.get("odds_history") or []:
                    if pnt.get("odds") is None or not pnt.get("timestamp"):
                        continue
                    rows.append(_row(match_id, market, bookmaker, selection, line,
                                     pnt["timestamp"], float(pnt["odds"]), 0, run_id))
    return rows


def _line_from_submarket(name: str | None) -> str:
    if not name:
        return ""
    # "Asian Handicap -0.5" -> "-0.5"
    parts = name.split()
    return parts[-1] if len(parts) > 1 else ""


def _row(match_id: str, market: str, bookmaker: str, selection: str, line: str,
         ts: str, odds: float, is_opening: int, run_id: str) -> dict:
    return {"match_id": match_id, "market": market, "bookmaker": bookmaker,
            "selection": selection, "line": line[:32], "ts": ts[:19],
            "odds": odds, "is_opening": is_opening, "run_id": run_id}


# ------------------------------------------------------------- DB insert

def insert_rows(conn, rows: List[dict]) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    q = """INSERT IGNORE INTO afb_odds_history_points
           (match_id, market, bookmaker, selection, line, ts, odds, is_opening, run_id)
           VALUES (%(match_id)s, %(market)s, %(bookmaker)s, %(selection)s,
                   %(line)s, %(ts)s, %(odds)s, %(is_opening)s, %(run_id)s)"""
    written = 0
    for r in rows:
        cur.execute(q, r)
        written += cur.rowcount
    conn.commit()
    return written


# --------------------------------------------------------------- per match

def scrape_match(cli_python: str, rec: dict, market: str, out_json: Path,
                 profile: str) -> Tuple[bool, float]:
    start = time.time()
    env = os.environ.copy()
    env["OH_PROFILE_DIR"] = profile
    env["OH_CHECKPOINT_SAVE"] = "0"
    slug = _league_slug(rec["league"])
    season = _season_for(rec)
    cmd = [cli_python, "-m", "oddsharvester.cli.cli", "historic", "-s", "football",
           "-l", slug, "--season", season,
           "--match-link", rec["match_link"],
           "-m", "1x2" if market == "1X2" else "asian_handicap",
           "--odds-history", "--headless",
           "--user-agent", UA, "--locale", "en-GB", "--timezone", "Europe/London",
           "-f", "json", "-o", str(out_json)]
    log = out_json.with_suffix(".log")
    try:
        with log.open("w", encoding="utf-8") as fh:
            subprocess.run(cmd, cwd=str(REPO_ROOT), stdout=fh, stderr=subprocess.STDOUT,
                           timeout=SCRAPE_TIMEOUT_S, env=env)
    except subprocess.TimeoutExpired:
        return False, time.time() - start
    ok = out_json.exists() and out_json.stat().st_size > 100
    return ok, time.time() - start


_SLUG_OVERRIDES = {"J1 League": "japan-j1-league", "K League 1": "south-korea-k-league-1"}


def _league_slug(league: str) -> str:
    base = league.lower().replace(" ", "-").replace("'", "")
    overrides = {
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
        "UCL": "champions-league",
        "UEL": "europa-league",
        "UECL": "conference-league",
        "EFL Cup": "england-efl-cup",
        "Chinese Super League": "china-super-league",
        "AFC Champions League": "afc-champions-league",
        "AFC Champions League Two": "afc-champions-league-2",
        "Emperor's Cup": "japan-emperors-cup",
        "Australia Cup": "australia-australia-cup",
    }
    return overrides.get(league, base)


def _season_for(rec: dict) -> str:
    league = rec["league"]
    d = rec["date"]
    if league in ("J1 League", "K League 1", "Chinese Super League",
                  "Emperor's Cup", "Australia Cup"):
        return d[:4]
    # European seasons: Aug YYYY -> Jun YYYY+1
    y = int(d[:4])
    return f"{y}-{y + 1}" if int(d[5:7]) >= 7 else f"{y - 1}-{y}"


# ----------------------------------------------------------------- workers

def process_partition(worker: int, leagues: List[str], matches: List[dict],
                      market: str, conn) -> dict:
    cli_python = str(REPO_ROOT / ".venv" / "bin" / "python")
    state_path = STATE_DIR / f"state_w{worker}_{market}.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"done": []}
    done = set(state["done"])
    profile = str(REPO_ROOT / "data" / f".patchright-profile-ohw{worker}")
    my_matches = [m for m in matches if m["league"] in set(leagues)]
    stats = {"bets": 0, "rows": 0, "skipped_id": 0, "failed": 0}
    for i, rec in enumerate(my_matches):
        if rec["match_link"] in done:
            continue
        match_id = resolve_match_id(rec, RESOLVER_IDX)
        if not match_id:
            stats["skipped_id"] += 1
            done.add(rec["match_link"])
            state["done"] = list(done)
            state_path.write_text(json.dumps(state))
            continue
        out_json = OUT_DIR / f"m{worker}_{i}.json"
        ok, elapsed = scrape_match(cli_python, rec, market, out_json, profile)
        if not ok:
            stats["failed"] += 1
            # quarantine
            QUARANTINE.mkdir(parents=True, exist_ok=True)
            if out_json.exists():
                shutil.move(str(out_json), str(QUARANTINE / out_json.name))
            done.add(rec["match_link"])
            state["done"] = list(done)
            state_path.write_text(json.dumps(state))
            time.sleep(INTER_MATCH_SLEEP_S)
            continue
        try:
            rec_data = json.load(open(out_json, encoding="utf-8"))[0]
            rows = parse_history_rows(rec_data, match_id, market, RUN_ID)
            written = insert_rows(conn, rows)
            stats["bets"] += 1
            stats["rows"] += written
        except (json.JSONDecodeError, IndexError, KeyError) as exc:
            stats["failed"] += 1
            QUARANTINE.mkdir(parents=True, exist_ok=True)
            if out_json.exists():
                shutil.move(str(out_json), str(QUARANTINE / out_json.name))
        # cleanup the per-match scrape temp (keep host clean)
        if out_json.exists():
            out_json.unlink()
        lg_path = out_json.with_suffix(".log")
        if lg_path.exists():
            lg_path.unlink()
        done.add(rec["match_link"])
        state["done"] = list(done)
        state_path.write_text(json.dumps(state))
        time.sleep(INTER_MATCH_SLEEP_S)
    return stats


RESOLVER_IDX: Dict[str, List[dict]] = {}


def main() -> int:
    p = argparse.ArgumentParser(description="T13 Phase 0 odds-history backfill")
    p.add_argument("--tier", choices=["1x2", "ah"], required=True)
    p.add_argument("--worker", type=int, default=1)
    p.add_argument("--leagues", type=str, default="",
                   help="comma-separated league partition (default: all)")
    args = p.parse_args()

    market = "1X2" if args.tier == "1x2" else "AH"
    tier_seasons = ["2023-2024", "2024-2025", "2025-2026"]

    conn = afb_mysql_connect(ensure_schema=False)

    global RESOLVER_IDX
    RESOLVER_IDX = build_resolver(conn)

    matches = load_match_list(tier_seasons)
    leagues = [l for l in args.leagues.split("|") if l] or sorted(
        {m["league"] for m in matches})
    print(f"[tier {args.tier}] worker {args.worker}: {len(matches)} matches, "
          f"leagues {leagues}")

    stats = process_partition(args.worker, leagues, matches, market, conn)
    print(f"[done] worker {args.worker} {args.tier}: {stats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
