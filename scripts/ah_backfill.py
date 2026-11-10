#!/usr/bin/env python3
"""afb-T10-AH (Kris approved 2026-11-07): Asian Handicap historic odds scrape + MySQL ingest.

Scope: same league-seasons as CLV Batch 1+2 (~5,430 matches with 1x2), AH
FullTime, all bookmakers, open->close sequence. NO backtest / CLV computation
anywhere in this script (Kris: scraping first, backtest needs separate approval).

Pipeline per league-season (checkpoint/resume via state.json, mirrors
clv_backfill.py):
  Stage A scrape   : oddsharvester historic -m asian_handicap (Patchright
                     persistent context, OH_PROFILE_DIR per worker,
                     OH_CHECKPOINT_SAVE=0) -> JSON in data/ah_backfill/
  Stage B mapping  : scraped matches -> aifootballbets matches.match_id via
                     the learned alias table (data/clv_backfill/alias_table.json,
                     99.9% mechanism) + token-subset fold matching + PHASE7
                     side-swap fallback. Unmatched recorded, never guessed.
  Stage C insert   : afb_odds_snapshots INSERT IGNORE (T5 idempotency) via
                     afbdb.ledger.db_write_snapshots. Convention (aligns with
                     the_odds_api dual-write in aifootballbets ingest/odds.py):
                       market_key = "AH <line>"   e.g. "AH -0.25", "AH 1.5"
                       selection  = "home" | "away"   (line is HOME handicap)
                       run_id     = "clv_backfill_ah"

Usage:
  python scripts/ah_backfill.py                # run all pending
  python scripts/ah_backfill.py --league "Australia Cup" --season 2023
  python scripts/ah_backfill.py --worker 1     # parallel worker (own profile)
  python scripts/ah_backfill.py --list         # state summary
  python scripts/ah_backfill.py --dry-run-insert  # Stage B only, no DB writes

Data only to repo data/ + appdb afb_odds_snapshots (no other DB writes).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]          # oddsharvester repo
AIFB_ROOT = Path("/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, str(AIFB_ROOT))                        # afbdb module

# clv_backfill provides LEAGUE_SLUGS / SEASON_* / season-candidate logic
_spec = importlib.util.spec_from_file_location("clv_backfill", REPO_ROOT / "scripts" / "clv_backfill.py")
clv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(clv)

OUT_DIR = REPO_ROOT / "data" / "ah_backfill"
STATE_PATH = OUT_DIR / "state.json"
ALIAS_PATH = REPO_ROOT / "data" / "clv_backfill" / "alias_table.json"
RUN_ID = "clv_backfill_ah"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
# Same partial-scrape guard as 1x2 backfill (2026-11-05 lesson): a full
# league-season yields 20+ matches; below this the output is partial.
MIN_RECORDS = 20
# afb-T10-AH (2026-11-07): preview-only (passive) AH capture measured
# ~15-20s/match (no per-line clicks; the AH tab renders ALL lines in one
# table). Full-line click mode measured 2.6-4.5 min/match -> multi-day runs
# that also risk the aggregate-write-at-end losing everything on timeout
# (Australia Cup 2023 lost exactly this way). 4h timeout is ample headroom.
SCRAPE_TIMEOUT_S = 14400


# ---------------- state helpers ----------------

def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------- name matching (clv_remap 99.9% mechanism) ----------------

def norm(s: str) -> str:
    return clv.norm_name(s)


def fold(s: str) -> str:
    """ASCII-fold accents + PHASE6 explicit chars + Utd->united (from clv_remap)."""
    import unicodedata
    s = (s.replace("ə", "a").replace("Ə", "A")
           .replace("ø", "o").replace("Ø", "O")
           .replace("đ", "d").replace("Đ", "D")
           .replace("ð", "d").replace("Ð", "D")
           .replace("ł", "l").replace("Ł", "L")
           .replace("ß", "ss"))
    for abbr, full in (("Utd", "united"), ("utd", "united")):
        s = s.replace(abbr, full)
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def toks(s: str) -> set:
    return set(fold(norm(s)).split())


def name_match(a: str, b: str) -> bool:
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return False
    return ta <= tb or tb <= ta


def load_db_index(conn, league: str) -> Dict[str, list]:
    """{date_iso: [(match_id, home, away, hs, as_), ...]} for the competition."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.match_id, m.match_date, th.name, ta.name, m.home_score, m.away_score
        FROM matches m
        JOIN competitions c ON c.competition_id = m.competition_id
        JOIN teams th ON th.team_id = m.home_team_id
        JOIN teams ta ON ta.team_id = m.away_team_id
        WHERE c.name = %s AND m.status = 'finished'
        """,
        (league,),
    )
    idx: Dict[str, list] = {}
    for mid, d, h, a, hs, as_ in cur.fetchall():
        idx.setdefault(d.isoformat(), []).append((str(mid), h, a, hs, as_))
    return idx


def load_alias_table() -> Dict[str, Dict[str, str]]:
    """{league: {folded_norm: db_name}} from the CLV 1x2 remap run."""
    if not ALIAS_PATH.exists():
        return {}
    return json.loads(ALIAS_PATH.read_text(encoding="utf-8"))


def match_pair(h_scrape: str, a_scrape: str, db_home: str, db_away: str, alias: dict) -> bool:
    def resolves(scrape: str, db: str) -> bool:
        if name_match(scrape, db):
            return True
        return alias.get(fold(norm(scrape))) == db
    return resolves(h_scrape, db_home) and resolves(a_scrape, db_away)


def _parse_rec_date(rec: dict) -> Optional[date]:
    try:
        return date.fromisoformat(str(rec.get("match_date")).strip().split(" ")[0])
    except Exception:
        return None


def map_match(rec: dict, idx: Dict[str, list], alias: dict) -> Tuple[Optional[str], str]:
    """Map a scraped AH record to a match_id (date +/-1d, both-side token/alias
    match, PHASE7 side-swap fallback). Returns (match_id|None, method)."""
    d = _parse_rec_date(rec)
    if d is None:
        return None, "unparseable-date"
    h, a = rec.get("home_team") or "", rec.get("away_team") or ""
    if not h or not a:
        return None, "missing-teams"
    for off in (0, -1, 1):
        dd = (d + timedelta(days=off)).isoformat()
        cands = [m for m in idx.get(dd, []) if match_pair(h, a, m[1], m[2], alias)]
        if len(cands) == 1:
            return cands[0][0], "direct" if off == 0 else "date-tolerance"
        if len(cands) > 1:
            return None, "ambiguous"
    # PHASE7 side-swap: OddsPortal occasionally lists cup ties home/away flipped
    for off in (0, -1, 1):
        dd = (d + timedelta(days=off)).isoformat()
        cands = [m for m in idx.get(dd, []) if match_pair(a, h, m[1], m[2], alias)]
        if len(cands) == 1:
            rec["home_team"], rec["away_team"] = a, h
            sc_h, sc_a = rec.get("home_score"), rec.get("away_score")
            rec["home_score"], rec["away_score"] = sc_a, sc_h
            return cands[0][0], "side-swap"
        if len(cands) > 1:
            break
    return None, "no-match"


# ---------------- Stage A: scrape ----------------

def scrape_league_season(slug: str, season: str, worker: int, timeout_s: int) -> Optional[Path]:
    """oddsharvester historic AH scrape -> JSON. Skips valid existing output."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{slug}_{season}.json"
    if out.exists() and out.stat().st_size > 100:
        try:
            n = len(json.loads(out.read_text(encoding="utf-8")))
            if n >= MIN_RECORDS:
                print(f"  [scrape] skip (exists, {n} records): {out.name}")
                return out
        except (json.JSONDecodeError, ValueError):
            pass
        print(f"  [scrape] existing output partial (<{MIN_RECORDS}) - re-scraping")
    profile = REPO_ROOT / "data" / f".patchright-profile-ahw{worker}"
    cmd = [
        str(REPO_ROOT / ".venv" / "bin" / "python"), "-m", "oddsharvester.cli.cli",
        "historic", "-s", "football", "-l", slug, "--season", season,
        "-m", "asian_handicap", "--preview-only", "--headless",
        "--user-agent", UA, "--locale", "en-GB", "--timezone", "Europe/London",
        "-f", "json", "-o", str(out),
    ]
    env = os.environ.copy()
    env["OH_PROFILE_DIR"] = str(profile)
    # CRITICAL (2026-11-05, same as 1x2 backfill): checkpoints rewrite the
    # output with a SINGLE record after every match and skip the final
    # aggregate write. Disable so the aggregate store_data writes everything.
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
                # CLI exits non-zero on partial listing failures even with
                # valid records (measured 2026-11-05: PL 24-25 rc=1, 230 records).
                print(f"  [scrape] OK with rc={rc}: {out.name} ({n} records)")
            else:
                print(f"  [scrape] OK: {out.name} ({n} records)")
            return out
        print(f"  [scrape] FAILED: rc={rc}, only {n} records (<{MIN_RECORDS}) - see {log.name}")
        out.rename(out.with_suffix(".partial.json"))
        return None
    print(f"  [scrape] FAILED or empty: rc={rc} - see {log.name}")
    return None


# ---------------- Stage C: insert ----------------

def _ah_line_from_token(token: str) -> Optional[str]:
    """'asian_handicap_-3_75' -> '-3.75'; 'asian_handicap_+1_25' -> '1.25'.

    Normalized to str(float) so market_key matches the the_odds_api convention
    (positive lines carry no '+'). None for unparseable tokens.
    """
    raw = token.replace("asian_handicap_", "")
    try:
        val = float(raw.replace("_", "."))
    except ValueError:
        return None
    return str(val)


_AH_LINE_RE = re.compile(r"Asian Handicap\s*([-+]?[0-9]+(?:\.[0-9]+)?)")


def extract_ah_entries(rec: dict) -> List[Tuple[str, List[dict]]]:
    """Collect one entry list per DISTINCT AH line present in a record.

    Preview-only (passive) mode distributes the SAME all-lines table under
    every expanded line-token key; the true line lives in each entry's
    submarket_name ('Asian Handicap -3.5'). Dedupe by (submarket_name, odds):
    one quote per line (OddsPortal average view; no bookmaker breakdown).
    Full-line (click) mode entries carry real per-token lines - same dedupe
    is safe there because each token's entries share one submarket_name.
    """
    merged: Dict[Tuple[str, str, str], dict] = {}
    order: List[Tuple[str, str, str]] = []
    for key, val in rec.items():
        if not key.startswith("asian_handicap_") or not key.endswith("_market"):
            continue
        if not isinstance(val, list):
            continue
        token_line = _ah_line_from_token(key[: -len("_market")])
        for entry in val:
            if not isinstance(entry, dict):
                continue
            sm = str(entry.get("submarket_name") or "")
            m = _AH_LINE_RE.search(sm)
            line = str(float(m.group(1))) if m else token_line
            if line is None:
                continue
            k = (line, str(entry.get("team1_handicap")), str(entry.get("team2_handicap")))
            if k not in merged:
                merged[k] = entry
                order.append(k)
    by_line: Dict[str, List[dict]] = {}
    for line, o1, o2 in order:
        by_line.setdefault(line, []).append(merged[(line, o1, o2)])
    return sorted(by_line.items(), key=lambda kv: float(kv[0]))


def insert_snapshots(conn, match_id: str, scraped_date: str, rec: dict) -> Tuple[int, int]:
    """Insert one match's AH bookmaker odds into afb_odds_snapshots (INSERT IGNORE).

    Returns (rows_attempted, rows_inserted_by_db_estimate).
    """
    from afbdb.ledger import db_write_snapshots

    rows = []
    for line, entries in extract_ah_entries(rec):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            # preview-only passive rows carry no bookmaker breakdown (one
            # quote per line = OddsPortal average view) - label honestly.
            bookie = str(entry.get("bookmaker_name") or "average")[:64]
            for sel, key in (("home", "team1_handicap"), ("away", "team2_handicap")):
                try:
                    odds = float(entry[key])
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
                        "market_key": f"AH {line}",
                        "selection": sel,
                        "odds": odds,
                        "bookmaker": bookie,
                        "captured_at_utc": scraped_date,
                        "run_id": RUN_ID,
                    }
                )
    if not rows:
        return 0, 0
    written = db_write_snapshots(rows)
    return len(rows), written


# ---------------- per league-season orchestration ----------------

def process_league_season(league: str, season_label: str, state: dict, worker: int,
                          conn, dry_insert: bool = False) -> dict:
    key = f"{league}|{season_label}"
    st = state.setdefault(key, {"stage": "pending"})
    stats = {"scraped": 0, "mapped": 0, "inserted": 0, "no_match": 0, "skipped_odds": 0, "rows": 0}

    if st.get("stage") == "done":
        print(f"  [skip] {key}: done")
        return stats

    slug = clv.scrape_slug_for(league, season_label)
    # ---- Stage A: scrape (try season candidates in order) ----
    scraped_path = None
    for cand in clv.scrape_season_candidates(league, season_label):
        scraped_path = scrape_league_season(slug, cand, worker, timeout_s=SCRAPE_TIMEOUT_S)
        if scraped_path:
            break
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

    idx = load_db_index(conn, league)
    alias = ALIAS_GLOBAL.get(league, {})
    matched: Dict[str, dict] = {}
    methods: Dict[str, int] = {}
    for rec in data:
        match_id, method = map_match(rec, idx, alias)
        if match_id:
            stats["mapped"] += 1
            methods[method] = methods.get(method, 0) + 1
            matched[match_id] = rec
        else:
            stats["no_match"] += 1
    print(f"  [map] {len(data)} scraped -> {stats['mapped']} matched, {stats['no_match']} no-match "
          f"({', '.join(f'{k}={v}' for k, v in sorted(methods.items()))})")

    # ---- Stage C: insert ----
    total_rows = inserted = 0
    for match_id, rec in matched.items():
        entries = extract_ah_entries(rec)
        if not entries:
            stats["skipped_odds"] += 1
            continue
        scraped_date = str(rec.get("scraped_date") or "")
        if dry_insert:
            for _, es in entries:
                for e in es:
                    if isinstance(e, dict):
                        total_rows += 2  # home + away potential
            continue
        attempted, written = insert_snapshots(conn, match_id, scraped_date, rec)
        total_rows += attempted
        inserted += written
    stats["rows"] = total_rows
    stats["inserted"] = inserted
    st["stage"] = "done"
    st.update({k: stats[k] for k in ("scraped", "mapped", "no_match", "rows", "inserted")})
    state[key] = st
    save_state(state)
    print(f"  [insert] {key}: {total_rows} rows attempted, {inserted} written"
          + (" (dry-run)" if dry_insert else ""))
    return stats


ALIAS_GLOBAL: Dict[str, Dict[str, str]] = {}


def main() -> int:
    global ALIAS_GLOBAL
    p = argparse.ArgumentParser()
    p.add_argument("--league", action="append", help="restrict to league name(s)")
    p.add_argument("--season", action="append", help="restrict to DB season label(s)")
    p.add_argument("--worker", type=int, default=1)
    p.add_argument("--list", action="store_true")
    p.add_argument("--dry-run-insert", action="store_true",
                   help="Stage A+B only: scrape + map + count rows, no DB writes")
    args = p.parse_args()

    # league x season work list: Batch 1+2 pairs plus any --season labels
    # (afb-T13: 2025-2026 OOS season is not in clv.SEASONS - union them so
    # the --season filter cannot silently produce an empty work list).
    season_labels = sorted(set(list(clv.SEASONS) + list(args.season or [])))
    pairs: List[Tuple[str, str]] = []
    for league in clv.LEAGUE_SLUGS:
        for season in season_labels:
            pairs.append((league, season))
    if args.league:
        pairs = [x for x in pairs if x[0] in set(args.league)]
    if args.season:
        pairs = [x for x in pairs if x[1] in set(args.season)]

    if args.list:
        state = load_state()
        for key in sorted(state):
            print(f"{key}: {state[key]}")
        return 0

    ALIAS_GLOBAL = load_alias_table()
    n_alias = sum(len(v) for v in ALIAS_GLOBAL.values())
    print(f"[ah_backfill] aliases loaded: {n_alias} across {len(ALIAS_GLOBAL)} leagues")

    conn = None
    if not args.dry_run_insert:
        from afbdb.connection import afb_mysql_connect
        conn = afb_mysql_connect()

    state = load_state()
    totals = {"scraped": 0, "mapped": 0, "inserted": 0, "no_match": 0, "skipped_odds": 0, "rows": 0}
    for league, season_label in pairs:
        try:
            s = process_league_season(league, season_label, state, args.worker, conn,
                                      dry_insert=args.dry_run_insert)
        except Exception as exc:
            print(f"  [error] {league}|{season_label}: {exc!r}")
            continue
        for k in totals:
            totals[k] += s.get(k, 0)
    print(f"[ah_backfill] worker {args.worker} totals: {totals}")
    if conn:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
