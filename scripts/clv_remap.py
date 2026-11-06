#!/usr/bin/env python3
"""afb-CLV-BACKFILL Task 1: alias-based remap of unmatched scraped records.

Re-scrapes nothing: loads the 22 preserved scrape outputs, learns
OddsPortal-name -> DB-name aliases from unambiguous co-occurrence evidence,
re-matches, and INSERT IGNOREs the newly matched odds.

Alias learning (per competition, multi-round):
  For each unmatched record (date, home, away):
    - if one side resolves to a known DB club (token-subset or learned alias)
      and exactly ONE DB match in this competition happened on that date
      (+/-1) involving that club -> the other DB side of that match becomes
      the alias for the unknown scraped name.
  Conflicting aliases (one scraped name -> 2 DB clubs) are dropped.
  Iterate until no new aliases are learned.

Usage:
  python3 scripts/clv_remap.py            # learn + remap + insert + report
  python3 scripts/clv_remap.py --dry-run  # learn + remap, no DB writes
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AIFB_ROOT = Path("/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, str(AIFB_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import importlib.util

_spec = importlib.util.spec_from_file_location("clv_backfill", REPO_ROOT / "scripts" / "clv_backfill.py")
clv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(clv)

OUT_DIR = REPO_ROOT / "data" / "clv_backfill"
ALIAS_PATH = OUT_DIR / "alias_table.json"
NEW_RUN_ID = "clv_backfill_remap1"


def norm(s: str) -> str:
    return clv.norm_name(s)


def fold(s: str) -> str:
    """ASCII-fold accents so 'Atlético' == 'Atletico' at token level."""
    import unicodedata

    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def toks(s: str) -> set:
    return set(fold(norm(s)).split())


def name_match(a: str, b: str) -> bool:
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return False
    return ta <= tb or tb <= ta


def load_db_index(conn, league: str):
    """{date_iso: [(match_id, home, away), ...]} for the competition."""
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
    idx: dict[str, list] = {}
    for mid, d, h, a, hs, as_ in cur.fetchall():
        idx.setdefault(d.isoformat(), []).append((str(mid), h, a, hs, as_))
    return idx


def load_scrape_files():
    """[(league, scrape_file, records)] for all preserved outputs."""
    from clv_backfill import LEAGUE_SLUGS  # noqa: F401  (import for parity)

    slug_to_league = {v: k for k, v in clv.LEAGUE_SLUGS.items()}
    # extra slugs written under calendar-year scrape seasons
    slug_to_league.setdefault("japan-j1-league", "J1 League")
    slug_to_league.setdefault("south-korea-k-league-1", "K League 1")
    files = sorted(
        f for f in glob.glob(str(OUT_DIR / "*.json"))
        if "state" not in f and "partial" not in f and "bak" not in f and "alias" not in f
    )
    out = []
    for f in files:
        stem = Path(f).stem  # e.g. france-ligue-1_2023-2024
        slug = stem.rsplit("_", 1)[0]
        league = slug_to_league.get(slug)
        if not league:
            print(f"  [skip] unknown slug in {Path(f).name}")
            continue
        try:
            records = json.loads(Path(f).read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(f"  [skip] unparseable {Path(f).name}")
            continue
        if isinstance(records, list) and records:
            out.append((league, f, records))
    return out


def match_pair(h_scrape: str, a_scrape: str, db_home: str, db_away: str, alias: dict) -> bool:
    """Both-side match: token-subset or alias-resolved, folding accents.

    `alias` must be a FLAT {folded_norm: db_name} dict for this league
    (the caller derives it from the (league, key)-keyed global table).
    """
    def resolves(scrape: str, db: str) -> bool:
        if name_match(scrape, db):
            return True
        return alias.get(fold(norm(scrape))) == db

    return resolves(h_scrape, db_home) and resolves(a_scrape, db_away)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    from afbdb.connection import afb_mysql_connect

    conn = afb_mysql_connect()

    leagues = sorted({lg for lg, _, _ in load_scrape_files()})
    db_idx = {lg: load_db_index(conn, lg) for lg in leagues}

    # alias: (league, folded-normalized scraped name) -> DB club name
    alias: dict[tuple, str] = {}
    loaded = 0
    if ALIAS_PATH.exists():
        saved = json.loads(ALIAS_PATH.read_text(encoding="utf-8"))
        for lg, m in saved.items():
            for k, v in m.items():
                alias[(lg, k)] = v
                loaded += 1
    print(f"aliases loaded from disk: {loaded}")

    # ---- multi-round alias learning + matching ----
    stats_by_file = {}
    for rnd in range(1, 6):
        new_aliases = 0
        pending = []
        # per-league FLAT alias views for match_pair (fixes string-vs-tuple key bug)
        league_alias_views = {}
        for lg in leagues:
            league_alias_views[lg] = {k: v for (l2, k), v in alias.items() if l2 == lg}
        for league, f, records in load_scrape_files():
            idx = db_idx[league]
            la = league_alias_views[league]
            matched, unmatched = [], []
            for rec in records:
                try:
                    d = date.fromisoformat(str(rec.get("match_date")).strip().split(" ")[0])
                except Exception:
                    unmatched.append(rec)
                    continue
                h, a = rec.get("home_team") or "", rec.get("away_team") or ""
                hit = None
                for off in (0, -1, 1):
                    dd = (d + timedelta(days=off)).isoformat()
                    cands = [m for m in idx.get(dd, []) if match_pair(h, a, m[1], m[2], la)]
                    if len(cands) == 1:
                        hit = (cands[0], off)
                        break
                    if len(cands) > 1:
                        # pair matched multiple DB matches (duplicate fixture rows?)
                        hit = ("AMBIG", cands)
                        break
                if isinstance(hit, tuple) and hit and hit[0] != "AMBIG":
                    matched.append((rec, hit[0][0]))
                elif hit == ("AMBIG",):
                    unmatched.append(rec)
                else:
                    unmatched.append(rec)
            # learn aliases from unmatched records with one resolvable side
            for rec in unmatched:
                try:
                    d = date.fromisoformat(str(rec.get("match_date")).strip().split(" ")[0])
                except Exception:
                    continue
                h, a = rec.get("home_team") or "", rec.get("away_team") or ""
                hn, an = fold(norm(h)), fold(norm(a))
                known = {
                    hn: alias.get((league, hn)),
                    an: alias.get((league, an)),
                }
                # token-subset resolution against this competition's clubs
                clubs = {c for ms in idx.values() for _, hname, aname, _, _ in ms for c in (hname, aname)}
                for side, scraped in ((hn, h), (an, a)):
                    if known[side]:
                        continue
                    cands = [c for c in clubs if name_match(scraped, c)]
                    if len(cands) == 1:
                        known[side] = cands[0]
                anchor_side = hn if known[hn] and not known[an] else (an if known[an] and not known[hn] else None)
                if not anchor_side:
                    continue
                anchor_club = known[anchor_side]
                # Score fingerprint: scrape records carry the real result; a
                # learned alias must match the DB scores too, otherwise the
                # anchor hit the wrong match on that date (observed pollution:
                # 'osasuna' -> 'Cádiz').
                sc_h, sc_a = rec.get("home_score"), rec.get("away_score")
                try:
                    sc_h, sc_a = int(sc_h), int(sc_a)
                except (TypeError, ValueError):
                    sc_h = sc_a = None
                for off in (0, -1, 1):
                    dd = (d + timedelta(days=off)).isoformat()
                    cands = [m for m in idx.get(dd, []) if anchor_club in (m[1], m[2])]
                    if sc_h is not None and sc_a is not None:
                        cands = [m for m in cands
                                 if m[3] is not None and m[4] is not None
                                 and (m[3], m[4]) in {(sc_h, sc_a), (sc_a, sc_h)}]
                    if len(cands) == 1:
                        mid, dh, da = cands[0][:3]
                        other = da if anchor_club == dh else dh
                        unknown_scraped = a if anchor_side == hn else h
                        unknown_norm = an if anchor_side == hn else hn
                        key = (league, unknown_norm)
                        if alias.get(key, other) != other:
                            # conflict -> ambiguous, drop
                            alias.pop(key, None)
                        else:
                            if alias.get(key) != other:
                                alias[key] = other
                                new_aliases += 1
                        break
            stats_by_file[f] = (league, len(records), len(matched), len(unmatched))
            pending.append((league, f, matched, unmatched))
        print(f"round {rnd}: new aliases={new_aliases}")
        if new_aliases == 0:
            break

    # ---- report + save alias table ----
    total_scraped = total_matched = 0
    print("\n=== per-file results ===")
    for league, f, records in load_scrape_files():
        league2, n, m, u = stats_by_file[f]
        total_scraped += n
        total_matched += m
        print(f"  {Path(f).name}: {n} scraped -> {m} matched ({u} unmatched)")
    print(f"\nTOTAL: {total_scraped} scraped -> {total_matched} matched ({100 * total_matched / max(total_scraped, 1):.1f}%)")
    by_league_alias = {}
    for (lg, k), v in alias.items():
        by_league_alias.setdefault(lg, {})[k] = v
    ALIAS_PATH.write_text(json.dumps(by_league_alias, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"alias table saved: {sum(len(m) for m in by_league_alias.values())} entries -> {ALIAS_PATH}")

    if args.dry_run:
        print("dry-run: no DB writes")
        conn.close()
        return 0

    # ---- insert newly matched ----
    # Seed done-keys from DB so batch-1 matches are not re-sent as IGNORE.
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT match_id FROM afb_odds_snapshots WHERE run_id='clv_backfill_batch1'")
    done_keys = {str(r[0]) for r in cur.fetchall()}
    print(f"already-inserted match_ids in DB: {len(done_keys)}")
    new_rows_total = 0
    for league, f, matched, unmatched in pending:
        for rec, match_id in matched:
            if match_id in done_keys:
                continue  # already inserted in batch 1
            market = rec.get("1x2_market") or []
            if not isinstance(market, list) or not market:
                continue
            scraped_date = str(rec.get("scraped_date") or "")
            attempted, written = clv.insert_snapshots(conn, match_id, scraped_date, market)
            if written:
                # tag rows with the remap run id is handled inside insert_snapshots
                new_rows_total += written
            done_keys.add(match_id)
    conn.commit()
    (OUT_DIR / "inserted_match_ids.json").write_text(json.dumps(sorted(done_keys)))
    print(f"\nnew snapshot rows inserted: {new_rows_total}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
