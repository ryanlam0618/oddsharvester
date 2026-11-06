#!/usr/bin/env python3
"""PHASE4 Task 1 read-only: precise FA Cup gap analysis using clv_remap's matcher.

Outputs genuinely-missing fixtures (no DB match by name_match/alias within +-2 days)
for seasons 23/24 + 24/25, plus the club roster needed for teams-row inserts.
"""
import json
import sys
from datetime import date, timedelta

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, "/root/.openclaw/workspace/oddsharvester/scripts")

from afbdb.connection import afb_mysql_connect
import importlib.util

spec = importlib.util.spec_from_file_location("clv_remap", "/root/.openclaw/workspace/oddsharvester/scripts/clv_remap.py")
crm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crm)

OUT = "/root/.openclaw/workspace/oddsharvester/data/clv_backfill"


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    cur.execute("SELECT competition_id FROM competitions WHERE name='FA Cup'")
    comp = cur.fetchone()[0]
    cur.execute("SELECT season_id, year_label FROM seasons WHERE competition_id=%s AND year_label IN ('23/24','24/25')", (comp,))
    smap = {lbl: sid for sid, lbl in cur.fetchall()}

    # saved alias table (287 entries) for name resolution
    alias_all = json.loads(open(f"{OUT}/alias_table.json").read())

    for label, fname in [("23/24", "england-fa-cup_2023-2024.json"), ("24/25", "england-fa-cup_2024-2025.json")]:
        sid = smap[label]
        cur.execute(
            """SELECT m.match_id, m.match_date, th.name, ta.name, m.home_score, m.away_score
               FROM matches m JOIN teams th ON th.team_id=m.home_team_id JOIN teams ta ON ta.team_id=m.away_team_id
               WHERE m.competition_id=%s AND m.season_id=%s""",
            (comp, sid),
        )
        db_rows = cur.fetchall()
        # per-db-row name pair + date for matching
        db_fx = []
        for mid, d, hn, an, hs, as_ in db_rows:
            db_fx.append((mid, str(d)[:10], hn, an, hs, as_))
        la = alias_all.get("FA Cup", {})
        scraped = json.load(open(f"{OUT}/{fname}"))
        matched, missing = [], []
        for r in scraped:
            d0 = str(r["match_date"])[:10]
            hit = None
            for k in (0, 1, -1, 2, -2):
                d = (date.fromisoformat(d0) + timedelta(days=k)).isoformat()
                for mid, dd, hn, an, hs, as_ in db_fx:
                    if dd != d:
                        continue
                    if crm.match_pair(r["home_team"], r["away_team"], hn, an, la):
                        hit = mid
                        break
                if hit:
                    break
            (matched if hit else missing).append(r)
        print(f"{label}: scraped={len(scraped)} db={len(db_rows)} matched={len(matched)} GENUINE-MISSING={len(missing)}")
        json.dump(missing, open(f"{OUT}/facup_genuinemissing_{label.replace('/', '')}.json", "w"), ensure_ascii=False, indent=1)
        # clubs in missing fixtures not present in teams
        cur.execute("SELECT name FROM teams")
        import unicodedata

        def foldn(s):
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
            return " ".join(s.split())
        all_names = {foldn(n) for (n,) in cur.fetchall()}
        clubs = set()
        for r in missing:
            clubs.add(r["home_team"])
            clubs.add(r["away_team"])
        new_clubs = sorted({c for c in clubs if foldn(c) not in all_names})
        print(f"  clubs in missing: {len(clubs)}, not-in-teams(new rows needed): {len(new_clubs)}")
        json.dump(new_clubs, open(f"{OUT}/facup_newclubs_{label.replace('/', '')}.json", "w"), ensure_ascii=False, indent=1)
        # score availability
        with_score = sum(1 for r in missing if r.get("home_score") is not None)
        print(f"  missing with scores: {with_score}/{len(missing)}")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
