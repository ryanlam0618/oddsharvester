#!/usr/bin/env python3
"""PHASE6 read-only: precise unmatched inventory + per-record diagnosis.

Replicates clv_remap matching (token-subset + alias), then classifies each
unmatched record:
  - NO_FX        : no DB fixture within +-2 days matching either side
  - ONE_SIDE     : a same-date fixture matches one side only (mislabeled other
                   side, or coincidental second fixture same day)
  - BOTH_NEAR    : fixtures near date match both sides separately but never together
Writes phase6_unmatched_diag.json.
"""
import json
import sys
import importlib.util
from datetime import date, timedelta

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
spec = importlib.util.spec_from_file_location(
    "clv_remap", "/root/.openclaw/workspace/oddsharvester/scripts/clv_remap.py"
)
crm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crm)

from afbdb.connection import afb_mysql_connect

OUT = "/root/.openclaw/workspace/oddsharvester/data/clv_backfill"


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    alias_all = json.loads(open(f"{OUT}/alias_table.json").read())

    # enumerate all scraped files + their (comp, season_id)
    cur.execute(
        """SELECT c.name, s.season_id, s.year_label FROM seasons s
           JOIN competitions c ON c.competition_id=s.competition_id"""
    )
    comp_seasons = cur.fetchall()
    by_cs = {(c, l): sid for c, sid, l in comp_seasons}

    import glob
    from pathlib import Path

    files = sorted(f for f in glob.glob(f"{OUT}/*.json")
                   if "state" not in f and "partial" not in f and "bak" not in f
                   and "alias" not in f and "diag" not in f and "facup_" not in f
                   and "phase5" not in f and "manifest" not in f and "report" not in f
                   and "missing" not in f and "newclubs" not in f and "evidence" not in f
                   and "cache" not in f and "sofascore" not in f and "smoke" not in f
                   and "coverage" not in f)
    slug_league = {}
    cur.execute("SELECT competition_id, name FROM competitions")
    comps = {n.strip(): cid for cid, n in cur.fetchall()}

    diag = []
    for f in files:
        stem = Path(f).stem
        slug = stem.rsplit("_", 1)[0]
        season_file = stem.rsplit("_", 1)[1]
        try:
            records = json.loads(Path(f).read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(records, list) or not records:
            continue
        rev = {v.strip(): k for k, v in crm.clv.LEAGUE_SLUGS.items()}
        league = rev.get(slug.strip())
        if not league:
            continue
        year_label = {'2024-2025': '24/25', '2023-2024': '23/24'}.get(season_file, season_file)
        sid = by_cs.get((league, year_label))
        if not sid:
            continue
        cur.execute(
            """SELECT m.match_date, th.name, ta.name FROM matches m
               JOIN teams th ON th.team_id=m.home_team_id JOIN teams ta ON ta.team_id=m.away_team_id
               WHERE m.competition_id=%s AND m.season_id=%s""",
            (comps[league], sid),
        )
        db_fx = [(str(d)[:10], hn, an) for d, hn, an in cur.fetchall()]
        la = alias_all.get(league, {})
        for r in records:
            d0 = str(r["match_date"])[:10]
            hit = False
            for k in (0, 1, -1, 2, -2):
                d = (date.fromisoformat(d0) + timedelta(days=k)).isoformat()
                for dd, hn, an in db_fx:
                    if dd == d and crm.match_pair(r["home_team"], r["away_team"], hn, an, la):
                        hit = True
                        break
                if hit:
                    break
            if hit:
                continue
            # diagnose
            one_side, both_near = [], False
            for k in (0, 1, -1, 2, -2):
                d = (date.fromisoformat(d0) + timedelta(days=k)).isoformat()
                for dd, hn, an in db_fx:
                    if dd != d:
                        continue
                    hm = crm.name_match(r["home_team"], hn) or la.get(crm.fold(crm.norm(r["home_team"]))) == hn
                    am = crm.name_match(r["away_team"], an) or la.get(crm.fold(crm.norm(r["away_team"]))) == an
                    if hm and am:
                        both_near = True
                    if hm or am:
                        one_side.append((d, hn, an, hm, am))
            cls = "BOTH_NEAR" if both_near else ("ONE_SIDE" if one_side else "NO_FX")
            diag.append({
                "file": Path(f).name, "league": league, "season_file": season_file,
                "date": d0, "home": r["home_team"], "away": r["away_team"],
                "class": cls,
                "near": [{"d": d, "db": f"{hn} vs {an}", "h_ok": hm, "a_ok": am} for d, hn, an, hm, am in one_side[:3]],
            })
    json.dump(diag, open(f"{OUT}/phase6_unmatched_diag.json", "w"), ensure_ascii=False, indent=1)
    from collections import Counter
    cnt = Counter((d["file"], d["class"]) for d in diag)
    print(f"total unmatched: {len(diag)}")
    for (f, c), n in sorted(cnt.items(), key=lambda x: -x[1]):
        print(f"  {f}: {c} x{n}")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
