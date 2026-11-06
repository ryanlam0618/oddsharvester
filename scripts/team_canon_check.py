#!/usr/bin/env python3
"""PHASE4 read-only: fetch canonical Sofascore team names for all team_ids used
in the suspect leagues' seasons, diff vs current teams.name. No DB writes."""
import json
import sys
import time
import urllib.request

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
from afbdb.connection import afb_mysql_connect

LEAGUES = {
    "Ligue 1": [52571, 61736],
    "Serie A": [52760, 63515],
    "J1 League": [48055, 57353],
    "A-League Men": [54447, 64864],
    "K League 1": [48379, 57878],
}

CACHE = "/root/.openclaw/workspace/oddsharvester/data/sofascore_team_cache.json"


def fetch_team(tid: int):
    url = f"https://api.sofascore.com/api/v1/team/{tid}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
            t = data.get("team", {})
            return {"name": t.get("name"), "short": t.get("shortName"), "country": (t.get("country") or {}).get("name")}
    except Exception as e:
        return {"error": str(e)[:80]}


def main() -> int:
    try:
        cache = json.load(open(CACHE))
    except Exception:
        cache = {}
    conn = afb_mysql_connect()
    cur = conn.cursor()
    ids = set()
    for league, seasons in LEAGUES.items():
        cur.execute("SELECT competition_id FROM competitions WHERE name=%s", (league,))
        comp_id = cur.fetchone()[0]
        cur.execute(
            """SELECT DISTINCT home_team_id FROM matches WHERE competition_id=%s AND season_id IN %s
               UNION SELECT DISTINCT away_team_id FROM matches WHERE competition_id=%s AND season_id IN %s""",
            (comp_id, tuple(seasons), comp_id, tuple(seasons)),
        )
        ids.update(r[0] for r in cur.fetchall())
    cur.execute("SELECT team_id, name FROM teams")
    db_names = dict(cur.fetchall())
    cur.close()
    conn.close()

    todo = sorted(t for t in ids if t not in cache)
    print(f"{len(ids)} distinct team_ids, {len(todo)} to fetch")
    for i, tid in enumerate(todo):
        cache[str(tid)] = fetch_team(tid)
        if (i + 1) % 10 == 0:
            print(f"  fetched {i + 1}/{len(todo)}")
        time.sleep(1.2)
    json.dump(cache, open(CACHE, "w"), ensure_ascii=False, indent=1)

    print("\n===== DIFF (sofascore canonical vs current teams.name) =====")
    diffs = 0
    for tid in sorted(ids):
        canon = cache[str(tid)].get("name")
        cur_label = db_names.get(tid, "??MISSING??")
        mark = "" if canon == cur_label else "  <<< MISMATCH"
        if canon != cur_label:
            diffs += 1
        print(f"{tid:>8}  DB={cur_label!r:<38} Sofascore={canon!r}{mark}")
    print(f"\n{diffs} mismatches out of {len(ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
