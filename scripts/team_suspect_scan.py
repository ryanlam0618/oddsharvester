#!/usr/bin/env python3
"""PHASE4 read-only scan: per league-season team_id usage + W/D/L fingerprints.

Flags teams whose current `teams.name` looks anomalous for the league
(cross-league names, defunct clubs, duplicate labels). No DB writes.
"""
import sys
from collections import defaultdict

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
from afbdb.connection import afb_mysql_connect

LEAGUES = {
    "Ligue 1": [52571, 61736],       # 23/24, 24/25
    "Serie A": [52760, 63515],
    "J1 League": [48055, 57353],
    "A-League Men": [54447, 64864],
    "K League 1": [48379, 57878],
}


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    for league, seasons in LEAGUES.items():
        cur.execute("SELECT competition_id FROM competitions WHERE name=%s", (league,))
        comp_id = cur.fetchone()[0]
        print(f"\n===== {league} (comp {comp_id}) =====")
        for season_id in seasons:
            cur.execute(
                """SELECT m.home_team_id, m.away_team_id, m.home_score, m.away_score
                   FROM matches m
                   WHERE m.competition_id=%s AND m.season_id=%s AND m.status='finished'""",
                (comp_id, season_id),
            )
            rows = cur.fetchall()
            usage = defaultdict(lambda: [0, 0, 0, 0, 0])  # id -> [P, W, D, L, GF-GA]
            for h, a, hs, as_ in rows:
                hs = hs if hs is not None else -1
                as_ = as_ if as_ is not None else -1
                for tid, gf, ga in ((h, hs, as_), (a, as_, hs)):
                    u = usage[tid]
                    u[0] += 1
                    if gf < 0:
                        continue
                    u[4] += gf - ga
                    if gf > ga:
                        u[1] += 1
                    elif gf == ga:
                        u[2] += 1
                    else:
                        u[3] += 1
            print(f"-- season {season_id}: {len(rows)} FT matches, {len(usage)} team_ids")
            cur.execute("SELECT team_id, name FROM teams")
            names = dict(cur.fetchall())
            # mark teams used in FEW matches (sub-20 P in a 34-game league) as rare
            for tid in sorted(usage):
                p, w, d, l, gd = usage[tid]
                nm = names.get(tid, "??MISSING??")
                flag = " <<< RARE" if p < 20 else ""
                print(f"   {tid:>8} {nm:<32} P{p:>3} W{w:>3} D{d:>2} L{l:>3} GD{gd:>+5}{flag}")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
