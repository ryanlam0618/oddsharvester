#!/usr/bin/env python3
"""PHASE4 read-only: scan ALL competitions' 23/24 + 24/25 (and calendar-year 2023/2024)
seasons for team usage. Flags duplicate labels + rare participants per season."""
import sys
from collections import defaultdict

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
from afbdb.connection import afb_mysql_connect


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    # all competitions that have matches in seasons labeled 23/24, 24/25, 2023, 2024
    cur.execute(
        """SELECT c.competition_id, c.name, s.season_id, s.year_label, COUNT(*) AS n
           FROM matches m JOIN seasons s ON s.season_id = m.season_id
           JOIN competitions c ON c.competition_id = s.competition_id
           WHERE s.year_label IN ('23/24', '24/25', '2023', '2024')
           GROUP BY c.competition_id, c.name, s.season_id, s.year_label
           ORDER BY c.name, s.year_label"""
    )
    rows = cur.fetchall()
    cur.execute("SELECT team_id, name FROM teams")
    names = dict(cur.fetchall())

    for comp_id, comp, season_id, label, n in rows:
        cur.execute(
            """SELECT home_team_id, away_team_id FROM matches
               WHERE competition_id=%s AND season_id=%s AND status='finished'""",
            (comp_id, season_id),
        )
        usage = defaultdict(int)
        for h, a in cur.fetchall():
            usage[h] += 1
            usage[a] += 1
        # duplicates: same label on 2+ ids
        by_label = defaultdict(list)
        for tid in usage:
            by_label[names.get(tid, "??")].append(tid)
        dups = {k: v for k, v in by_label.items() if len(v) > 1}
        rare = [tid for tid, c in usage.items() if c <= 4 and len(usage) > 6]
        line = f"{comp} [{label}] s{season_id}: {n} rows, {len(usage)} ids"
        if dups:
            line += f"  DUPS={ {k: v for k, v in dups.items()} }"
        if rare:
            line += f"  RARE={[(tid, names.get(tid, '?'), usage[tid]) for tid in sorted(rare)]}"
        print(line)
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
