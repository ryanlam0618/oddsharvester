#!/usr/bin/env python3
"""PHASE4 read-only: lineup player-name evidence for all suspect team_ids."""
import sys

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
from afbdb.connection import afb_mysql_connect

# (team_id, current_label, season_ids, hypothesis)
SUSPECTS = [
    (1648, "Amiens SC", [52571, 61736], "Lens?"),
    (1684, "Olympique Lyonnais", [61736], "Angers?"),
    (1656, "RC Strasbourg", [52571], "Lorient?"),
    (1659, "RC Strasbourg", [52571], "real Strasbourg?"),
    (1651, "Metz", [52571], "Metz ok? P36 odd"),
    (2692, "Lille", [52760, 63515], "AC Milan?"),
    (2686, "Fiorentina", [52760], "Atalanta?"),
    (2693, "Fiorentina", [52760, 63515], "real Fiorentina?"),
    (2688, "Hellas Verona", [63515], "Venezia/Como?"),
    (2690, "Sampdoria", [63515], "Parma?"),
    (3140, "Kashiwa Reysol", [48055], "Cerezo?"),
    (3141, "Kashiwa Reysol", [48055, 57353], "real Kashiwa?"),
    (3146, "Sagan Tosu", [48055], "Vissel Kobe?"),
    (21904, "Sagan Tosu", [48055, 57353], "Machida/Sagan?"),
    (3147, "Vissel Kobe", [48055, 57353], "FC Tokyo?"),
    (5128, "Albirex Niigata", [48055], "real Albirex?"),
    (44409, "Albirex Niigata", [57353], "Machida?"),
    (5968, "Melbourne City", [54447, 64864], "Brisbane Roar?"),
    (42210, "Melbourne City", [54447, 64864], "real Melb City?"),
    (7568, "Wellington Phoenix", [54447, 64864], "Macarthur?"),
    (371682, "Wellington Phoenix", [54447, 64864], "real Welly?"),
    (7652, "Suwon Samsung Bluewings", [48379], "real Suwon?"),
    (189422, "Suwon Samsung Bluewings", [57878], "??"),
    (339827, "Pohang Steelers", [57878], "??"),
]


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    for tid, label, seasons, hyp in SUSPECTS:
        print(f"\n### {tid} {label!r} ({hyp})")
        for sid in seasons:
            cur.execute(
                """SELECT m.match_date, p.name FROM match_lineups l
                   JOIN matches m ON m.match_id = l.match_id
                   JOIN players p ON p.player_id = l.player_id
                   WHERE l.team_id = %s AND m.season_id = %s AND l.is_starter = 1
                   ORDER BY m.match_date DESC LIMIT 14""",
                (tid, sid),
            )
            rows = cur.fetchall()
            if not rows:
                print(f"  season {sid}: NO LINEUPS")
                continue
            print(f"  season {sid} (latest {rows[0][0]}): {[r[1] for r in rows]}")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
