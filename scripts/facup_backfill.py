#!/usr/bin/env python3
"""PHASE4 Task 1: backfill missing FA Cup fixtures (23/24 + 24/25) into matches.

- Resolves each fixture club against existing teams (token-subset + alias matcher);
  creates synthetic teams rows (id >= 900000001) only for genuinely-new clubs.
- Inserts missing matches with synthetic match_id (>= 900000001).
- Idempotent: re-checks (date +-2, resolved pair) before each insert.
- Writes a full id-mapping manifest for traceability.
"""
import json
import sys
import unicodedata
from datetime import date, datetime, timedelta

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
sys.path.insert(0, "/root/.openclaw/workspace/oddsharvester/scripts")

from afbdb.connection import afb_mysql_connect

OUT = "/root/.openclaw/workspace/oddsharvester/data/clv_backfill"
TEAM_ID_START = 900000001
MATCH_ID_START = 900000001


def foldn(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return " ".join(s.split())


def main() -> int:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "clv_remap", "/root/.openclaw/workspace/oddsharvester/scripts/clv_remap.py"
    )
    crm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(crm)

    conn = afb_mysql_connect()
    cur = conn.cursor()
    cur.execute("SELECT competition_id FROM competitions WHERE name='FA Cup'")
    comp = cur.fetchone()[0]
    cur.execute("SELECT season_id, year_label FROM seasons WHERE competition_id=%s AND year_label IN ('23/24','24/25')", (comp,))
    smap = {lbl: sid for sid, lbl in cur.fetchall()}
    alias_all = json.loads(open(f"{OUT}/alias_table.json").read())
    la = alias_all.get("FA Cup", {})

    # existing teams: id -> name; plus folded index + full list for token matching
    cur.execute("SELECT team_id, name FROM teams")
    teams_by_id = dict(cur.fetchall())
    all_names = list(teams_by_id.values())
    by_fold = {}
    for tid, n in teams_by_id.items():
        by_fold.setdefault(foldn(n), tid)

    def resolve_team(scraped_name: str):
        """Return existing team_id or None. Uses fold equality then token-subset + alias."""
        f = foldn(scraped_name)
        if f in by_fold:
            return by_fold[f]
        for n in all_names:
            if crm.name_match(scraped_name, n):
                return teams_by_id_inverse[n] if False else by_fold[foldn(n)]
        # alias table may map scraped short name to a DB club name
        db_name = la.get(f)
        if db_name and foldn(db_name) in by_fold:
            return by_fold[foldn(db_name)]
        return None

    teams_by_id_inverse = {v: k for k, v in teams_by_id.items()}

    manifest = {"teams_created": {}, "matches_created": {}, "skipped_existing": []}
    next_team_id = TEAM_ID_START
    next_match_id = MATCH_ID_START

    # DB fixtures already present (per season) for idempotency re-check
    db_fx = {}
    for label in ("23/24", "24/25"):
        sid = smap[label]
        cur.execute(
            """SELECT m.match_date, th.name, ta.name FROM matches m
               JOIN teams th ON th.team_id=m.home_team_id JOIN teams ta ON ta.team_id=m.away_team_id
               WHERE m.competition_id=%s AND m.season_id=%s""",
            (comp, sid),
        )
        db_fx[label] = [(str(d)[:10], n, a) for d, n, a in cur.fetchall()]

    total_matches = 0
    for label, fname in [("23/24", "facup_genuinemissing_2324.json"), ("24/25", "facup_genuinemissing_2425.json")]:
        sid = smap[label]
        missing = json.load(open(f"{OUT}/{fname}"))
        print(f"== {label}: {len(missing)} fixtures to process")
        for r in missing:
            d0 = str(r["match_date"])[:10]
            t0 = str(r["match_date"])[11:19]
            h_raw, a_raw = r["home_team"], r["away_team"]
            hid = resolve_team(h_raw)
            if hid is None:
                hid = next_team_id
                next_team_id += 1
                cur.execute(
                    "INSERT INTO teams (team_id, name, short_name, slug, country_code, team_type) VALUES (%s,%s,%s,%s,'ENG','club')",
                    (hid, h_raw, h_raw, foldn(h_raw).replace(" ", "-")),
                )
                teams_by_id[hid] = h_raw
                all_names.append(h_raw)
                by_fold[foldn(h_raw)] = hid
                manifest["teams_created"][str(hid)] = h_raw
            aid = resolve_team(a_raw)
            if aid is None:
                aid = next_team_id
                next_team_id += 1
                cur.execute(
                    "INSERT INTO teams (team_id, name, short_name, slug, country_code, team_type) VALUES (%s,%s,%s,%s,'ENG','club')",
                    (aid, a_raw, a_raw, foldn(a_raw).replace(" ", "-")),
                )
                teams_by_id[aid] = a_raw
                all_names.append(a_raw)
                by_fold[foldn(a_raw)] = aid
                manifest["teams_created"][str(aid)] = a_raw

            # idempotency: existing match with same pair within +-2 days?
            dup = False
            for k in (0, 1, -1, 2, -2):
                d = (date.fromisoformat(d0) + timedelta(days=k)).isoformat()
                for dd, hn, an in db_fx[label]:
                    if dd == d and crm.match_pair(h_raw, a_raw, hn, an, la):
                        dup = True
                        break
                if dup:
                    break
            if dup:
                manifest["skipped_existing"].append(f"{label}:{h_raw} vs {a_raw} {d0}")
                continue

            hs = r.get("home_score")
            as_ = r.get("away_score")
            hs = int(hs) if hs not in (None, "") else None
            as_ = int(as_) if as_ not in (None, "") else None
            mt = datetime.strptime(t0, "%H:%M:%S").time() if t0 else None
            mid = next_match_id
            next_match_id += 1
            cur.execute(
                """INSERT INTO matches (match_id, season_id, competition_id, match_date, match_time,
                                        home_team_id, away_team_id, home_score, away_score, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'finished')""",
                (mid, sid, comp, d0, mt, hid, aid, hs, as_),
            )
            db_fx[label].append((d0, h_raw, a_raw))
            manifest["matches_created"][str(mid)] = {
                "season": label, "date": d0, "home": h_raw, "away": a_raw,
                "home_team_id": hid, "away_team_id": aid, "home_score": hs, "away_score": as_,
            }
            total_matches += 1
        conn.commit()
        print(f"   committed. cumulative matches inserted: {total_matches}")

    json.dump(manifest, open(f"{OUT}/facup_backfill_manifest.json", "w"), ensure_ascii=False, indent=1)
    print(f"DONE: matches inserted={total_matches}, teams created={len(manifest['teams_created'])}, "
          f"skipped={len(manifest['skipped_existing'])}")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
