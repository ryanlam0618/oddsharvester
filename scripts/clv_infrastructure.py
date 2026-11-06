#!/usr/bin/env python3
"""T9-CLV: Closing Line Value infrastructure on afb_odds_snapshots.

Data model (verified 2026-11-07):
- afb_odds_snapshots holds ONE archived snapshot per (match, bookmaker) with
  selections '1'/'X'/'2' - OddsPortal historical pages expose the final
  pre-kickoff (closing) line, so the single snapshot IS the closing line.
- 'avg' bookmaker rows use Home/Draw/Away labels and are an aggregate, not a
  bookmaker -> excluded.
- 32,215 complete (match, bookie) trios; ~5,431 matches with >=2 complete-trio
  bookmakers.

Outputs (computation tables, no schema change to existing tables):
- clv_closing_line      : one row per (match, bookmaker) - raw trio odds,
                          margin, proportional + Shin de-vigged probabilities
- clv_consensus_closing : one row per match - median de-vigged probabilities
                          across bookmakers + dispersion (consistency check)

Reusable functions for the model layer:
- devig_proportional(o1, ox, o2)
- devig_shin(o1, ox, o2)
- clv_ratio(bet_odds, closing_odds)
- ev_vs_fair(bet_odds, fair_prob)
"""
import sys
import statistics
from decimal import Decimal

sys.path.insert(0, "/root/.openclaw/workspace-coding/aifootballbets")
from afbdb.connection import afb_mysql_connect


# ---------------------------------------------------------------- de-vig ----
def implied_probs(o1, ox, o2):
    return (1.0 / o1, 1.0 / ox, 1.0 / o2)


def devig_proportional(o1, ox, o2):
    """Multiplicative (equal-margin) de-vig: p_i = q_i / sum(q)."""
    q1, qx, q2 = implied_probs(o1, ox, o2)
    s = q1 + qx + q2
    return (q1 / s, qx / s, q2 / s)


def _shin_probs(z, q1, qx, q2, Q):
    out = []
    for q in (q1, qx, q2):
        out.append(((z * z + 4.0 * (1.0 - z) * q * q / Q) ** 0.5 - z) / (2.0 * (1.0 - z)))
    return out


def devig_shin(o1, ox, o2):
    """Shin (1992) de-vig: solve sum(p_i(z)) = 1 for the insider proportion z.

    p_i(z) = (sqrt(z^2 + 4(1-z) q_i^2 / Q) - z) / (2(1-z)),  Q = sum(q_i).
    Bisection on z in (0, 1); at z->0 sum -> sqrt(Q) > 1, z increasing lowers it.
    """
    q1, qx, q2 = implied_probs(o1, ox, o2)
    Q = q1 + qx + q2

    def total(z):
        return sum(_shin_probs(z, q1, qx, q2, Q))

    lo, hi = 1e-12, 1.0 - 1e-9
    # total(lo) = sqrt(Q) - 1 > 0 for Q > 1; total(hi) < 0 for sane odds
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if total(mid) > 1.0:
            lo = mid
        else:
            hi = mid
    z = (lo + hi) / 2.0
    p1, px, p2 = _shin_probs(z, q1, qx, q2, Q)
    s = p1 + px + p2
    return (p1 / s, px / s, p2 / s)  # renormalise numeric residue


# ------------------------------------------------------------------- CLV ----
def clv_ratio(bet_odds, closing_odds):
    """Raw closing-line value: bet price vs closing price."""
    return bet_odds / closing_odds - 1.0


def ev_vs_fair(bet_odds, fair_prob):
    """Expected value per unit staked using de-vigged fair closing probability."""
    return bet_odds * fair_prob - 1.0


# --------------------------------------------------------- infrastructure ----
def create_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clv_closing_line (
            match_id BIGINT NOT NULL,
            bookmaker VARCHAR(32) NOT NULL,
            odds_home DECIMAL(8,3) NOT NULL,
            odds_draw DECIMAL(8,3) NOT NULL,
            odds_away DECIMAL(8,3) NOT NULL,
            margin DECIMAL(8,5) NOT NULL,
            p_home_prop DECIMAL(8,6) NOT NULL,
            p_draw_prop DECIMAL(8,6) NOT NULL,
            p_away_prop DECIMAL(8,6) NOT NULL,
            p_home_shin DECIMAL(8,6) NOT NULL,
            p_draw_shin DECIMAL(8,6) NOT NULL,
            p_away_shin DECIMAL(8,6) NOT NULL,
            captured_at_utc DATETIME NULL,
            match_date DATE NULL,
            PRIMARY KEY (match_id, bookmaker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clv_consensus_closing (
            match_id BIGINT NOT NULL PRIMARY KEY,
            n_bookies INT NOT NULL,
            match_date DATE NULL,
            p_home_prop_med DECIMAL(8,6) NOT NULL,
            p_draw_prop_med DECIMAL(8,6) NOT NULL,
            p_away_prop_med DECIMAL(8,6) NOT NULL,
            p_home_shin_med DECIMAL(8,6) NOT NULL,
            p_draw_shin_med DECIMAL(8,6) NOT NULL,
            p_away_shin_med DECIMAL(8,6) NOT NULL,
            disp_home DECIMAL(8,6) NOT NULL,
            disp_draw DECIMAL(8,6) NOT NULL,
            disp_away DECIMAL(8,6) NOT NULL,
            method_note VARCHAR(64) NOT NULL DEFAULT 'shin_median'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")


def main() -> int:
    conn = afb_mysql_connect()
    cur = conn.cursor()
    create_tables(cur)
    cur.execute("DELETE FROM clv_closing_line")
    cur.execute("DELETE FROM clv_consensus_closing")

    # kickoff dates from matches (context; snapshots are archived closings)
    cur.execute("SELECT match_id, match_date FROM matches")
    kdates = dict(cur.fetchall())

    cur.execute("""
        SELECT s.match_id, s.bookmaker,
               MAX(CASE WHEN s.selection='1' THEN s.odds END) o1,
               MAX(CASE WHEN s.selection='X' THEN s.odds END) ox,
               MAX(CASE WHEN s.selection='2' THEN s.odds END) o2,
               MIN(s.captured_at_utc)
        FROM afb_odds_snapshots s
        WHERE s.selection IN ('1','X','2') AND s.bookmaker <> 'avg'
        GROUP BY s.match_id, s.bookmaker
        HAVING o1 IS NOT NULL AND ox IS NOT NULL AND o2 IS NOT NULL""")
    trios = cur.fetchall()
    print(f"complete trios: {len(trios)}")

    per_match = {}
    ins = 0
    for mid, bookie, o1, ox, o2, cap in trios:
        o1f, oxf, o2f = float(o1), float(ox), float(o2)
        if min(o1f, oxf, o2f) <= 1.001:
            continue
        pp = devig_proportional(o1f, oxf, o2f)
        ps = devig_shin(o1f, oxf, o2f)
        margin = 1.0 / o1f + 1.0 / oxf + 1.0 / o2f
        per_match.setdefault(mid, []).append((pp, ps))
        cur.execute("""INSERT IGNORE INTO clv_closing_line
            (match_id, bookmaker, odds_home, odds_draw, odds_away, margin,
             p_home_prop, p_draw_prop, p_away_prop,
             p_home_shin, p_draw_shin, p_away_shin, captured_at_utc, match_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (mid, bookie, o1, ox, o2, round(margin, 5),
             round(pp[0], 6), round(pp[1], 6), round(pp[2], 6),
             round(ps[0], 6), round(ps[1], 6), round(ps[2], 6), cap, kdates.get(mid)))
        ins += 1
    print(f"clv_closing_line rows inserted: {ins}")

    # consensus: median across bookies (shin primary, prop retained for comparison)
    cons = 0
    disp_all = {"home": [], "draw": [], "away": []}
    for mid, trios_m in per_match.items():
        if len(trios_m) < 1:
            continue
        phs = [t[1][0] for t in trios_m]
        pxs = [t[1][1] for t in trios_m]
        pas = [t[1][2] for t in trios_m]
        pps_h = [t[0][0] for t in trios_m]
        pps_x = [t[0][1] for t in trios_m]
        pps_a = [t[0][2] for t in trios_m]
        mh, mx, ma = (statistics.median(x) for x in (phs, pxs, pas))
        mph, mpx, mpa = (statistics.median(x) for x in (pps_h, pps_x, pps_a))
        dh, dx, da = max(phs) - min(phs), max(pxs) - min(pxs), max(pas) - min(pas)
        disp_all["home"].append(dh)
        disp_all["draw"].append(dx)
        disp_all["away"].append(da)
        cur.execute("""INSERT IGNORE INTO clv_consensus_closing
            (match_id, n_bookies, match_date,
             p_home_prop_med, p_draw_prop_med, p_away_prop_med,
             p_home_shin_med, p_draw_shin_med, p_away_shin_med,
             disp_home, disp_draw, disp_away) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (mid, len(trios_m), kdates.get(mid),
             round(mph, 6), round(mpx, 6), round(mpa, 6),
             round(mh, 6), round(mx, 6), round(ma, 6),
             round(dh, 6), round(dx, 6), round(da, 6)))
        cons += 1
    print(f"clv_consensus_closing rows inserted: {cons}")

    # consistency summary
    for k, v in disp_all.items():
        v.sort()
        n = len(v)
        print(f"dispersion[{k}]: median={statistics.median(v):.4f} p90={v[int(n*0.9)]:.4f} max={v[-1]:.4f}")
    conn.commit()
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
