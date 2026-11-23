"""
Same-day study, report: does Kalshi's price lag the thermometer?

For each city-day with daily-HIGH markets (from data/backtest.db) and each hour
from 1 PM to 11 PM local standard time, it reconstructs what was publicly known
10 minutes before that hour from the station's METARs (data/sameday.db):

  running max R = highest of each report's temperature (tenths T-group when
                  present, else the whole-degree reading) and every 6-hour max
                  group whose 6 hours lie inside the climate day;
  current temp  = the latest report;  drop = R - current temp.

Then it asks:
  A. The bracket containing R (the high so far): how often does it actually
     win, versus what the market charged for it at that hour?
  B. "Dead" brackets lying wholly at least 2F below R (the high has already
     passed them; the 2F margin covers reconstruction error): how often do they
     still win, versus the YES bid you could sell them at?
  C. How well R at midnight matches the official high (data-quality check).

Fixed BEFORE any results (2026-11-22): an edge counts only if, in the
development period, some hour x drop cell with at least 100 city-days shows the
running-max bracket winning at least 3 points more often than its average ask,
with positive simulated P&L after fees and z >= 2 - AND the same cell is
positive in the holdout (market dates from 2026-08-01). One contract per bet,
buying at the ask, tradeable quotes only (bid >= 1c, ask <= 98c, spread <= 20c).

Usage: python -m src.sameday_report [--db data/sameday.db] [--backtest-db data/backtest.db]
"""
import argparse
import bisect
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from src.backtest import climate_day_start_utc, load_cities
from src.backtest_report import actual_high, complete_days_only
from src.fees import kalshi_fee_cents
from src.sameday import c_to_f_whole

HOLDOUT_FROM = "2026-08-01"
HOURS = list(range(13, 24))           # 1 PM .. 11 PM local standard time
OBS_LAG_S = 600                       # a report is usable 10 minutes after its time
QUOTE_MAX_AGE_S = 2 * 3600
DROP_BUCKETS = [("0-1F", 0, 1), ("2-3F", 2, 3), ("4F+", 4, 999)]
MIN_CELL = 100


# --- reconstruction (unit-tested) -------------------------------------------------

def obs_value_f(o: dict) -> int | None:
    if o.get("t_group_c") is not None:
        return c_to_f_whole(o["t_group_c"])
    if o.get("tmpf") is not None:
        return int(round(o["tmpf"]))
    return None


def state_at(obs: list[dict], day_start: int, day_end: int, t: int,
             ts: list[int] | None = None) -> tuple[int | None, int | None]:
    """(running max R, current temp) using only reports from the climate day that
    were usable by time t. obs must be sorted by valid_ts; ts (optional) is the
    list of their valid_ts, used to jump straight to the day."""
    cutoff = t - OBS_LAG_S
    vals, current = [], None
    first = bisect.bisect_left(ts if ts is not None else [o["valid_ts"] for o in obs], day_start)
    for o in obs[first:]:
        v = o["valid_ts"]
        if v < day_start:
            continue
        if v > cutoff or v >= day_end:
            break
        f = obs_value_f(o)
        if f is not None:
            current = f
            # Only the tenths-of-a-degree T-group recovers the ASOS whole-degree F
            # exactly. A report without one (e.g. a 5-minute METAR, whole degrees C)
            # can read 1-2F high after conversion, so it never raises the running max.
            if o.get("t_group_c") is not None:
                vals.append(f)
        if o.get("max6_c") is not None and v - 6 * 3600 >= day_start:
            vals.append(c_to_f_whole(o["max6_c"]))
    return (max(vals) if vals else None), current


def bracket_contains(m: dict, x: int) -> bool:
    st = m.get("strike_type") or ""
    if m["kind"] == "bracket":
        return m["floor_strike"] <= x <= m["cap_strike"]
    if st.startswith("greater"):
        return x > m["floor_strike"]
    return x < m["cap_strike"]


def wholly_below(m: dict, x: int, margin: int = 2) -> bool:
    """True if every temperature this market pays on is at least `margin` below x."""
    st = m.get("strike_type") or ""
    if m["kind"] == "bracket":
        return m["cap_strike"] <= x - margin
    if st.startswith("less"):
        return m["cap_strike"] - 1 <= x - margin
    return False


def quote_at(series: list[tuple], t: int):
    """Latest (end_ts, bid, ask) with end_ts <= t and not older than 2h."""
    i = bisect.bisect_right(series, (t, 10 ** 9, 10 ** 9)) - 1
    if i < 0 or series[i][0] < t - QUOTE_MAX_AGE_S:
        return None
    return series[i]


def tradeable(bid, ask) -> bool:
    return bid is not None and ask is not None and bid >= 1 and 1 <= ask <= 98 and ask - bid <= 20


# --- report ------------------------------------------------------------------------

def _z(bets):
    sd = math.sqrt(sum(p * (100 - p) for p, _, _ in bets))
    return sum((100 if w else 0) - p for p, w, _ in bets) / sd if sd else 0.0


def build_report(db: str, backtest_db: str, config_path: str = "config.yaml") -> str:
    out = []
    say = out.append
    cities, _ = load_cities(config_path)
    info = {c["name"]: c for c, _ in cities}
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    b = sqlite3.connect(backtest_db)
    b.row_factory = sqlite3.Row
    markets, _ = complete_days_only([dict(r) for r in b.execute("SELECT * FROM bt_markets")])
    by_day = defaultdict(list)
    for m in markets:
        by_day[(m["city"], m["market_date"])].append(m)
    obs_by_station = defaultdict(list)
    for r in conn.execute("SELECT station, valid_ts, tmpf, t_group_c, max6_c FROM obs ORDER BY valid_ts"):
        obs_by_station[r["station"]].append(dict(r))
    ts_by_station = {s_: [o["valid_ts"] for o in v] for s_, v in obs_by_station.items()}
    quotes = defaultdict(list)
    for r in conn.execute("SELECT ticker, end_ts, yes_bid_cents, yes_ask_cents FROM quotes ORDER BY end_ts"):
        quotes[r["ticker"]].append((r["end_ts"], r["yes_bid_cents"], r["yes_ask_cents"]))

    say(f"=== Same-day study: does the market lag the thermometer? (daily HIGHS) — "
        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%MZ} ===")
    say("Pass rule (fixed 2026-11-22): a development cell (hour x drop, >= 100 city-days) where the high-so-far "
        "bracket wins >= 3 pts more often than its average ask, with positive P&L after fees and z >= 2, AND "
        "positive in the holdout (from 2026-08-01). Tradeable quotes only; one contract, bought at the ask.")
    say(f"Observations: {sum(len(v) for v in obs_by_station.values()):,} METARs for "
        f"{len(obs_by_station)} stations; quotes for {len(quotes):,} markets; "
        f"{len(by_day):,} complete city-days in backtest.db.")

    cells = {p: defaultdict(list) for p in ("dev", "hold")}
    dead = {p: defaultdict(list) for p in ("dev", "hold")}
    recon = defaultdict(list)
    covered = 0
    for (city, d), ms in by_day.items():
        c = info.get(city)
        if not c or not obs_by_station.get(c["expected_station"]):
            continue
        if not any(quotes.get(m["ticker"]) for m in ms):
            continue
        obs = obs_by_station[c["expected_station"]]
        ts = ts_by_station[c["expected_station"]]
        start = int(climate_day_start_utc(date.fromisoformat(d), c["timezone"]).timestamp())
        end = start + 86400
        period = "hold" if d >= HOLDOUT_FROM else "dev"
        covered += 1
        r_end, _ = state_at(obs, start, end, end + OBS_LAG_S, ts)
        actual, censored = actual_high(ms)
        if r_end is not None and actual is not None and not censored:
            recon[city].append(r_end - actual)
        for h in HOURS:
            t = start + h * 3600
            r, cur = state_at(obs, start, end, t, ts)
            if r is None or cur is None:
                continue
            drop = r - cur
            bucket = next(lbl for lbl, lo, hi in DROP_BUCKETS if lo <= drop <= hi) if drop >= 0 else None
            if bucket is None:
                continue
            for m in ms:
                q = quote_at(quotes.get(m["ticker"], []), t)
                if not q or not tradeable(q[1], q[2]):
                    continue
                won = m["outcome"] == "yes"
                if bracket_contains(m, r):
                    cells[period][(h, bucket)].append((q[2], won, q[1]))
                elif wholly_below(m, r):
                    dead[period][h].append((q[1], won))

    say(f"City-days with observations and intraday quotes: {covered:,}")
    say("\nC. DATA CHECK — reconstructed high at midnight minus the official high")
    say(f"  {'city':14s} {'days':>5s} {'exact':>6s} {'within 1F':>9s} {'mean diff':>9s}")
    for city in sorted(recon):
        v = recon[city]
        say(f"  {city:14s} {len(v):5d} {100 * sum(x == 0 for x in v) / len(v):5.0f}% "
            f"{100 * sum(abs(x) <= 1 for x in v) / len(v):8.0f}% {sum(v) / len(v):+9.2f}")
    say("  (Large or one-sided differences mean the reconstruction can't be trusted for that city.)")

    def cell_line(rows, label):
        n = len(rows)
        wins = sum(w for _, w, _ in rows)
        ask = sum(a for a, _, _ in rows) / n
        mid = sum((a + bd) / 2 for a, _, bd in rows) / n
        bets = [(a, w, bd) for a, w, bd in rows]
        pnl = sum((100 if w else 0) - a - kalshi_fee_cents(1, a) for a, w, _ in bets)
        return (f"  {label:16s} {n:6d} {100 * wins / n:7.1f}% {ask:7.1f}c {mid:7.1f}c "
                f"{100 * wins / n - ask:+7.1f} {pnl / n:+8.2f}c {_z(bets):+6.1f}"), n, 100 * wins / n - ask, pnl, _z(bets)

    passing = []
    for period, title in (("dev", "DEVELOPMENT"), ("hold", "HOLDOUT (read last)")):
        say(f"\nA. THE HIGH-SO-FAR BRACKET — {title}")
        say("  hour = local standard time; drop = running max minus current temperature.")
        say(f"  {'hour / drop':16s} {'days':>6s} {'won':>8s} {'avg ask':>8s} {'avg mid':>8s} "
            f"{'won-ask':>7s} {'P&L/bet':>9s} {'z':>6s}")
        for h in HOURS:
            for lbl, _, _ in DROP_BUCKETS:
                rows = cells[period].get((h, lbl))
                if not rows or len(rows) < 20:
                    continue
                line, n, edge, pnl, z = cell_line(rows, f"{(h - 1) % 12 + 1} {'PM' if h >= 12 else 'AM'} / {lbl}")
                say(line)
                if period == "dev" and n >= MIN_CELL and edge >= 3 and pnl > 0 and z >= 2:
                    passing.append((h, lbl))

        say(f"\nB. 'DEAD' BRACKETS (wholly >= 2F below the high so far) — {title}")
        say(f"  {'hour':6s} {'markets':>8s} {'still won':>9s} {'avg YES bid':>11s} {'NO P&L/bet':>10s}")
        for h in HOURS:
            rows = dead[period].get(h)
            if not rows or len(rows) < 20:
                continue
            pnl = sum((100 if not w else 0) - (100 - bd) - kalshi_fee_cents(1, 100 - bd) for bd, w in rows)
            say(f"  {(h - 1) % 12 + 1:2d} {'PM':3s} {len(rows):8d} {100 * sum(w for _, w in rows) / len(rows):8.2f}% "
                f"{sum(bd for bd, _ in rows) / len(rows):10.1f}c {pnl / len(rows):+9.2f}c")

    say("\nVERDICT (by the pass rule above)")
    if not passing:
        say("  No development cell passes. The market does not measurably lag the public observations "
            "at hourly resolution.")
    else:
        for h, lbl in passing:
            rows = cells["hold"].get((h, lbl), [])
            if rows:
                _, n, edge, pnl, z = cell_line(rows, "")
                ok = pnl > 0 and edge > 0
                say(f"  {(h - 1) % 12 + 1} PM / {lbl}: development PASSES; holdout n={n}, won-ask {edge:+.1f}, "
                    f"P&L {pnl / max(n, 1):+.2f}c/bet -> {'HOLDS' if ok else 'FAILS'}")
            else:
                say(f"  {(h - 1) % 12 + 1} PM / {lbl}: development passes; no holdout data yet")
    return "\n".join(out) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/sameday.db")
    ap.add_argument("--backtest-db", default="data/backtest.db")
    ap.add_argument("--out-dir", default="data/backtest_reports")
    args = ap.parse_args(argv)
    if not os.path.exists(args.db):
        print(f"{args.db} not found - run the same-day collector first.")
        return 0
    text = build_report(args.db, args.backtest_db)
    print(text)
    os.makedirs(args.out_dir, exist_ok=True)
    path = os.path.join(args.out_dir, f"sameday_report_{datetime.now(timezone.utc):%Y-%m-%dT%H%MZ}.txt")
    with open(path, "w") as f:
        f.write(text)
    print(f"(saved to {path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
