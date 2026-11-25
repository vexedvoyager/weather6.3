"""
Minute study report (added 2026-11-24; see "claude/project-status.md" in the
Claude project). Reads the chunks written by src/minute_recorder.py.

Modes
  --mode health  (any time) data health only: minutes covered per day, gaps,
                 markets seen, which size fields Kalshi provides, and how often
                 books are empty or wide by hour of day. No profit numbers, so
                 looking early can't bias anything.
  --mode warmup  (any time) runs checks A-D on the WARM-UP days only (before
                 Dec 1), which are never scored, to catch bugs on real data early.
                 Rule (fixed 2026-11-24): warm-up results may be used to fix bugs
                 only, never to change the test.
  --mode final   (after collection ends, 2026-12-15) the full test below.

The test (fixed 2026-11-24, before any data was collected)
  Three kinds of short-lived bad price, each acted on ONE MINUTE AFTER it was
  first seen (the next poll), at that minute's prices, and only if it was still
  there. Taker fee 0.07 x P x (1 - P) per contract. Size = contracts actually
  resting at that price (order-book snapshot or top-of-book size), capped at 100;
  1 contract when unknown.
    A. Ladder gap: a city-day's YES asks add to less than 100c (buy every YES), or
       its YES bids add to more than 100c (buy every NO), after fees.
    B. Stray price: an ask 5c+ below (or bid 5c+ above) the market's own median
       midpoint over the previous 30 minutes. Buy it; value at settlement.
    C. Slow reaction: a new hourly report raises the day's high so far (13
       configured cities' daily-high markets). Brackets now >= 2F below it are
       certain NO, and "above X" markets now >= 2F inside are certain YES. If the
       certain side can still be bought one minute after the report is public
       (report time + 5 min) for at least 1c net profit per contract after the fee
       (i.e. the losing side is still bid 2c+; leftover 1c bids don't count),
       buy it.
    D. Slow reaction to a new FORECAST (added 2026-11-24, before any data): a new NOAA
       NBP bulletin moves a configured city's median forecast (p50) high or low for an
       open market date by >= 2F versus the previous bulletin. One minute after the
       recorder first saw the bulletin, buy YES on the bracket containing the new p50
       ("toward") and buy NO on the bracket containing the old p50 ("away"), at the
       quoted prices; close both 30 minutes later at the quoted prices. Profit = exit
       minus entry minus the taker fee on both trades. Bulletins already present when
       a recorder run started (release minute unknown) are skipped.
  Periods by action time: development 2026-12-01..07, holdout 2026-12-08..14 (UTC).
  A type PASSES if in development it has >= 20 episodes, total profit >= $10,
  AND its holdout total is positive. Data before Dec 1 is warm-up (health only).
"""
import argparse
import bisect
import glob
import gzip
import json
import math
import os
import shutil
import sqlite3
import statistics
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.minute_recorder import STOP_AT, STRAY_CENTS, STRAY_LOOKBACK, STRAY_MIN_HISTORY

DEV_FROM = datetime(2026, 12, 1, tzinfo=timezone.utc)
HOLD_FROM = datetime(2026, 12, 8, tzinfo=timezone.utc)
HOLD_TO = datetime(2026, 12, 15, tzinfo=timezone.utc)
SIZE_CAP = 100
PASS_MIN_EPISODES = 20
PASS_MIN_DOLLARS = 10.0
REPORT_PUBLIC_S = 5 * 60          # a METAR is public about 5 minutes after its time
DEAD_MARGIN = 2
MIN_NET_C = 1.0                   # check C: at least 1c per contract after the fee
FORECAST_SHIFT_F = 2
HOLD_MINUTES = 30


# --- helpers (unit-tested) ------------------------------------------------------

def fee_c(price_c: float) -> float:
    p = price_c / 100.0
    return 7.0 * p * (1 - p)        # cents per contract (0.07 x P x (1-P) dollars)


_MODE = "final"


def period_of(ts: int) -> str | None:
    if _MODE == "warmup":
        return "warmup" if ts < DEV_FROM.timestamp() else None
    if DEV_FROM.timestamp() <= ts < HOLD_FROM.timestamp():
        return "dev"
    if HOLD_FROM.timestamp() <= ts < HOLD_TO.timestamp():
        return "hold"
    return None


def level_size_at(levels: list, price_c: int) -> float | None:
    """Contracts resting at exactly price_c (cents) in a [[price_dollars, count], ...] list."""
    for lvl in levels or []:
        try:
            if round(float(lvl[0]) * 100) == price_c:
                return float(lvl[1])
        except (TypeError, ValueError, IndexError):
            continue
    return None


def certain_side(meta: dict, high_so_far: int, margin: int = DEAD_MARGIN) -> str | None:
    """'no' if this daily-high market can no longer win, 'yes' if it can no longer lose,
    given that the official high is at least high_so_far."""
    st = (meta.get("strike_type") or "").lower()
    fl, cap = meta.get("floor_strike"), meta.get("cap_strike")
    r = high_so_far - margin
    if st == "between" and cap is not None and cap <= r:
        return "no"
    if st == "less" and cap is not None and cap - 1 <= r:
        return "no"
    if st == "less_or_equal" and cap is not None and cap <= r:
        return "no"
    if st == "greater" and fl is not None and r > fl:
        return "yes"
    if st == "greater_or_equal" and fl is not None and r >= fl:
        return "yes"
    return None


def episodes(minutes: list[int], flags: list[bool]) -> list[tuple[int, int]]:
    """(start_index, length) runs of consecutive polled minutes where flags is True.
    A missing minute (gap > 60 s) ends a run."""
    out, start = [], None
    for i, f in enumerate(flags):
        contiguous = i > 0 and minutes[i] - minutes[i - 1] == 60
        if f and start is not None and contiguous:
            continue
        if start is not None:
            out.append((start, i - start))
            start = None
        if f:
            start = i
    if start is not None:
        out.append((start, len(flags) - start))
    return out


def verdict(dev: list, hold: list) -> tuple[bool, str]:
    n, tot = len(dev), sum(p for p in dev)
    htot = sum(p for p in hold)
    ok = n >= PASS_MIN_EPISODES and tot >= PASS_MIN_DOLLARS and htot > 0
    return ok, (f"dev {n} episodes, ${tot:,.2f}; holdout {len(hold)} episodes, ${htot:,.2f} -> "
                f"{'PASS' if ok else 'no pass'}")


# --- loading ------------------------------------------------------------------

def load(chunk_dir: str) -> sqlite3.Connection:
    from src.minute_recorder import SCHEMA
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    db.execute("DROP TABLE meta")
    db.execute("CREATE TABLE meta (ticker TEXT, event TEXT, series TEXT, strike_type TEXT, floor_strike REAL, "
               "cap_strike REAL, close_time TEXT, station TEXT)")
    tmpdir = tempfile.mkdtemp()
    files = sorted(glob.glob(os.path.join(chunk_dir, "chunk_*.db.gz")))
    for i, f in enumerate(files):
        raw = os.path.join(tmpdir, f"c{i}.db")
        with gzip.open(f, "rb") as g, open(raw, "wb") as o:
            shutil.copyfileobj(g, o)
        db.execute(f"ATTACH DATABASE '{raw}' AS c")
        tables = {r[0] for r in db.execute("SELECT name FROM c.sqlite_master WHERE type='table'")}
        for t in ("quotes", "polls", "depth", "meta", "bulletins", "forecasts"):
            if t not in tables:
                continue
            db.execute(f"INSERT INTO main.{t} SELECT * FROM c.{t}")
        db.execute("INSERT OR REPLACE INTO main.fields SELECT * FROM c.fields")
        db.commit()
        db.execute("DETACH DATABASE c")
    shutil.rmtree(tmpdir, ignore_errors=True)
    db.execute("CREATE INDEX q_t ON quotes(ticker, ts)")
    db.execute("CREATE INDEX p_s ON polls(series, ts)")
    return db, len(files)


class Data:
    """Per-ticker minute series reconstructed from change-only rows."""

    def __init__(self, db):
        self.meta = {}
        for r in db.execute("SELECT ticker, event, series, strike_type, floor_strike, cap_strike, close_time, station "
                            "FROM meta"):
            self.meta.setdefault(r[0], dict(zip(("ticker", "event", "series", "strike_type", "floor_strike",
                                                 "cap_strike", "close_time", "station"), r)))
        self.polled = defaultdict(set)
        for s, ts in db.execute("SELECT series, ts FROM polls"):
            self.polled[s].add(ts)
        self.polled_sorted = {s: sorted(v) for s, v in self.polled.items()}
        self.rows = defaultdict(list)
        for t, ts, b, a, bs, as_ in db.execute(
                "SELECT ticker, ts, bid, ask, bid_size, ask_size FROM quotes ORDER BY ticker, ts"):
            self.rows[t].append((ts, b, a, bs, as_))
        self.depth = defaultdict(list)
        for t, ts, y, n in db.execute("SELECT ticker, ts, yes_levels, no_levels FROM depth ORDER BY ts"):
            self.depth[t].append((ts, json.loads(y or "[]"), json.loads(n or "[]")))
        self.events = defaultdict(list)
        for t, m in self.meta.items():
            self.events[m["event"]].append(t)
        self.bulletins = {}                          # run_id -> (first_seen_ts, at_start)
        for run_id, ts, at_start in db.execute("SELECT run_id, first_seen_ts, at_start FROM bulletins ORDER BY first_seen_ts"):
            if run_id not in self.bulletins:
                self.bulletins[run_id] = (ts, at_start)
        self.forecasts = defaultdict(dict)           # (station, kind, date) -> {run_id: p50}
        for run_id, st, kind, d, p50 in db.execute("SELECT run_id, station, kind, market_date, p50 FROM forecasts"):
            self.forecasts[(st, kind, d)].setdefault(run_id, p50)

    def series_minutes(self, ticker) -> list[int]:
        m = self.meta.get(ticker)
        rows = self.rows.get(ticker)
        if not m or not rows:
            return []
        first, last = rows[0][0], rows[-1][0]
        close = None
        try:
            close = int(datetime.fromisoformat(str(m["close_time"]).replace("Z", "+00:00")).timestamp())
        except (TypeError, ValueError):
            pass
        hi = close if close else last + 3600
        mins = self.polled_sorted.get(m["series"], [])
        return mins[bisect.bisect_left(mins, first):bisect.bisect_left(mins, hi)]

    def path(self, ticker) -> dict:
        """{minute_ts: (bid, ask, bid_size, ask_size)} for every polled minute the market was open."""
        rows = self.rows.get(ticker, [])
        out, j, cur = {}, 0, None
        for t in self.series_minutes(ticker):
            while j < len(rows) and rows[j][0] <= t:
                cur = rows[j]
                j += 1
            if cur is not None:
                out[t] = cur[1:]
        return out

    def depth_size(self, ticker, ts, side: str, price_c: int) -> float | None:
        """Contracts available to BUY `side` at price_c, from a snapshot within 2 minutes of ts.
        Buying YES at price p hits NO bids at 100 - p; buying NO at q hits YES bids at 100 - q."""
        for t, y, n in self.depth.get(ticker, []):
            if abs(t - ts) <= 120:
                return level_size_at(n if side == "yes" else y, 100 - price_c)
        return None


def capped(size) -> float:
    if size is None or size <= 0:
        return 1.0
    return float(min(size, SIZE_CAP))


# --- analyses -------------------------------------------------------------------

def ladder_opps(d: Data) -> list[dict]:
    out = []
    for ev, tickers in d.events.items():
        if len(tickers) < 5:
            continue
        paths = {t: d.path(t) for t in tickers}
        common = sorted(set.intersection(*(set(p) for p in paths.values()))) if paths else []
        for side in ("yes", "no"):
            vals = []
            for t in common:
                qs = [paths[k][t] for k in tickers]
                if any(q[0] is None or q[1] is None for q in qs):
                    vals.append(None)
                    continue
                if side == "yes":
                    vals.append(100 - sum(q[1] for q in qs) - sum(fee_c(q[1]) for q in qs))
                else:
                    vals.append(sum(q[0] for q in qs) - 100 - sum(fee_c(q[0]) for q in qs))
            for s, n in episodes(common, [v is not None and v > 0 for v in vals]):
                act = s + 1                               # one minute after first seen
                if n < 2:
                    out.append({"event": ev, "side": side, "ts": common[s], "minutes": n, "caught": False,
                                "profit": 0.0})
                    continue
                t = common[act]
                sizes = []
                for k in tickers:
                    q = paths[k][t]
                    price = q[1] if side == "yes" else 100 - q[0]
                    sz = d.depth_size(k, t, side, price)
                    if sz is None:
                        sz = q[3] if side == "yes" else q[2]
                    sizes.append(sz)
                size = capped(min(sizes) if all(s_ is not None for s_ in sizes) else None)
                out.append({"event": ev, "side": side, "ts": t, "minutes": n, "caught": True,
                            "profit": vals[act] / 100 * size, "per_set": vals[act], "size": size})
    return out


def stray_opps(d: Data) -> list[dict]:
    out = []
    for t, m in d.meta.items():
        path = d.path(t)
        mins = sorted(path)
        mids, flags, kinds = [], [], []
        for ts in mins:
            b, a = path[ts][0], path[ts][1]
            hist = [x for x in mids[-STRAY_LOOKBACK:] if x is not None]
            kind = None
            if len(hist) >= STRAY_MIN_HISTORY:
                med = statistics.median(hist)
                if a is not None and a <= med - STRAY_CENTS:
                    kind = "ask"
                elif b is not None and b >= med + STRAY_CENTS:
                    kind = "bid"
            kinds.append(kind)
            flags.append(kind is not None)
            mids.append(None if b is None or a is None else (b + a) / 2)
        for s, n in episodes(mins, flags):
            if n < 2 or kinds[s + 1] != kinds[s]:
                out.append({"ticker": t, "ts": mins[s], "minutes": n, "caught": False})
                continue
            ts = mins[s + 1]
            b, a, bs, as_ = path[ts]
            side, price = ("yes", a) if kinds[s] == "ask" else ("no", 100 - b)
            sz = d.depth_size(t, ts, side, price)
            if sz is None:
                sz = as_ if side == "yes" else bs
            out.append({"ticker": t, "ts": ts, "minutes": n, "caught": True, "side": side,
                        "price": price, "size": capped(sz)})
    return out


def settle_strays(opps: list, results: dict) -> None:
    for o in opps:
        if not o.get("caught"):
            o["profit"] = 0.0
            continue
        r = results.get(o["ticker"])
        if r not in ("yes", "no"):
            o["profit"] = None
            continue
        win = 100 if r == o["side"] else 0
        o["profit"] = (win - o["price"] - fee_c(o["price"])) / 100 * o["size"]


def slow_reaction_opps(d: Data, obs_by_station: dict, tz_by_series: dict) -> list[dict]:
    from src.backtest import climate_day_start_utc
    from src.sameday_report import obs_value_f
    from src.sameday import c_to_f_whole
    out = []
    for ev, tickers in d.events.items():
        m0 = d.meta[tickers[0]]
        tz = tz_by_series.get(m0["series"])
        obs = obs_by_station.get(m0["station"])
        if not tz or not obs:
            continue
        try:
            day = datetime.strptime(ev.split("-")[1], "%y%b%d").date()
        except (IndexError, ValueError):
            continue
        start = int(climate_day_start_utc(day, tz).timestamp())
        end = start + 86400
        paths = {t: d.path(t) for t in tickers}
        run_max = None
        for o in obs:
            v = o["valid_ts"]
            if v < start or v >= end:
                continue
            vals = []
            if o.get("t_group_c") is not None:
                vals.append(obs_value_f(o))
            if o.get("max6_c") is not None and v - 6 * 3600 >= start:
                vals.append(c_to_f_whole(o["max6_c"]))
            if not vals:
                continue
            new = max(vals)
            before = run_max
            if before is not None and new <= before:
                continue
            run_max = new if before is None else max(before, new)
            if before is None:
                continue                      # first report of the day: no "new high" moment
            public = v + REPORT_PUBLIC_S
            act_after = public + 60
            for t in tickers:
                side_now = certain_side(d.meta[t], run_max)
                if side_now is None or certain_side(d.meta[t], before) == side_now:
                    continue
                p = paths[t]
                act = next((ts for ts in sorted(p) if ts >= act_after), None)
                if act is None or act - act_after > 120:
                    continue
                b, a, bs, as_ = p[act]
                price = (100 - b) if side_now == "no" and b is not None else (a if side_now == "yes" else None)
                if price is None or price > 99:
                    continue
                edge = 100 - price - fee_c(price)
                pre = p.get(max((ts for ts in p if ts <= public - 300), default=None)) if p else None
                out.append({"ticker": t, "ts": act, "side": side_now, "price": price, "edge_c": edge,
                            "caught": edge >= MIN_NET_C, "size": capped(as_ if side_now == "yes" else bs),
                            "profit": edge / 100 * capped(as_ if side_now == "yes" else bs),
                            "losing_side_before": None if not pre else (pre[0] if side_now == "no" else
                                                                         (100 - pre[1] if pre[1] else None))})
    return out


def event_date(event_ticker: str):
    try:
        return datetime.strptime(event_ticker.split("-")[1], "%y%b%d").date()
    except (IndexError, ValueError, AttributeError):
        return None


def series_kind(series: str) -> str | None:
    s = (series or "").upper()
    return "high" if s.startswith("KXHIGH") else "low" if s.startswith("KXLOWT") else None


def bracket_for(d: "Data", tickers: list, temp: float) -> str | None:
    x = round(temp)
    for t in tickers:
        m = d.meta[t]
        if (m.get("strike_type") or "").lower() == "between" and m["floor_strike"] is not None \
                and m["cap_strike"] is not None and m["floor_strike"] <= x <= m["cap_strike"]:
            return t
    return None


def at_or_after(path: dict, ts: int, within: int = 120):
    keys = sorted(k for k in path if ts <= k <= ts + within)
    return (keys[0], path[keys[0]]) if keys else (None, None)


def forecast_reaction_opps(d: "Data") -> tuple[list, list]:
    """Check D. Returns (episodes, speed) where speed holds, per leg, the share of the
    30-minute midpoint move that had already happened before we could act."""
    by_key = defaultdict(list)                  # (station, kind, date) -> [event tickers]
    for ev, tickers in d.events.items():
        m0 = d.meta[tickers[0]]
        kd, dt = series_kind(m0["series"]), event_date(ev)
        if kd and dt and m0.get("station"):
            by_key[(m0["station"], kd, dt.isoformat())].append(ev)
    order = sorted(d.bulletins)                 # run ids are ISO times: issue order
    out, speed = [], []
    for key, f in d.forecasts.items():
        runs = [r for r in order if r in f]
        for prev, cur in zip(runs, runs[1:]):
            seen_ts, at_start = d.bulletins[cur]
            if at_start or f[cur] is None or f[prev] is None or abs(f[cur] - f[prev]) < FORECAST_SHIFT_F:
                continue
            for ev in by_key.get(key, []):
                tickers = d.events[ev]
                legs = (("toward", bracket_for(d, tickers, f[cur])), ("away", bracket_for(d, tickers, f[prev])))
                if legs[0][1] == legs[1][1]:
                    continue
                for leg, t in legs:
                    if t is None:
                        continue
                    path = d.path(t)
                    act, q0 = at_or_after(path, seen_ts + 60)
                    if act is None:
                        continue
                    ex, q1 = at_or_after(path, act + HOLD_MINUTES * 60, 300)
                    if ex is None:
                        continue
                    b0, a0, bs0, as0 = q0
                    b1, a1 = q1[0], q1[1]
                    if leg == "toward":
                        if a0 is None or b1 is None:
                            continue
                        c = b1 - a0 - fee_c(a0) - fee_c(b1)
                        size = capped(as0)
                    else:
                        if b0 is None or a1 is None:
                            continue
                        c = b0 - a1 - fee_c(100 - b0) - fee_c(100 - a1)
                        size = capped(bs0)
                    out.append({"ticker": t, "ts": act, "leg": leg, "shift": f[cur] - f[prev], "caught": True,
                                "minutes": 1, "per_contract": c, "size": size, "profit": c / 100 * size})
                    _, qp = at_or_after(path, seen_ts - 5 * 60, 300)
                    if qp and None not in (qp[0], qp[1], b0, a0, b1, a1):
                        mp, ma, me = (qp[0] + qp[1]) / 2, (b0 + a0) / 2, (b1 + a1) / 2
                        if abs(me - mp) >= 3:
                            speed.append((ma - mp) / (me - mp))
    return out, speed


def staleness(d: Data, hours_tz: str = "America/New_York") -> dict:
    tz = ZoneInfo(hours_tz)
    agg = defaultdict(lambda: [0, 0, 0, []])       # hour -> [market-minutes, one-sided, wide, spreads]
    for t in d.meta:
        for ts, q in d.path(t).items():
            h = datetime.fromtimestamp(ts, tz).hour
            a = agg[h]
            a[0] += 1
            if q[0] is None or q[1] is None:
                a[1] += 1
            else:
                sp = q[1] - q[0]
                a[3].append(sp)
                if sp > 10:
                    a[2] += 1
    return agg


# --- report -------------------------------------------------------------------

def health(db, d: Data, n_files: int) -> list[str]:
    L = [f"Chunks loaded: {n_files}"]
    days = defaultdict(set)
    for s, mins in d.polled.items():
        for ts in mins:
            days[datetime.fromtimestamp(ts, timezone.utc).date()].add(ts)
    L.append(f"  {'UTC day':12s}{'minutes':>9s}{'coverage':>10s}")
    for day in sorted(days):
        n = len(days[day])
        L.append(f"  {day!s:12s}{n:>9,}{n / 1440:>10.0%}")
    allmins = sorted(set().union(*d.polled.values())) if d.polled else []
    gaps = sorted(((b - a) // 60 - 1, a) for a, b in zip(allmins, allmins[1:]) if b - a > 120)[-5:]
    if gaps:
        L.append("  longest gaps: " + ", ".join(
            f"{g} min after {datetime.fromtimestamp(a, timezone.utc):%m-%d %H:%MZ}" for g, a in reversed(gaps)))
    L.append(f"  series: {len(d.polled)}   markets: {len(d.meta):,}   quote rows: "
             f"{sum(len(v) for v in d.rows.values()):,}   order-book snapshots: "
             f"{sum(len(v) for v in d.depth.values()):,}")
    with_st = sum(1 for m in d.meta.values() if m.get("station"))
    L.append(f"  markets with a settlement station (needed for checks C and D): {with_st:,} of {len(d.meta):,}")
    f = db.execute("SELECT sample FROM fields LIMIT 1").fetchone()
    L.append(f"  size-like fields Kalshi provides: {f[0] if f else 'n/a'}")
    sizes = sum(1 for v in d.rows.values() for r in v if r[3] is not None or r[4] is not None)
    L.append(f"  quote rows with top-of-book sizes: {sizes:,}"
             + ("" if sizes else "  (none: sizes come only from order-book snapshots)"))
    nb = len(d.bulletins)
    known = [(r, v[0]) for r, v in d.bulletins.items() if not v[1]]
    L.append(f"  forecast bulletins: {nb} recorded, {len(known)} with a known release minute; "
             f"forecast rows: {sum(len(v) for v in d.forecasts.values()):,}")
    if known:
        delays = []
        for r, ts in known:
            try:
                run_t = datetime.strptime(r, "%Y-%m-%dT%HZ").replace(tzinfo=timezone.utc).timestamp()
                delays.append((ts - run_t) / 60)
            except ValueError:
                pass
        if delays:
            L.append(f"  bulletin released after its run hour: median {statistics.median(delays):.0f} min "
                     f"(range {min(delays):.0f}-{max(delays):.0f})")
    L.append("\nSTALENESS BY HOUR (Eastern time; all markets, all minutes collected)")
    L.append(f"  {'hour':>5s}{'mkt-minutes':>13s}{'one-sided':>11s}{'spread>10c':>12s}{'median spread':>15s}")
    for h, (n, one, wide, sp) in sorted(staleness(d).items()):
        L.append(f"  {h:>5d}{n:>13,}{one / n:>11.1%}{wide / n:>12.1%}"
                 f"{(str(statistics.median(sp)) + 'c') if sp else '':>15s}")
    return L


def summarize(name: str, opps: list) -> tuple[list[str], bool]:
    L = [f"\n{name}"]
    periods = ("warmup",) if _MODE == "warmup" else ("dev", "hold")
    by = {p: [] for p in periods}
    seen = {p: 0 for p in periods}
    durs = []
    for o in opps:
        p = period_of(o["ts"])
        if not p:
            continue
        seen[p] += 1
        durs.append(o.get("minutes", 1))
        if o.get("caught") and o.get("profit") is not None:
            by[p].append(o["profit"])
    for p in periods:
        v = by[p]
        L.append(f"  {p:6s} seen {seen[p]:>5,}   acted on {len(v):>5,}   "
                 f"profit ${sum(v):>9,.2f}   per episode {'' if not v else f'${sum(v) / len(v):.2f}'}")
    if durs:
        L.append(f"  episode length (minutes): median {statistics.median(durs)}   max {max(durs)}")
    if _MODE == "warmup":
        return L, False
    ok, line = verdict(by["dev"], by["hold"])
    L.append("  " + line)
    return L, ok


def build(db, n_files: int, mode: str, results: dict | None = None, obs_by_station=None,
          tz_by_series=None) -> str:
    global _MODE
    _MODE = "warmup" if mode == "warmup" else "final"
    d = Data(db)
    L = [f"=== Minute study ({mode}) — {datetime.now(timezone.utc):%Y-%m-%d %H:%MZ} ===",
         "Rule (fixed 2026-11-24): see src/minute_report.py. Development Dec 1-7, holdout Dec 8-14 (UTC).", ""]
    L += health(db, d, n_files)
    if mode == "health":
        L.append("\n(health mode: profit sections are only computed in warm-up and final modes)")
        return "\n".join(L)
    if mode == "warmup":
        L.append("\nWARM-UP CHECK: checks A-D on the unscored days before Dec 1, to catch bugs on real data. "
                 "These numbers are NOT the test and may only be used to fix bugs.")
    passes = []
    s, ok = summarize("A. LADDER GAPS (buy every bracket's YES, or every NO)", ladder_opps(d))
    L += s
    passes.append(("A", ok))
    strays = stray_opps(d)
    settle_strays(strays, results or {})
    s, ok = summarize("B. STRAY PRICES (5c+ away from the market's own 30-minute median)", strays)
    L += s
    unsettled = sum(1 for o in strays if o.get("caught") and o.get("profit") is None and period_of(o["ts"]))
    if unsettled:
        L.append(f"  left out: {unsettled:,} stray-price trades whose market had no settlement result")
    passes.append(("B", ok))
    if obs_by_station:
        slow = slow_reaction_opps(d, obs_by_station, tz_by_series or {})
        s, ok = summarize("C. SLOW REACTION TO A NEW HIGH (certain side still cheap 1 min after the report)", slow)
        L += s
        pre = [o["losing_side_before"] for o in slow if o.get("losing_side_before") is not None]
        if pre:
            L.append(f"  losing side's price 5 min BEFORE the report was public: median {statistics.median(pre)}c "
                     f"(low = the market had already moved)")
        passes.append(("C", ok))
    else:
        L.append("\nC. SLOW REACTION: skipped (no observations loaded)")
    fr, speed = forecast_reaction_opps(d)
    s, ok = summarize(f"D. SLOW REACTION TO A NEW FORECAST (median forecast moved >= {FORECAST_SHIFT_F}F; "
                      f"buy 1 min after release, close {HOLD_MINUTES} min later)", fr)
    L += s
    for leg in ("toward", "away"):
        v = [o["per_contract"] for o in fr if o["leg"] == leg and period_of(o["ts"])]
        if v:
            L.append(f"  {leg:6s} leg: {len(v)} trades, average {sum(v) / len(v):+.2f}c per contract after fees")
    if speed:
        L.append(f"  share of the 30-minute price move already done before a 1-minute bot could act: "
                 f"median {statistics.median(speed):.0%} ({len(speed)} moves of 3c+)")
    nb = sum(1 for v in d.bulletins.values() if not v[1])
    L.append(f"  bulletins with a known release minute: {nb}")
    passes.append(("D", ok))
    if mode == "warmup":
        L.append("\n(no verdict in warm-up mode)")
        return "\n".join(L)
    winners = [k for k, ok in passes if ok]
    L.append("\nVERDICT: " + (f"PASS for {', '.join(winners)}" if winners else
                              "no type passes. A bot scanning every minute would not find bad prices it could "
                              "profitably buy."))
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["health", "warmup", "final"], default="health")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--chunks", default="data/minute")
    ap.add_argument("--out-dir", default="data/backtest_reports")
    args = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    if args.mode == "final" and now < STOP_AT and not args.force:
        print(f"Final mode is locked until collection ends ({STOP_AT:%Y-%m-%d %H:%MZ}) so the test can't be "
              f"looked at early. Use health mode, or --force.")
        return 1
    db, n = load(args.chunks)
    results, obs, tzs = {}, {}, {}
    if args.mode in ("final", "warmup"):
        global _MODE
        _MODE = "warmup" if args.mode == "warmup" else "final"
        d = Data(db)
        from src.scan_common import build_client
        try:
            client = build_client()
        except Exception as e:                        # noqa: BLE001 - report the rest anyway
            client = None
            print(f"  (no Kalshi client: {e}; check B's trades will be listed as left out)", file=sys.stderr)
        for o in stray_opps(d) if client else []:
            if o.get("caught") and period_of(o["ts"]) and o["ticker"] not in results:
                try:
                    results[o["ticker"]] = (client.get_market(o["ticker"]) or {}).get("result")
                except Exception:                     # noqa: BLE001
                    results[o["ticker"]] = None
        import requests
        from src import sameday
        from src.backtest import load_cities
        cities, _ = load_cities("config.yaml")
        for c, _shadow in cities:
            tzs[c["kalshi_series_prefix"]] = c["timezone"]
            st = c["expected_station"]
            rows = []
            for y, mth in ((2026, 11), (2026, 12)):
                try:
                    rows += sameday.fetch_iem_month(requests.get, st, y, mth)
                except Exception as e:                # noqa: BLE001
                    print(f"  IEM {st} {y}-{mth}: {e}", file=sys.stderr)
            obs[st] = sorted(rows, key=lambda o: o["valid_ts"])
    text = build(db, n, args.mode, results, obs, tzs)
    print(text)
    os.makedirs(args.out_dir, exist_ok=True)
    path = os.path.join(args.out_dir, f"minute_report_{args.mode}_{now:%Y-%m-%dT%H%MZ}.txt")
    with open(path, "w") as f:
        f.write(text + "\n")
    print(f"\n(saved to {path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
