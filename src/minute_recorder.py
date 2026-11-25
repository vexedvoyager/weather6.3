"""
Minute recorder: are Kalshi's weather prices ever wrong for a few minutes?
(added 2026-11-24; see "claude/project-status.md" in the Claude project)

Our hourly data showed weather prices are accurate hour by hour. This records
EVERY MINUTE, for all open daily high/low temperature markets, so the report
(src/minute_report.py) can look for short-lived bad prices a fast bot could buy:
ladder gaps, stray orders, and slow reactions to new weather reports.

What it stores (read-only; never trades):
  quotes  - best YES bid / ask (and top-of-book sizes when Kalshi provides them)
            for each market, written only when something changes, plus a full
            copy at the top of every hour.
  polls   - one row per series per minute that was polled successfully, so the
            report knows the difference between "no change" and "no data".
  depth   - the full order book of a market, saved the moment a possible bad
            price is seen (ladder gap or stray price), to know how many
            contracts were really available.
  meta    - each market's ticker, event, strike and settlement station.
  fields  - which size-like fields Kalshi's market objects actually contain.
  bulletins / forecasts - (check D) the minute each NOAA NBP forecast bulletin
            first became downloadable, and the 13 configured cities' forecast
            highs and lows (p10..p90) from it. A bulletin already present when a
            run starts is marked at_start: its true release time is unknown.

Runs in chunks (default 345 minutes, under GitHub's 6-hour job limit); each run
writes its own file data/minute/chunk_<UTC start>.db.gz. Collection stops by
itself at STOP_AT.
"""
import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sqlite3
import statistics
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

logger = logging.getLogger("minute_recorder")

STOP_AT = datetime(2026, 12, 15, 6, 0, tzinfo=timezone.utc)
SERIES_RE = re.compile(r"^KX(HIGH|LOWT)[A-Z]+$")
SERIES_EXCLUDE = ("PERIOD",)
STRAY_CENTS = 5             # a quote this far beyond the market's own recent median midpoint
STRAY_LOOKBACK = 30         # minutes of history for that median
STRAY_MIN_HISTORY = 20
MAX_DEPTH_PER_MINUTE = 25
META_REFRESH_MIN = 60

SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (ts INTEGER, ticker TEXT, bid INTEGER, ask INTEGER,
    bid_size REAL, ask_size REAL, volume REAL, keyframe INTEGER);
CREATE TABLE IF NOT EXISTS polls (ts INTEGER, series TEXT, n_markets INTEGER);
CREATE TABLE IF NOT EXISTS depth (ts INTEGER, ticker TEXT, reason TEXT, yes_levels TEXT, no_levels TEXT);
CREATE TABLE IF NOT EXISTS meta (ticker TEXT PRIMARY KEY, event TEXT, series TEXT, strike_type TEXT,
    floor_strike REAL, cap_strike REAL, close_time TEXT, station TEXT);
CREATE TABLE IF NOT EXISTS fields (series TEXT PRIMARY KEY, keys TEXT, sample TEXT);
CREATE TABLE IF NOT EXISTS run_info (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS bulletins (run_id TEXT, first_seen_ts INTEGER, at_start INTEGER, url TEXT);
CREATE TABLE IF NOT EXISTS forecasts (run_id TEXT, station TEXT, kind TEXT, market_date TEXT,
    p10 REAL, p25 REAL, p50 REAL, p75 REAL, p90 REAL);
"""

AT_START_S = 180              # bulletins found in a run's first 3 minutes count as "already out"
BULLETIN_RETRY_MIN = 5        # re-read a bulletin that doesn't yet parse for every city, up to this long
BULLETIN_LOOKBACK_H = 8        # watch runs issued up to 8 h ago (so a new run also records the previous bulletin as a baseline)


# --- pure helpers (unit-tested) -----------------------------------------------

def is_daily_temp_series(ticker: str) -> bool:
    t = str(ticker or "").upper()
    return bool(SERIES_RE.match(t)) and not any(x in t for x in SERIES_EXCLUDE)


def cents(v) -> int | None:
    try:
        return None if v is None or v == "" else round(float(v) * 100)
    except (TypeError, ValueError):
        return None


def num(v) -> float | None:
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def first_present(m: dict, keys: tuple):
    for k in keys:
        if m.get(k) not in (None, ""):
            return m.get(k)
    return None


def quote_from_market(m: dict) -> dict:
    """Best YES bid/ask in cents. YES ask = 100 - best NO bid (Kalshi lists bids only).
    0 or missing on a side means nobody is bidding there."""
    yb = cents(m.get("yes_bid_dollars"))
    nb = cents(m.get("no_bid_dollars"))
    if yb is None:
        yb = m.get("yes_bid")
    if nb is None and m.get("no_bid") is not None:
        nb = m.get("no_bid")
    ask = None if nb in (None, 0) else 100 - int(nb)
    if nb is None:                                     # no NO-bid field at all: use Kalshi's direct ask
        ya = cents(m.get("yes_ask_dollars"))
        ask = ya if ya is not None and 0 < ya < 100 else None
    bid = None if yb in (None, 0) else int(yb)
    return {
        "bid": bid, "ask": ask,
        "bid_size": num(first_present(m, ("yes_bid_size_fp", "yes_bid_size"))),
        "ask_size": num(first_present(m, ("no_bid_size_fp", "no_bid_size", "yes_ask_size_fp", "yes_ask_size"))),
        "volume": num(first_present(m, ("volume_fp", "volume"))),
    }


def size_like_keys(m: dict) -> list:
    return sorted(k for k in m if any(w in k for w in ("size", "liquidity", "depth", "interest")))


def ladder_gap(quotes: dict) -> tuple[int, int] | None:
    """(sum of YES asks, sum of YES bids) across a ladder, or None if any leg lacks a
    two-sided quote. Buying every YES pays exactly 100c; so does buying every NO."""
    asks, bids = [], []
    for q in quotes.values():
        if q.get("ask") is None or q.get("bid") is None:
            return None
        asks.append(q["ask"])
        bids.append(q["bid"])
    return sum(asks), sum(bids)


def fee_c(price_c: float) -> float:
    """Kalshi taker fee in cents per contract: 0.07 x P x (1 - P) dollars."""
    p = price_c / 100.0
    return 7.0 * p * (1 - p)


def ladder_profit(quotes: dict) -> float | None:
    """Best after-fee profit in cents per set from buying every YES or every NO, or None."""
    g = ladder_gap(quotes)
    if g is None:
        return None
    asks = [q["ask"] for q in quotes.values()]
    bids = [q["bid"] for q in quotes.values()]
    return max(100 - g[0] - sum(fee_c(a) for a in asks), g[1] - 100 - sum(fee_c(b) for b in bids))


def is_stray(history, bid, ask) -> str | None:
    """'ask' if the ask sits STRAY_CENTS below the market's recent median midpoint,
    'bid' if the bid sits STRAY_CENTS above it; else None."""
    mids = [h for h in history if h is not None]
    if len(mids) < STRAY_MIN_HISTORY:
        return None
    med = statistics.median(mids)
    if ask is not None and ask <= med - STRAY_CENTS:
        return "ask"
    if bid is not None and bid >= med + STRAY_CENTS:
        return "bid"
    return None


def changed(prev: dict | None, q: dict) -> bool:
    if prev is None:
        return True
    return any(prev.get(k) != q.get(k) for k in ("bid", "ask", "bid_size", "ask_size"))


# --- forecast bulletin watcher (check D) ------------------------------------------

def bulletin_urls(run_date, hour: int) -> list:
    """Every address pattern the live bot tries (src/nbm.py PATH_TEMPLATES), in the same order."""
    from src import nbm
    return [t.format(host=nbm.NBM_HOST, date=run_date.strftime("%Y%m%d"), hour=hour) for t in nbm.PATH_TEMPLATES]


def due_runs(now_dt: datetime, lookback_h: int = BULLETIN_LOOKBACK_H) -> list:
    """(run_id, run_date, hour) for NBP runs issued in the last lookback_h hours, newest first."""
    from datetime import timedelta
    from src import nbm
    out = []
    for back in range(0, 2):
        d = (now_dt - timedelta(days=back)).date()
        for h in sorted(nbm.RUN_HOURS, reverse=True):
            run_t = datetime(d.year, d.month, d.day, h, tzinfo=timezone.utc)
            if run_t <= now_dt and (now_dt - run_t).total_seconds() <= lookback_h * 3600:
                out.append((f"{d.isoformat()}T{h:02d}Z", d, h))
    return sorted(out, key=lambda r: r[0], reverse=True)


def extract_forecasts(text: str, run_id: str, stations: list, run_date) -> list:
    """Rows (station, kind, market_date, p10..p90) for the market dates this bulletin covers."""
    from datetime import timedelta
    from src import nbm
    from src.backtest import get_min_temp_forecast
    rows = []
    for st in stations:
        parsed = nbm.parse_station_maxt(text, st)
        if not parsed:
            continue
        for k in range(-1, 4):
            d = run_date + timedelta(days=k)
            for kind, fn in (("high", nbm.get_max_temp_forecast), ("low", get_min_temp_forecast)):
                f = fn(parsed, run_id, d)
                if f:
                    v = f["values"]
                    rows.append((st, kind, d.isoformat(), v["p10"], v["p25"], v["p50"], v["p75"], v["p90"]))
    return rows


class ForecastWatcher:
    """Checks once a minute whether a due NBP bulletin has appeared; downloads it once."""

    def __init__(self, conn, stations: list, http_head=None, http_get=None):
        import requests
        self.conn, self.stations = conn, stations
        self.head = http_head or (lambda url: requests.head(url, timeout=20, allow_redirects=True))
        self.get = http_get or (lambda url: requests.get(url, timeout=120))
        self.seen = set()
        self.first_found = {}          # run_id -> minute the file was first available
        self.start_ts = None

    def _available(self, url) -> str | None:
        """The bulletin text if it's downloadable now, else None. Falls back to a
        plain download if the server refuses the cheap HEAD check."""
        r = self.head(url)
        if r.status_code in (403, 405, 501):
            g = self.get(url)
            return g.text if g.status_code == 200 else None
        if r.status_code != 200:
            return None
        g = self.get(url)
        return g.text if getattr(g, "status_code", 200) == 200 else None

    def poll(self, ts: int) -> int:
        if self.start_ts is None:
            self.start_ts = ts
        now_dt = datetime.fromtimestamp(ts, timezone.utc)
        found = 0
        for run_id, d, h in due_runs(now_dt):
            if run_id in self.seen:
                continue
            text, url = None, None
            for u in bulletin_urls(d, h):
                try:
                    text = self._available(u)
                except Exception as e:                 # noqa: BLE001
                    logger.debug("bulletin %s check failed at %s: %s", run_id, u, e)
                    text = None
                if text is not None:
                    url = u
                    break
            if text is None:
                continue
            first = self.first_found.setdefault(run_id, ts)
            rows = extract_forecasts(text, run_id, self.stations, d)
            complete = len({r[0] for r in rows}) >= len(self.stations)
            if not complete and ts - first < BULLETIN_RETRY_MIN * 60:
                continue                               # probably still uploading: read it again next minute
            at_start = int(first - self.start_ts < AT_START_S)
            self.conn.execute("INSERT INTO bulletins VALUES (?,?,?,?)", (run_id, first, at_start, url))
            self.conn.executemany("INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?)",
                                  [(run_id,) + r for r in rows])
            self.seen.add(run_id)
            found += 1
        return found


# --- recorder -----------------------------------------------------------------

class Recorder:
    def __init__(self, client, conn, now_fn=time.time, sleep_fn=time.sleep, watcher=None):
        self.client, self.conn, self.now, self.sleep = client, conn, now_fn, sleep_fn
        self.watcher = watcher
        self.last = {}                                   # ticker -> last written quote
        self.mids = defaultdict(lambda: deque(maxlen=STRAY_LOOKBACK))
        self.meta_seen = set()
        self.series = []
        self.series_refreshed = -10 ** 9
        self.fields_seen = set()
        self.event_station = {}                          # event -> station (looked up once if missing)
        self.stats = defaultdict(int)

    def refresh_series(self, minute):
        try:
            all_series = self.client.get_series_list("Climate and Weather")
            found = sorted({s.get("ticker", "").upper() for s in all_series if is_daily_temp_series(s.get("ticker"))})
            if found:
                self.series = found
                self.series_refreshed = minute
        except Exception as e:                         # noqa: BLE001
            logger.warning("series list failed: %s", e)

    def save_meta(self, series, m):
        t = m.get("ticker")
        if t in self.meta_seen:
            return
        from src.backtest import settlement_station
        ev = m.get("event_ticker")
        station = settlement_station(m) or self.event_station.get(ev)
        if station is None and ev not in self.event_station:
            # The market list may leave out the rules text; read one full market per event.
            try:
                station = settlement_station(self.client.get_market(t) or {})
            except Exception as e:                     # noqa: BLE001
                logger.debug("get_market %s failed: %s", t, e)
            self.event_station[ev] = station
        elif station is not None:
            self.event_station.setdefault(ev, station)
        self.conn.execute("INSERT OR IGNORE INTO meta VALUES (?,?,?,?,?,?,?,?)", (
            t, ev, series, m.get("strike_type"), num(m.get("floor_strike")),
            num(m.get("cap_strike")), m.get("close_time"), station))
        self.meta_seen.add(t)

    def depth(self, ts, ticker, reason):
        try:
            ob = self.client.get_orderbook(ticker) or {}
            self.conn.execute("INSERT INTO depth VALUES (?,?,?,?,?)", (
                ts, ticker, reason, json.dumps(ob.get("yes_dollars") or []), json.dumps(ob.get("no_dollars") or [])))
            self.stats["depth"] += 1
        except Exception as e:                         # noqa: BLE001
            self.stats["depth_err"] += 1
            logger.debug("orderbook %s failed: %s", ticker, e)

    def poll_once(self, ts):
        minute = ts // 60
        if not self.series or minute - self.series_refreshed >= META_REFRESH_MIN:
            self.refresh_series(minute)
        keyframe = (ts // 60) % 60 == 0
        by_event = defaultdict(dict)
        triggers = []
        for s in self.series:
            try:
                markets = self.client.get_markets_by_series(s, status="open")
            except Exception as e:                     # noqa: BLE001
                self.stats["series_err"] += 1
                logger.debug("markets %s failed: %s", s, e)
                continue
            self.conn.execute("INSERT INTO polls VALUES (?,?,?)", (ts, s, len(markets)))
            if markets and s not in self.fields_seen:
                self.conn.execute("INSERT OR REPLACE INTO fields VALUES (?,?,?)", (
                    s, json.dumps(sorted(markets[0].keys())),
                    json.dumps({k: markets[0].get(k) for k in size_like_keys(markets[0])})))
                self.fields_seen.add(s)
            for m in markets:
                t = m.get("ticker")
                if not t:
                    continue
                self.save_meta(s, m)
                q = quote_from_market(m)
                if keyframe or changed(self.last.get(t), q):
                    self.conn.execute("INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?)", (
                        ts, t, q["bid"], q["ask"], q["bid_size"], q["ask_size"], q["volume"], int(keyframe)))
                    self.last[t] = q
                    self.stats["rows"] += 1
                stray = is_stray(self.mids[t], q["bid"], q["ask"])
                if stray:
                    triggers.append((t, f"stray_{stray}"))
                if q["bid"] is not None and q["ask"] is not None:
                    self.mids[t].append((q["bid"] + q["ask"]) / 2)
                by_event[m.get("event_ticker")][t] = q
        for ev, qs in by_event.items():
            if len(qs) < 5:
                continue
            prof = ladder_profit(qs)
            if prof is not None and prof > 0:            # same after-fee test the report uses
                triggers.extend((t, "ladder") for t in qs)
        seen = set()
        for t, reason in triggers:
            if t in seen or len(seen) >= MAX_DEPTH_PER_MINUTE:
                continue
            seen.add(t)
            self.depth(ts, t, reason)
        if self.watcher is not None:
            try:
                self.stats["bulletins"] += self.watcher.poll(ts)
            except Exception as e:                     # noqa: BLE001
                self.stats["bulletin_err"] += 1
                logger.warning("bulletin watcher failed: %s", e)
        self.conn.commit()
        self.stats["minutes"] += 1

    stop_requested = False

    def run(self, minutes: int, stop_at: datetime = STOP_AT):
        end = self.now() + minutes * 60
        while self.now() < end and self.now() < stop_at.timestamp() and not self.stop_requested:
            start = self.now()
            ts = int(start // 60 * 60)
            try:
                self.poll_once(ts)
            except Exception as e:                     # noqa: BLE001 - never die mid-run
                self.stats["minute_err"] += 1
                logger.warning("minute %s failed: %s", ts, e)
            next_minute = (int(start // 60) + 1) * 60
            while not self.stop_requested and self.now() < next_minute:
                self.sleep(min(1.0, max(0.0, next_minute - self.now())))
        return dict(self.stats)


def open_chunk(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def summary_markdown(conn, stats: dict, started: datetime) -> str:
    n_series = conn.execute("SELECT COUNT(DISTINCT series) FROM polls").fetchone()[0]
    n_markets = conn.execute("SELECT COUNT(*) FROM meta").fetchone()[0]
    n_minutes = conn.execute("SELECT COUNT(DISTINCT ts) FROM polls").fetchone()[0]
    keys = conn.execute("SELECT sample FROM fields LIMIT 1").fetchone()
    return "\n".join([
        f"### Minute recorder — chunk started {started:%Y-%m-%d %H:%MZ}",
        f"- minutes recorded: **{n_minutes}** (errors: {stats.get('minute_err', 0)})",
        f"- series polled: {n_series}; markets seen: {n_markets} "
        f"({conn.execute('SELECT COUNT(*) FROM meta WHERE station IS NOT NULL').fetchone()[0]} with a settlement station)",
        f"- quote rows written: {stats.get('rows', 0):,}; order-book snapshots: {stats.get('depth', 0)}",
        f"- forecast bulletins caught: {stats.get('bulletins', 0)} "
        f"({conn.execute('SELECT COUNT(*) FROM bulletins WHERE at_start=0').fetchone()[0]} with a known release minute; "
        f"{conn.execute('SELECT COUNT(*) FROM forecasts').fetchone()[0]} forecast rows)",
        f"- size-like fields in Kalshi's market data: `{keys[0] if keys else 'n/a'}`",
    ])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=345)
    ap.add_argument("--out-dir", default="data/minute")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    started = datetime.now(timezone.utc)
    if started >= STOP_AT:
        print(f"Collection ended at {STOP_AT:%Y-%m-%d %H:%MZ}; nothing to do.")
        return 0
    from src.scan_common import build_client
    os.makedirs(args.out_dir, exist_ok=True)
    tmp = f"chunk_{started:%Y%m%dT%H%MZ}.db"
    conn = open_chunk(tmp)
    conn.execute("INSERT OR REPLACE INTO run_info VALUES ('started', ?)", (started.isoformat(),))
    from src.backtest import load_cities
    cities, _ = load_cities("config.yaml")
    stations = sorted({c["expected_station"] for c, _shadow in cities})
    rec = Recorder(build_client(), conn, watcher=ForecastWatcher(conn, stations))

    def _stop(signum, frame):                          # GitHub cancels with SIGINT/SIGTERM: save what we have
        logger.warning("stop signal %s received; saving this chunk", signum)
        rec.stop_requested = True
    import signal
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        stats = rec.run(args.minutes)
    except BaseException as e:                         # noqa: BLE001 - always save the chunk
        logger.error("recorder stopped early: %s", e)
        stats = dict(rec.stats)
        stats["fatal"] = str(e)
    conn.execute("INSERT OR REPLACE INTO run_info VALUES ('stats', ?)", (json.dumps(stats),))
    conn.commit()
    md = summary_markdown(conn, stats, started)
    conn.close()
    out = os.path.join(args.out_dir, tmp + ".gz")
    with open(tmp, "rb") as f, gzip.open(out, "wb", compresslevel=9) as g:
        shutil.copyfileobj(f, g)
    os.remove(tmp)
    print(md)
    print(f"saved {out} ({os.path.getsize(out) / 1e6:.1f} MB)")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(md + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
