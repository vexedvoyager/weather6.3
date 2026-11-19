"""
Backtest data collector (step 2 of the backtest plan - see "claude/backtest-plan.md"
in the Weather Bot Claude project).

For every day in a month it replays what the bot would have seen:
  * the 13Z NBM probabilistic bulletin (NBP) from NOAA's AWS archive, parsed
    with the bot's OWN parser (src/nbm.py) and daily-high column choice;
  * every Kalshi daily-high market for the next day, with its real outcome and
    the YES bid/ask at the decision hour (19Z, a few hours after the 13Z
    bulletin is published) from Kalshi's hourly candlesticks;
  * the bot's OWN probability (forecast_refresh.model_probability_for_market,
    v6.3 settings: whole-degree settlement, 1%/99% bounds, sigma 1.15).

Read-only with respect to trading: never places orders and never touches
data/trades.db. Everything goes into data/backtest.db. Re-running a month skips
city-days already collected (city-days that hit an error are retried).

No look-ahead: only the bulletin issued BEFORE the decision hour is used, and
only candles that END at or before the decision hour.

Usage:  python -m src.backtest --months 2025-03            (one month)
        python -m src.backtest --months 2024-11..2025-04   (a range, oldest first)
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests
import yaml

from src import nbm
from src.forecast_refresh import model_probability_for_market
from src.kalshi_client import dollars_to_cents
from src.market_parsing import extract_threshold

logger = logging.getLogger("backtest")

AWS_BASE = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
DEFAULT_DB = "data/backtest.db"
RUN_HOUR = 13            # NBM run used (published ~14-15Z)
DECISION_HOUR = 19       # UTC hour the simulated decision is made, on the run date
QUOTE_LOOKBACK_H = 6     # use the last quote within this many hours before the decision
VOLUME_WINDOW_H = 24     # candles fetched for the 24h volume figure
REQUEST_PAUSE_S = 0.08   # stay well under Kalshi's read rate limit
_VERSION_RE = re.compile(r"NBM\s+V(\d+\.\d+)", re.IGNORECASE)

SCHEMA = """
CREATE TABLE IF NOT EXISTS bt_city_days (
    city TEXT NOT NULL,
    market_date TEXT NOT NULL,          -- the date the markets are about
    status TEXT NOT NULL,               -- ok | no_markets | no_forecast | error
    detail TEXT,
    series_prefix TEXT,
    run_id TEXT,
    nbm_version TEXT,
    markets_scored INTEGER DEFAULT 0,
    collected_at TEXT NOT NULL,
    PRIMARY KEY (city, market_date)
);
CREATE TABLE IF NOT EXISTS bt_markets (
    ticker TEXT PRIMARY KEY,
    city TEXT NOT NULL,
    is_shadow INTEGER NOT NULL,         -- 1 = a shadow city in config.yaml (never traded live)
    series_prefix TEXT,
    market_date TEXT NOT NULL,
    run_id TEXT NOT NULL,
    nbm_version TEXT,
    forecast_hour INTEGER,
    ladder_json TEXT NOT NULL,          -- p10..p90 for the daily high
    kind TEXT NOT NULL,                 -- 'bracket' or 'threshold'
    strike_type TEXT,
    floor_strike REAL,
    cap_strike REAL,
    model_prob REAL NOT NULL,           -- clamped P(YES), exactly as the bot computes it
    raw_model_prob REAL,
    yes_ask_cents INTEGER,              -- quote at the decision hour (NULL = no quote)
    yes_bid_cents INTEGER,
    quote_ts INTEGER,                   -- end of the candle the quote came from (unix UTC)
    volume_24h INTEGER,                 -- contracts traded in the 24h before the decision
    outcome TEXT,                       -- 'yes' / 'no' from Kalshi's result
    expiration_value TEXT,              -- the actual high when Kalshi recorded it
    collected_at TEXT NOT NULL
);
"""


# --- small helpers (unit-tested) ---------------------------------------------

def aws_bulletin_url(run_date: date, run_hour: int = RUN_HOUR) -> str:
    return (f"{AWS_BASE}/blend.{run_date:%Y%m%d}/{run_hour:02d}/text/"
            f"blend_nbptx.t{run_hour:02d}z")


def run_id_for(run_date: date, run_hour: int = RUN_HOUR) -> str:
    return f"{run_date:%Y-%m-%d}T{run_hour:02d}Z"


def event_ticker(prefix: str, market_date: date) -> str:
    return f"{prefix}-{market_date:%y}{market_date.strftime('%b').upper()}{market_date:%d}"


def prefix_candidates(prefix: str) -> list[str]:
    """The configured series prefix first, then older spellings Kalshi has used
    (KXHIGHTPHX -> KXHIGHPHX; KXHIGHNY -> HIGHNY). Order matters: first hit wins."""
    out = [prefix]
    if prefix.startswith("KXHIGHT"):
        out.append("KXHIGH" + prefix[len("KXHIGHT"):])
    for p in list(out):
        if p.startswith("KX"):
            out.append(p[2:])
    seen, uniq = set(), []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def parse_months(spec: str) -> list[tuple[int, int]]:
    """'2025-03' -> [(2025, 3)]; '2024-11..2025-01' -> three months; commas allowed."""
    months = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ".." in part:
            a, b = part.split("..")
            y, m = map(int, a.split("-"))
            y2, m2 = map(int, b.split("-"))
            if (y2, m2) < (y, m):
                raise ValueError(f"range runs backwards: {part}")
            while (y, m) <= (y2, m2):
                months.append((y, m))
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        else:
            y, m = map(int, part.split("-"))
            months.append((y, m))
    return months


def days_in_month(year: int, month: int) -> list[date]:
    d = date(year, month, 1)
    out = []
    while d.month == month:
        out.append(d)
        d += timedelta(days=1)
    return out


def bulletin_version(block_header: str) -> str | None:
    m = _VERSION_RE.search(block_header or "")
    return m.group(1) if m else None


def _cents(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return dollars_to_cents(v)
    except (TypeError, ValueError):
        return None


def quote_at_decision(candles: list[dict], decision_ts: int,
                      lookback_h: int = QUOTE_LOOKBACK_H) -> dict | None:
    """The YES bid/ask as of the decision time: the close of the latest candle
    that ENDS at or before decision_ts (never a later one - no look-ahead), and
    no older than lookback_h. Returns {"yes_ask", "yes_bid", "ts"} in cents, or
    None if there is no usable quote. A missing/zero bid is kept as 0 (no bid);
    an ask of 0 or 100+ means no real offer and is treated as no quote."""
    usable = [c for c in candles
              if c.get("end_period_ts") is not None
              and decision_ts - lookback_h * 3600 <= c["end_period_ts"] <= decision_ts]
    for c in sorted(usable, key=lambda c: c["end_period_ts"], reverse=True):
        ask = _cents((c.get("yes_ask") or {}).get("close_dollars",
                                                  (c.get("yes_ask") or {}).get("close")))
        bid = _cents((c.get("yes_bid") or {}).get("close_dollars",
                                                  (c.get("yes_bid") or {}).get("close")))
        if ask is None or ask <= 0 or ask >= 100:
            continue
        return {"yes_ask": ask, "yes_bid": bid or 0, "ts": c["end_period_ts"]}
    return None


def volume_before(candles: list[dict], decision_ts: int, window_h: int = VOLUME_WINDOW_H) -> int:
    total = 0.0
    for c in candles:
        ts = c.get("end_period_ts")
        if ts is not None and decision_ts - window_h * 3600 < ts <= decision_ts:
            try:
                total += float(c.get("volume_fp", c.get("volume")) or 0)
            except (TypeError, ValueError):
                pass
    return int(total)


# --- data sources --------------------------------------------------------------

class Sources:
    """Network access, isolated so tests can substitute fakes."""

    def __init__(self, client, http_get=requests.get, pause_s: float = REQUEST_PAUSE_S):
        self.client = client
        self.http_get = http_get
        self.pause_s = pause_s

    def bulletin(self, run_date: date, run_hour: int) -> str | None:
        url = aws_bulletin_url(run_date, run_hour)
        for attempt in range(3):
            try:
                r = self.http_get(url, timeout=180)
                if r.status_code == 200:
                    return r.text
                if r.status_code == 404:
                    return None
            except requests.RequestException as e:
                logger.warning("bulletin %s attempt %d failed: %s", url, attempt + 1, e)
            time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"could not download {url}")

    def _get(self, path, params=None):
        time.sleep(self.pause_s)
        return self.client._request("GET", path, params=params)

    def markets(self, event: str) -> list[dict]:
        """Settled markets for one event, from the historical tier, falling back
        to the live tier for events settled after Kalshi's historical cutoff."""
        for path in ("/historical/markets", "/markets"):
            try:
                found = (self._get(path, {"event_ticker": event, "limit": 100}) or {}).get("markets", [])
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (400, 404):
                    continue
                raise
            if found:
                return found
        return []

    def candles(self, series: str, ticker: str, start_ts: int, end_ts: int) -> list[dict]:
        params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 60}
        for path in (f"/historical/markets/{ticker}/candlesticks",
                     f"/series/{series}/markets/{ticker}/candlesticks"):
            try:
                found = (self._get(path, params) or {}).get("candlesticks", [])
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (400, 404):
                    continue
                raise
            if found:
                return found
        return []


# --- collection --------------------------------------------------------------

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def _done(conn, city: str, market_date: date) -> bool:
    row = conn.execute("SELECT status FROM bt_city_days WHERE city=? AND market_date=?",
                       (city, market_date.isoformat())).fetchone()
    return row is not None and row[0] != "error"


def _mark(conn, city, market_date, status, detail="", prefix=None, run_id=None,
          version=None, n=0):
    conn.execute(
        "INSERT OR REPLACE INTO bt_city_days (city, market_date, status, detail, series_prefix,"
        " run_id, nbm_version, markets_scored, collected_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (city, market_date.isoformat(), status, (detail or "")[:300], prefix, run_id, version, n,
         datetime.now(timezone.utc).isoformat()))


def collect_city_day(conn, src: Sources, city: dict, is_shadow: bool, parsed: dict | None,
                     run_id: str, version: str | None, run_date: date, market_date: date,
                     sigma: float, decision_hour: int = DECISION_HOUR,
                     prefix_hint: dict | None = None) -> str:
    name = city["name"]
    if parsed is None:
        _mark(conn, name, market_date, "no_forecast", "station not parsed", run_id=run_id,
              version=version)
        return "no_forecast"
    chosen = nbm.get_max_temp_forecast(parsed, run_id, market_date)
    if chosen is None:
        _mark(conn, name, market_date, "no_forecast", "no daily-high column", run_id=run_id,
              version=version)
        return "no_forecast"
    pct = chosen["values"]

    prefixes = prefix_candidates(city["kalshi_series_prefix"])
    hint = (prefix_hint or {}).get(name)
    if hint in prefixes:
        prefixes = [hint] + [p for p in prefixes if p != hint]
    markets, prefix = [], None
    for p in prefixes:
        markets = src.markets(event_ticker(p, market_date))
        if markets:
            prefix = p
            break
    if not markets:
        _mark(conn, name, market_date, "no_markets", "tried " + ",".join(prefixes),
              run_id=run_id, version=version)
        return "no_markets"
    if prefix_hint is not None:
        prefix_hint[name] = prefix

    decision_ts = int(datetime(run_date.year, run_date.month, run_date.day, decision_hour,
                               tzinfo=timezone.utc).timestamp())
    start_ts = decision_ts - VOLUME_WINDOW_H * 3600
    n = 0
    now = datetime.now(timezone.utc).isoformat()
    for m in markets:
        ticker = m.get("ticker")
        result = m.get("result")
        threshold = extract_threshold(m)
        if not ticker or threshold is None or result not in ("yes", "no"):
            continue
        try:
            probs = model_probability_for_market(threshold, pct, sigma)
        except (ValueError, RuntimeError) as e:
            logger.warning("probability failed %s: %s", ticker, e)
            continue
        candles = src.candles(prefix, ticker, start_ts, decision_ts)
        q = quote_at_decision(candles, decision_ts)
        conn.execute(
            "INSERT OR REPLACE INTO bt_markets (ticker, city, is_shadow, series_prefix, market_date,"
            " run_id, nbm_version, forecast_hour, ladder_json, kind, strike_type, floor_strike,"
            " cap_strike, model_prob, raw_model_prob, yes_ask_cents, yes_bid_cents, quote_ts,"
            " volume_24h, outcome, expiration_value, collected_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ticker, name, int(is_shadow), prefix, market_date.isoformat(), run_id, version,
             chosen["forecast_hour"], json.dumps(pct),
             "bracket" if threshold["kind"] == "between" else "threshold",
             m.get("strike_type"), m.get("floor_strike"), m.get("cap_strike"),
             probs["clamped"], probs["raw"],
             q["yes_ask"] if q else None, q["yes_bid"] if q else None, q["ts"] if q else None,
             volume_before(candles, decision_ts), result,
             str(m.get("expiration_value") or "") or None, now))
        n += 1
    _mark(conn, name, market_date, "ok", "", prefix, run_id, version, n)
    return "ok"


def collect_month(conn, src: Sources, cities: list[tuple[dict, bool]], year: int, month: int,
                  sigma: float, run_hour: int = RUN_HOUR, decision_hour: int = DECISION_HOUR,
                  redo: bool = False) -> dict:
    counts = {"ok": 0, "no_markets": 0, "no_forecast": 0, "error": 0, "skipped": 0,
              "no_bulletin": 0}
    prefix_hint = {}
    for run_date in days_in_month(year, month):
        market_date = run_date + timedelta(days=1)
        todo = [(c, s) for c, s in cities if redo or not _done(conn, c["name"], market_date)]
        counts["skipped"] += len(cities) - len(todo)
        if not todo:
            continue
        run_id = run_id_for(run_date, run_hour)
        try:
            text = src.bulletin(run_date, run_hour)
        except RuntimeError as e:
            for c, _ in todo:
                _mark(conn, c["name"], market_date, "error", str(e), run_id=run_id)
            counts["error"] += len(todo)
            conn.commit()
            continue
        if text is None:
            for c, _ in todo:
                _mark(conn, c["name"], market_date, "no_forecast", "bulletin not in archive",
                      run_id=run_id)
            counts["no_bulletin"] += 1
            counts["no_forecast"] += len(todo)
            conn.commit()
            continue

        parsed_by_city, version, ladders = {}, None, {}
        for c, _ in cities:
            block = nbm.get_station_block(text, c["expected_station"])
            if block and version is None:
                version = bulletin_version(block[0])
            # Parse from the station's own block, not the whole 35 MB file (same result, faster).
            parsed = nbm.parse_station_maxt("\n".join(block), c["expected_station"]) if block else None
            parsed_by_city[c["name"]] = parsed
            if parsed:
                ladders[c["name"]] = json.dumps({k: parsed[k] for k in ("p10", "p50", "p90")})
        if len(ladders) >= 3 and len(set(ladders.values())) == 1:
            # Same guard as forecast_refresh (v6.3): identical stations = broken parse.
            for c, _ in todo:
                _mark(conn, c["name"], market_date, "error", "identical forecasts - parse broken",
                      run_id=run_id, version=version)
            counts["error"] += len(todo)
            conn.commit()
            continue

        for c, is_shadow in todo:
            try:
                status = collect_city_day(conn, src, c, is_shadow, parsed_by_city.get(c["name"]),
                                          run_id, version, run_date, market_date, sigma,
                                          decision_hour, prefix_hint)
            except Exception as e:      # one bad city-day never stops the month
                logger.exception("city-day failed %s %s", c["name"], market_date)
                _mark(conn, c["name"], market_date, "error", f"{type(e).__name__}: {e}",
                      run_id=run_id, version=version)
                status = "error"
            counts[status] += 1
        conn.commit()
        logger.info("%s done (NBM V%s): %s", market_date, version, counts)
    return counts


def load_cities(config_path: str = "config.yaml") -> tuple[list[tuple[dict, bool]], float]:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    cities = [(c, False) for c in cfg["cities"]] + \
             [(c, True) for c in (cfg.get("shadow_cities") or [])]
    return cities, float(cfg["probability_model"]["sigma_multiplier"])


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True, help="e.g. 2025-03 or 2024-11..2025-04")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--redo", action="store_true", help="re-collect city-days already stored")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from src.scan_common import build_client
    cities, sigma = load_cities()
    src = Sources(build_client())
    conn = open_db(args.db)
    for y, m in parse_months(args.months):
        started = time.time()
        counts = collect_month(conn, src, cities, y, m, sigma, redo=args.redo)
        print(f"{y}-{m:02d}: {counts}  ({time.time() - started:.0f}s)", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
