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

Daily LOWS (added 2026-11-20): --kind low collects Kalshi's daily-low markets
instead, using NBM's overnight-MINIMUM column (valid 12Z on the market date) and
writing to data/backtest_low.db. Every low market's settlement station is read
from Kalshi's own rules and must match the city's station, or the day is skipped.

Usage:  python -m src.backtest --months 2025-03            (one month, daily highs)
        python -m src.backtest --months 2024-11..2025-04   (a range, oldest first)
        python -m src.backtest --kind low --months 2025-11..2026-10
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import yaml

from src import nbm
from src.forecast_refresh import model_probability_for_market
from src.kalshi_client import dollars_to_cents
from src.list_kalshi_series import extract_cli_code, icao_from_cli
from src.market_parsing import extract_threshold

logger = logging.getLogger("backtest")

AWS_BASE = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
DEFAULT_DB = "data/backtest.db"
DEFAULT_DBS = {"high": "data/backtest.db", "low": "data/backtest_low.db"}
KINDS = ("high", "low")
RUN_HOUR = 13            # NBM run used for HIGHS (published ~14-15Z)
DECISION_HOUR = 19       # UTC hour the simulated HIGHS decision is made, on the run date

# LOWS timing (revised 2026-11-21 after the first lows run): at 19Z the day before,
# 712 of 720 low markets had no trading in the previous 24h and showed placeholder
# quotes (1c bid / 99c ask), so that is not a real market price. Lows now use the
# 19Z NBM run of the previous day (published ~20-21Z) and take quotes at fixed
# hours relative to MIDNIGHT LOCAL STANDARD TIME on the market date (the start of
# Kalshi's climate day). PRIMARY = 9 PM the evening before (-3h): the latest point
# at which nothing of the climate day has been observed yet, so the market and the
# model are both still forecasting. The other hours are reported for context only;
# +1h and +3h can include early-morning observations the model doesn't have.
LOW_RUN_HOUR = 19
LOW_QUOTE_OFFSETS_H = (-6, -3, 1, 3)
LOW_PRIMARY_OFFSET_H = -3
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
CREATE TABLE IF NOT EXISTS bt_quotes (
    ticker TEXT NOT NULL,               -- lows only: the quote at several hours around the climate day start
    offset_h INTEGER NOT NULL,          -- hours from midnight local standard time on the market date
    yes_ask_cents INTEGER,
    yes_bid_cents INTEGER,
    quote_ts INTEGER,
    volume_24h INTEGER,
    PRIMARY KEY (ticker, offset_h)
);
CREATE TABLE IF NOT EXISTS bt_meta (
    key TEXT PRIMARY KEY,               -- 'kind' = high | low
    value TEXT
);
CREATE TABLE IF NOT EXISTS bt_skipped (
    ticker TEXT PRIMARY KEY,            -- markets Kalshi returned that were NOT scored, and why
    city TEXT NOT NULL,
    market_date TEXT NOT NULL,
    reason TEXT NOT NULL,
    result TEXT,
    status TEXT,
    strike_type TEXT,
    floor_strike TEXT,
    cap_strike TEXT,
    raw_json TEXT                       -- the market as Kalshi returned it (trimmed)
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


def low_prefix_candidates(city: dict) -> list[str]:
    """Kalshi series names to try for a city's daily-LOW markets. An explicit
    `kalshi_low_series_prefix` in config.yaml comes first; then names derived
    from the daily-high prefix (KXHIGHNY -> KXLOWTNY, KXLOWTNYC, KXLOWNY ...).
    Guesses are safe: every market's settlement station is checked before use."""
    out = []
    if city.get("kalshi_low_series_prefix"):
        out.append(city["kalshi_low_series_prefix"])
    hp = city["kalshi_series_prefix"].upper()
    code = hp[len("KXHIGHT"):] if hp.startswith("KXHIGHT") else (
        hp[len("KXHIGH"):] if hp.startswith("KXHIGH") else hp)
    codes = [code] + (["NYC"] if code == "NY" else [])
    for c in codes:
        out += [f"KXLOWT{c}", f"KXLOW{c}"]
    seen, uniq = set(), []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def series_candidates(city: dict, kind: str) -> list[str]:
    return prefix_candidates(city["kalshi_series_prefix"]) if kind == "high" else low_prefix_candidates(city)


def settlement_station(market: dict) -> str | None:
    """The ICAO station a market settles on, read from Kalshi's own rules text
    (e.g. 'Data for CLINYC ...' -> KNYC). None if the rules don't name one."""
    text = " ".join(str(market.get(k, "") or "") for k in
                    ("rules_primary", "rules_secondary", "settlement_sources"))
    return icao_from_cli(extract_cli_code(text))


def get_min_temp_forecast(parsed: dict, run_id: str, market_date) -> dict | None:
    """The percentile forecast for the overnight LOW of `market_date`: the TXN
    column valid at 12Z ON the market date (NBM MinT window 00Z-18Z). Mirrors
    nbm.get_max_temp_forecast, which uses 00Z the day after for the high."""
    hours = parsed.get("forecast_hours")
    if not hours or market_date is None:
        return None
    target = datetime(market_date.year, market_date.month, market_date.day, 12, tzinfo=timezone.utc)
    for idx, fhr in enumerate(hours):
        if nbm.valid_time_utc(run_id, fhr) != target:
            continue
        out = {}
        for key in ("p10", "p25", "p50", "p75", "p90"):
            values = parsed.get(key)
            if not values or idx >= len(values) or values[idx] is None:
                return None
            out[key] = values[idx]
        return {"values": out, "column_index": idx, "forecast_hour": fhr}
    return None


def climate_day_start_utc(market_date, tz_name: str) -> datetime:
    """Midnight LOCAL STANDARD TIME on market_date, in UTC - the start of the NWS
    climate day Kalshi settles on (standard time all year, even during DST).
    New York -> 05Z, Chicago -> 06Z, Denver/Phoenix -> 07Z, Los Angeles -> 08Z."""
    std_offset = ZoneInfo(tz_name).utcoffset(datetime(market_date.year, 1, 15)) or timedelta(0)
    # January is standard time everywhere in the US; Phoenix has no DST at all.
    local_midnight = datetime(market_date.year, market_date.month, market_date.day)
    return (local_midnight - std_offset).replace(tzinfo=timezone.utc)


def decision_times(kind: str, run_date, market_date, tz_name: str | None,
                   decision_hour: int = DECISION_HOUR) -> dict:
    """{offset_h or 'primary': unix ts}. Highs: one decision at 19Z on the run date.
    Lows: one per LOW_QUOTE_OFFSETS_H around the climate-day start; 'primary' is -3h."""
    if kind == "low":
        start = climate_day_start_utc(market_date, tz_name or "America/New_York")
        times = {o: int((start + timedelta(hours=o)).timestamp()) for o in LOW_QUOTE_OFFSETS_H}
        times["primary"] = times[LOW_PRIMARY_OFFSET_H]
        return times
    ts = int(datetime(run_date.year, run_date.month, run_date.day, decision_hour,
                      tzinfo=timezone.utc).timestamp())
    return {"primary": ts}


def forecast_for(kind: str, parsed: dict, run_id: str, market_date):
    if kind == "low":
        return get_min_temp_forecast(parsed, run_id, market_date)
    return nbm.get_max_temp_forecast(parsed, run_id, market_date)


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

def open_db(path: str, kind: str = "high") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT value FROM bt_meta WHERE key='kind'").fetchone()
    if row is None:
        conn.execute("INSERT INTO bt_meta (key, value) VALUES ('kind', ?)", (kind,))
        conn.commit()
    elif row[0] != kind:
        raise ValueError(f"{path} holds {row[0]} data; refusing to add {kind} data to it")
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
                     prefix_hint: dict | None = None, kind: str = "high") -> str:
    name = city["name"]
    if parsed is None:
        _mark(conn, name, market_date, "no_forecast", "station not parsed", run_id=run_id,
              version=version)
        return "no_forecast"
    chosen = forecast_for(kind, parsed, run_id, market_date)
    if chosen is None:
        _mark(conn, name, market_date, "no_forecast", f"no daily-{kind} column", run_id=run_id,
              version=version)
        return "no_forecast"
    pct = chosen["values"]

    prefixes = series_candidates(city, kind)
    hint = (prefix_hint or {}).get(name)
    if hint in prefixes:
        prefixes = [hint] + [p for p in prefixes if p != hint]
    markets, prefix, wrong, verified = [], None, [], False
    for p in prefixes:
        found = src.markets(event_ticker(p, market_date))
        if not found:
            continue
        # The series must settle on the SAME station as the city (e.g. never pair a
        # Midway high with an O'Hare low). Checked from Kalshi's own rules text.
        stations = {settlement_station(m) for m in found} - {None}
        if stations and stations != {city["expected_station"]}:
            wrong.append(f"{p}->{','.join(sorted(stations))}")
            continue
        markets, prefix, verified = found, p, bool(stations)
        break
    if not markets:
        if wrong:
            _mark(conn, name, market_date, "wrong_station", "; ".join(wrong), run_id=run_id,
                  version=version)
            return "wrong_station"
        _mark(conn, name, market_date, "no_markets", "tried " + ",".join(prefixes),
              run_id=run_id, version=version)
        return "no_markets"
    if prefix_hint is not None:
        prefix_hint[name] = prefix

    times = decision_times(kind, run_date, market_date, city.get("timezone"), decision_hour)
    decision_ts = times["primary"]
    start_ts = min(times.values()) - VOLUME_WINDOW_H * 3600
    end_ts = max(times.values())
    n = 0
    now = datetime.now(timezone.utc).isoformat()
    for m in markets:
        ticker = m.get("ticker")
        result = m.get("result")
        threshold = extract_threshold(m)
        reason = None
        if not ticker:
            reason = "no ticker"
        elif threshold is None:
            reason = "unreadable strike"
        elif result not in ("yes", "no"):
            reason = f"result={result!r}"
        if reason is None:
            try:
                probs = model_probability_for_market(threshold, pct, sigma)
            except (ValueError, RuntimeError) as e:
                reason = f"probability failed: {e}"
        if reason is not None:
            # Never drop a market silently: a skipped WINNER makes a whole day look
            # like "everything lost" (found in Run 1: Jan 16 - Feb 9 2025).
            logger.warning("skipped %s: %s", ticker, reason)
            conn.execute(
                "INSERT OR REPLACE INTO bt_skipped (ticker, city, market_date, reason, result,"
                " status, strike_type, floor_strike, cap_strike, raw_json)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (ticker or f"?{name}{market_date}", name, market_date.isoformat(), reason[:200],
                 str(result), str(m.get("status")), str(m.get("strike_type")),
                 str(m.get("floor_strike")), str(m.get("cap_strike")), json.dumps(m)[:2000]))
            continue
        candles = src.candles(prefix, ticker, start_ts, end_ts)
        q = quote_at_decision(candles, decision_ts)
        for off, ts in times.items():
            if off == "primary":
                continue
            qo = quote_at_decision(candles, ts)
            conn.execute(
                "INSERT OR REPLACE INTO bt_quotes (ticker, offset_h, yes_ask_cents, yes_bid_cents,"
                " quote_ts, volume_24h) VALUES (?,?,?,?,?,?)",
                (ticker, off, qo["yes_ask"] if qo else None, qo["yes_bid"] if qo else None,
                 qo["ts"] if qo else None, volume_before(candles, ts)))
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
    _mark(conn, name, market_date, "ok", "" if verified else "station not named in rules",
          prefix, run_id, version, n)
    return "ok"


def collect_month(conn, src: Sources, cities: list[tuple[dict, bool]], year: int, month: int,
                  sigma: float, run_hour: int | None = None, decision_hour: int = DECISION_HOUR,
                  redo: bool = False, kind: str = "high") -> dict:
    if run_hour is None:
        run_hour = LOW_RUN_HOUR if kind == "low" else RUN_HOUR
    counts = {"ok": 0, "no_markets": 0, "no_forecast": 0, "error": 0, "skipped": 0,
              "no_bulletin": 0, "wrong_station": 0}
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
                                          decision_hour, prefix_hint, kind)
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
    ap.add_argument("--kind", choices=KINDS, default="high", help="daily high or daily low markets")
    ap.add_argument("--db", default=None, help="defaults to data/backtest.db (high) or data/backtest_low.db (low)")
    ap.add_argument("--redo", action="store_true", help="re-collect city-days already stored")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from src.scan_common import build_client
    cities, sigma = load_cities()
    src = Sources(build_client())
    conn = open_db(args.db or DEFAULT_DBS[args.kind], args.kind)
    for y, m in parse_months(args.months):
        started = time.time()
        counts = collect_month(conn, src, cities, y, m, sigma, redo=args.redo, kind=args.kind)
        print(f"{y}-{m:02d}: {counts}  ({time.time() - started:.0f}s)", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
