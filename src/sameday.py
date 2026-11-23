"""
Same-day study, data collector (2026-11-22; see "claude/backtest-plan.md").

Question: during the market day, once the thermometer has (probably) set the
day's high, does Kalshi's price lag behind? This collects two things for the
days already in data/backtest.db:

  1. Station observations from the Iowa Environmental Mesonet (IEM) METAR
     archive: each report's temperature, its tenths-of-a-degree T-group, and
     the 6-hour maximum/minimum groups (1sTTT / 2sTTT). Per IEM's note on
     wagering (news item 1469), official daily highs are 2-minute averages in
     whole degrees F; the 6-hour max group is the closest public, real-time
     proxy, and routine whole-degree readings can be off by ~1F.
  2. Kalshi hourly candles for every daily-HIGH market across its market day
     (14Z on the market date to 09Z the next day, covering 10 AM - midnight
     local in every US time zone).

Read-only with respect to trading. Writes only data/sameday.db; reads (never
writes) data/backtest.db for the list of markets and their outcomes.

Usage: python -m src.sameday --months 2025-06..2025-12 [--part obs|quotes|both]
"""
import argparse
import csv
import io
import logging
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests

from src.backtest import Sources, load_cities, parse_months

logger = logging.getLogger("sameday")

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
IEM_PAUSE_S = 2.0            # IEM asks for gentle, one-at-a-time requests
DEFAULT_DB = "data/sameday.db"
BACKTEST_DB = "data/backtest.db"
QUOTE_START_H = 14           # candles from 14Z on the market date ...
QUOTE_END_H = 9 + 24         # ... to 09Z the next day

SCHEMA = """
CREATE TABLE IF NOT EXISTS obs (
    station TEXT NOT NULL,           -- ICAO, e.g. KNYC
    valid_ts INTEGER NOT NULL,       -- observation time, unix UTC
    tmpf REAL,                       -- the report's whole-degree temperature (F)
    t_group_c REAL,                  -- T-group temperature, tenths of a degree C
    max6_c REAL,                     -- 6-hour maximum group (1sTTT), deg C
    min6_c REAL,                     -- 6-hour minimum group (2sTTT), deg C
    metar TEXT,
    PRIMARY KEY (station, valid_ts)
);
CREATE TABLE IF NOT EXISTS obs_done (station TEXT, month TEXT, n INTEGER, PRIMARY KEY (station, month));
CREATE TABLE IF NOT EXISTS quotes (
    ticker TEXT NOT NULL,
    end_ts INTEGER NOT NULL,         -- end of the hourly candle, unix UTC
    yes_bid_cents INTEGER,
    yes_ask_cents INTEGER,
    volume INTEGER,
    PRIMARY KEY (ticker, end_ts)
);
CREATE TABLE IF NOT EXISTS quote_done (ticker TEXT PRIMARY KEY, n INTEGER);
"""

# --- METAR parsing (unit-tested) -------------------------------------------------

_T_GROUP = re.compile(r"(?<![A-Z0-9])T([01])(\d{3})(?:[01]\d{3})?(?![A-Z0-9])")
_FIVE = re.compile(r"^([12])([01])(\d{3})$")


def _signed_tenths(sign: str, digits: str) -> float:
    v = int(digits) / 10.0
    return -v if sign == "1" else v


def parse_metar(metar: str) -> dict:
    """{'t_group_c', 'max6_c', 'min6_c'} from a raw METAR (None when absent).
    6-hour groups are only read from the remarks (after 'RMK'), where a
    five-character token 1sTTT / 2sTTT is unambiguous."""
    out = {"t_group_c": None, "max6_c": None, "min6_c": None}
    if not metar:
        return out
    text = metar.upper()
    rmk = text.split(" RMK ", 1)[1] if " RMK " in text else ""
    m = _T_GROUP.search(rmk)
    if m:
        out["t_group_c"] = _signed_tenths(m.group(1), m.group(2))
    for tok in rmk.split():
        g = _FIVE.match(tok.rstrip("="))
        if g:
            key = "max6_c" if g.group(1) == "1" else "min6_c"
            if out[key] is None:
                out[key] = _signed_tenths(g.group(2), g.group(3))
    return out


def c_to_f_whole(c: float) -> int:
    """Degrees C -> whole degrees F, rounding half up (the NWS climate convention)."""
    f = c * 9.0 / 5.0 + 32.0
    return int(f + 0.5) if f >= 0 else -int(-f + 0.5)


def parse_iem_csv(text: str, station: str) -> list[dict]:
    """Rows from IEM asos.py (format=onlycomma, data=tmpf&data=metar)."""
    rows = []
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if not lines or "valid" not in lines[0]:
        return rows
    for r in csv.DictReader(io.StringIO("\n".join(lines))):
        try:
            valid = datetime.strptime(r["valid"].strip(), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        tmpf = r.get("tmpf", "").strip()
        metar = (r.get("metar") or "").strip()
        p = parse_metar(metar)
        rows.append({"station": station, "valid_ts": int(valid.timestamp()),
                     "tmpf": float(tmpf) if tmpf not in ("", "M", "T") else None,
                     "metar": metar, **p})
    return rows


# --- collection -------------------------------------------------------------------

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def fetch_iem_month(http_get, station: str, year: int, month: int) -> list[dict]:
    """One month of METARs (plus a 1-day margin either side for climate-day edges)."""
    start = datetime(year, month, 1, tzinfo=timezone.utc) - timedelta(days=1)
    end = (datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc) + timedelta(days=1))
    params = [("station", station[1:] if station.startswith("K") else station),
              ("data", "tmpf"), ("data", "metar"),
              ("sts", start.strftime("%Y-%m-%dT%H:%MZ")), ("ets", end.strftime("%Y-%m-%dT%H:%MZ")),
              ("tz", "Etc/UTC"), ("format", "onlycomma"), ("latlon", "no"), ("missing", "M"),
              ("report_type", "3"), ("report_type", "4")]
    for attempt in range(5):
        try:
            r = http_get(IEM_URL, params=params, timeout=120)
            if r.status_code == 200:
                rows = parse_iem_csv(r.text, station)
                if rows or attempt >= 1:
                    return rows
                # Retry once without report_type filters in case the server rejects them.
                params = [p for p in params if p[0] != "report_type"]
            elif r.status_code in (429, 503):
                logger.warning("IEM busy (%s) for %s %d-%02d", r.status_code, station, year, month)
            else:
                logger.warning("IEM HTTP %s for %s %d-%02d", r.status_code, station, year, month)
        except requests.RequestException as e:
            logger.warning("IEM request failed for %s: %s", station, e)
        time.sleep(IEM_PAUSE_S * (2 ** attempt))
    raise RuntimeError(f"IEM: could not fetch {station} {year}-{month:02d}")


def collect_obs(conn, http_get, stations: list[str], year: int, month: int, sleep=time.sleep) -> dict:
    label = f"{year}-{month:02d}"
    counts = {"stations": 0, "rows": 0, "skipped": 0, "failed": 0}
    for st in stations:
        if conn.execute("SELECT 1 FROM obs_done WHERE station=? AND month=?", (st, label)).fetchone():
            counts["skipped"] += 1
            continue
        try:
            rows = fetch_iem_month(http_get, st, year, month)
        except RuntimeError as e:
            logger.error("%s", e)
            counts["failed"] += 1
            continue
        conn.executemany(
            "INSERT OR REPLACE INTO obs (station, valid_ts, tmpf, t_group_c, max6_c, min6_c, metar)"
            " VALUES (:station, :valid_ts, :tmpf, :t_group_c, :max6_c, :min6_c, :metar)", rows)
        conn.execute("INSERT OR REPLACE INTO obs_done VALUES (?,?,?)", (st, label, len(rows)))
        conn.commit()
        counts["stations"] += 1
        counts["rows"] += len(rows)
        sleep(IEM_PAUSE_S)
    return counts


def collect_quotes(conn, src: Sources, markets: list[dict]) -> dict:
    counts = {"markets": 0, "candles": 0, "skipped": 0, "errors": 0}
    for i, m in enumerate(markets):
        if conn.execute("SELECT 1 FROM quote_done WHERE ticker=?", (m["ticker"],)).fetchone():
            counts["skipped"] += 1
            continue
        d = date.fromisoformat(m["market_date"])
        base = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        start = int((base + timedelta(hours=QUOTE_START_H)).timestamp())
        end = int((base + timedelta(hours=QUOTE_END_H)).timestamp())
        try:
            candles = src.candles(m["series_prefix"], m["ticker"], start - 3600, end)
        except Exception as e:                     # one bad market never stops the run
            logger.warning("candles failed %s: %s", m["ticker"], e)
            counts["errors"] += 1
            continue
        rows = []
        for c in candles:
            ts = c.get("end_period_ts")
            ask = (c.get("yes_ask") or {}).get("close_dollars", (c.get("yes_ask") or {}).get("close"))
            bid = (c.get("yes_bid") or {}).get("close_dollars", (c.get("yes_bid") or {}).get("close"))
            try:
                vol = int(float(c.get("volume_fp", c.get("volume")) or 0))
            except (TypeError, ValueError):
                vol = 0
            rows.append((m["ticker"], ts, None if bid in (None, "") else round(float(bid) * 100),
                         None if ask in (None, "") else round(float(ask) * 100), vol))
        conn.executemany("INSERT OR REPLACE INTO quotes VALUES (?,?,?,?,?)", rows)
        conn.execute("INSERT OR REPLACE INTO quote_done VALUES (?,?)", (m["ticker"], len(rows)))
        counts["markets"] += 1
        counts["candles"] += len(rows)
        if i % 50 == 0:
            conn.commit()
    conn.commit()
    return counts


def markets_for_months(backtest_db: str, months: list[tuple[int, int]]) -> list[dict]:
    b = sqlite3.connect(backtest_db)
    b.row_factory = sqlite3.Row
    out = []
    for y, m in months:
        lo, hi = f"{y}-{m:02d}-01", f"{y}-{m:02d}-31"
        out += [dict(r) for r in b.execute(
            "SELECT ticker, city, market_date, series_prefix FROM bt_markets "
            "WHERE market_date BETWEEN ? AND ? ORDER BY market_date, ticker", (lo, hi))]
    b.close()
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--part", choices=("obs", "quotes", "both"), default="both")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--backtest-db", default=BACKTEST_DB)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    months = parse_months(args.months)
    conn = open_db(args.db)
    cities, _ = load_cities()
    stations = [c["expected_station"] for c, _ in cities]
    if args.part in ("obs", "both"):
        session = requests.Session()
        session.headers["User-Agent"] = "predict-weather-bot same-day study (one request at a time)"
        for y, m in months:
            print(f"obs {y}-{m:02d}: {collect_obs(conn, session.get, stations, y, m)}", flush=True)
    if args.part in ("quotes", "both"):
        from src.scan_common import build_client
        src = Sources(build_client())
        for y, m in months:
            mk = markets_for_months(args.backtest_db, [(y, m)])
            print(f"quotes {y}-{m:02d}: {len(mk)} markets -> {collect_quotes(conn, src, mk)}", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
