"""
Backtest data-source check (step 1 of the backtest plan, 2026-11-18).

Read-only. Never trades, never touches data/trades.db. Answers three questions
before any backtest is built:

  1. Does NOAA's AWS archive hold the SAME probabilistic bulletin (NBP,
     blend_nbptx) the bot reads live from NOMADS - for dates a year or more ago?
  2. Does the bot's own parser (src/nbm.py) read those archived bulletins
     correctly for every station - including the daily-HIGH column the bot uses?
  3. Does Kalshi still serve settled daily-high markets from those dates,
     with their outcome and an hourly price history (candlesticks)?

Run via Actions -> "Verify Backtest Sources". Upload the output file
(backtest-source-check artifact) to Claude.
"""
import argparse
import json
import sys
import traceback
from datetime import date, datetime, timedelta, timezone

import requests
import yaml

from src import nbm
from src.scan_common import build_client

AWS_BASE = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
RUN_HOUR = 13          # the NBP run most live trades use
CHECK_CITY = "New York"
RESULTS = []           # (label, ok, detail) for the summary at the end


def record(label: str, ok: bool, detail: str = ""):
    RESULTS.append((label, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}{(' - ' + detail) if detail else ''}")


def aws_bulletin_url(run_date: date, run_hour: int | None = None) -> str:
    run_hour = RUN_HOUR if run_hour is None else run_hour
    return (f"{AWS_BASE}/blend.{run_date:%Y%m%d}/{run_hour:02d}/text/"
            f"blend_nbptx.t{run_hour:02d}z")


def run_id_for(run_date: date, run_hour: int | None = None) -> str:
    run_hour = RUN_HOUR if run_hour is None else run_hour
    return f"{run_date:%Y-%m-%d}T{run_hour:02d}Z"


def event_ticker(prefix: str, market_date: date) -> str:
    """KXHIGHNY + 2025-11-16 -> KXHIGHNY-25NOV16 (Kalshi's event ticker format)."""
    return f"{prefix}-{market_date:%y}{market_date.strftime('%b').upper()}{market_date:%d}"


def load_cities() -> list[dict]:
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    return list(cfg["cities"]) + list(cfg.get("shadow_cities") or [])


# --- 1 + 2: NOAA archive and parser ------------------------------------------

def check_nbm(run_date: date, cities: list[dict]):
    market_date = run_date + timedelta(days=1)
    url = aws_bulletin_url(run_date)
    print(f"\n=== NBM archive: run {run_id_for(run_date)} (market date {market_date}) ===")
    print(f"  URL: {url}")
    try:
        resp = requests.get(url, timeout=180)
    except requests.RequestException as e:
        record(f"NBM {run_date} download", False, str(e))
        return
    if resp.status_code != 200:
        record(f"NBM {run_date} download", False, f"HTTP {resp.status_code}")
        return
    text = resp.text
    record(f"NBM {run_date} download", True, f"{len(text):,} characters")

    run_id = run_id_for(run_date)
    ladders, parsed_ok, high_ok = {}, 0, 0
    for c in cities:
        st = c["expected_station"]
        block = nbm.get_station_block(text, st)
        header = block[0].strip() if block else "(station not found)"
        parsed = nbm.parse_station_maxt(text, st) if block else None
        if parsed is None:
            print(f"    {c['name']:12s} {st}: NOT PARSED  header: {header[:70]}")
            continue
        parsed_ok += 1
        chosen = nbm.get_max_temp_forecast(parsed, run_id, market_date)
        if chosen is None:
            print(f"    {c['name']:12s} {st}: parsed, but no daily-HIGH column for {market_date}"
                  f"  hours={parsed['forecast_hours'][:8]}")
            continue
        high_ok += 1
        v = chosen["values"]
        ladders[st] = json.dumps(v, sort_keys=True)
        print(f"    {c['name']:12s} {st}: high P10/P25/P50/P75/P90 = "
              f"{v['p10']}/{v['p25']}/{v['p50']}/{v['p75']}/{v['p90']}F  "
              f"(FHR {chosen['forecast_hour']})  header: {header[:60]}")

    n = len(cities)
    record(f"NBM {run_date} stations parsed", parsed_ok == n, f"{parsed_ok}/{n}")
    record(f"NBM {run_date} daily-high column found", high_ok == n, f"{high_ok}/{n}")
    distinct = len(set(ladders.values()))
    record(f"NBM {run_date} stations differ (no repeat of the Nov 3 bug)",
           len(ladders) < 3 or distinct > 1, f"{distinct} distinct forecasts among {len(ladders)}")


# --- 3: Kalshi settled markets and prices ------------------------------------

def _try(client, path, params=None):
    try:
        return client._request("GET", path, params=params), None
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:150]}"


def _candle_summary(c: dict) -> str:
    """Pull a readable YES bid/ask out of one candlestick, whatever field names Kalshi uses."""
    parts = []
    for key in ("yes_bid", "yes_ask", "price"):
        block = c.get(key) or {}
        close = block.get("close_dollars", block.get("close"))
        if close is not None:
            parts.append(f"{key} close={close}")
    ts = c.get("end_period_ts")
    when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%MZ") if ts else "?"
    return f"{when} " + ", ".join(parts) + f", volume={c.get('volume', c.get('volume_fp', '?'))}"


def check_kalshi(client, prefixes: list[str], market_date: date):
    print(f"\n=== Kalshi: {CHECK_CITY} markets for {market_date} ===")
    markets, used = [], None
    for prefix in prefixes:
        ev = event_ticker(prefix, market_date)
        for path in ("/historical/markets", "/markets"):
            data, err = _try(client, path, {"event_ticker": ev, "limit": 100})
            found = (data or {}).get("markets", [])
            print(f"  {path}?event_ticker={ev}: "
                  f"{err or str(len(found)) + ' market(s)'}")
            if found:
                markets, used = found, (prefix, path)
                break
        if markets:
            break
    record(f"Kalshi {market_date} settled markets found", bool(markets),
           f"via {used[1]} with prefix {used[0]}" if used else "none under any prefix/endpoint")
    if not markets:
        return

    for m in sorted(markets, key=lambda m: m.get("ticker", "")):
        print(f"    {m.get('ticker')}: status={m.get('status')} result={m.get('result')} "
              f"expiration_value={m.get('expiration_value')} strike_type={m.get('strike_type')}")
    has_outcome = all(m.get("result") in ("yes", "no") for m in markets)
    record(f"Kalshi {market_date} outcomes recorded", has_outcome)

    # Hourly prices for one market, from the day before through settlement.
    ticker = sorted(markets, key=lambda m: m.get("ticker", ""))[len(markets) // 2]["ticker"]
    start = int(datetime(market_date.year, market_date.month, market_date.day,
                         tzinfo=timezone.utc).timestamp()) - 2 * 86400
    end = start + 4 * 86400
    params = {"start_ts": start, "end_ts": end, "period_interval": 60}
    prefix, path = used
    candles = []
    for cpath in (f"/historical/markets/{ticker}/candlesticks",
                  f"/series/{prefix}/markets/{ticker}/candlesticks"):
        data, err = _try(client, cpath, params)
        candles = (data or {}).get("candlesticks", [])
        print(f"  {cpath}: {err or str(len(candles)) + ' hourly candle(s)'}")
        if candles:
            break
    record(f"Kalshi {market_date} hourly price history ({ticker})", bool(candles),
           f"{len(candles)} candles" if candles else "")
    if candles:
        print("    raw first candle (field names):", json.dumps(candles[0])[:600])
        # Show the candles around 19Z the day before - when a 13Z-run trade would be placed.
        target = start + 86400 + 19 * 3600
        near = sorted(candles, key=lambda c: abs((c.get("end_period_ts") or 0) - target))[:3]
        for c in sorted(near, key=lambda c: c.get("end_period_ts") or 0):
            print("    near decision time:", _candle_summary(c))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="2025-11-15,2025-05-15,2024-11-15",
                    help="NBM run dates to test (comma-separated). Markets are checked for the day after.")
    args = ap.parse_args()
    run_dates = [date.fromisoformat(d.strip()) for d in args.dates.split(",") if d.strip()]

    cities = load_cities()
    ny = next(c for c in cities if c["name"] == CHECK_CITY)
    prefixes = [ny["kalshi_series_prefix"]]
    if prefixes[0].startswith("KX"):
        prefixes.append(prefixes[0][2:])   # older series may use HIGHNY rather than KXHIGHNY

    print(f"Backtest source check - {datetime.now(timezone.utc):%Y-%m-%d %H:%MZ}")
    print(f"Run dates: {', '.join(map(str, run_dates))}; stations: {len(cities)}")

    client = build_client()
    data, err = _try(client, "/historical/cutoff")
    print(f"\nKalshi historical cutoff: {err or json.dumps(data)}")

    for d in run_dates:
        try:
            check_nbm(d, cities)
        except Exception:
            record(f"NBM {d} check crashed", False, traceback.format_exc(limit=2).strip()[-200:])
        try:
            check_kalshi(client, prefixes, d + timedelta(days=1))
        except Exception:
            record(f"Kalshi {d + timedelta(days=1)} check crashed", False,
                   traceback.format_exc(limit=2).strip()[-200:])

    passed = sum(ok for _, ok, _ in RESULTS)
    print(f"\n=== SUMMARY: {passed}/{len(RESULTS)} checks passed ===")
    for label, ok, detail in RESULTS:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{(' - ' + detail) if detail else ''}")
    return 0  # always succeed so the log/artifact is saved; the summary says what failed


if __name__ == "__main__":
    sys.exit(main())
