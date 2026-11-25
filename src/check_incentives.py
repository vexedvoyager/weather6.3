"""
Liquidity-reward check, step 1 (added 2026-11-24; see "claude/project-status.md"
in the Claude project).

Question: do Kalshi's weather markets get liquidity rewards often enough, and
big enough, to make a resting-order (market-making) strategy worth studying?

Kalshi publishes every reward program, past and present, at
GET /incentive_programs (market, type, start/end, pool, target size, discount
factor). This script pulls them all, keeps the weather ones, and reports:

  1. All programs by type and status (and how far back the history goes).
  2. Weather LIQUIDITY programs: by series (city), pool sizes, target sizes,
     durations, per-account caps.
  3. Weather liquidity pool per day over the last 60 days (each program's pool
     spread evenly over its own start..end).
  4. For comparison: the 15 series with the most liquidity rewards overall.
  5. Right now: each active weather liquidity program's order-book depth
     versus its target size (how much competition is already resting).
  6. VERDICT by the rule fixed before any data was seen (below).

Decision rule (fixed 2026-11-24, before running):
  Over the last 60 days, total weather liquidity pool per day, averaged:
    >= $50/day AND rewards on >= 30 of the 60 days  -> PASS  (build step 2)
    >= $10/day                                      -> MARGINAL (step 2 optional)
    otherwise                                       -> STOP  (weather version ends)
  Rationale: a small account might earn ~10% of a pool. Below $10/day in total,
  that is under $1/day before any losses from bad fills; Kalshi also doesn't pay
  a program reward under $1.

Read-only: never trades, never touches any database.
Run: Actions -> "Check Liquidity Rewards" -> Run workflow; upload the
"liquidity-rewards-check" artifact to Claude.
"""
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

WINDOW_DAYS = 60
PASS_PER_DAY = 50.0
PASS_MIN_DAYS = 30
MARGINAL_PER_DAY = 10.0
WEATHER_PREFIXES = ("KXHIGH", "KXLOW", "HIGH", "LOW")   # fallback if the series list is unavailable
MAX_DEPTH_CHECKS = 40


# --- pure helpers (unit-tested) -----------------------------------------------

def parse_ts(s) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def series_of(market_ticker: str) -> str:
    """'KXHIGHNY-25FEB08-B70.5' -> 'KXHIGHNY'."""
    return str(market_ticker or "").split("-", 1)[0].upper()


def pool_dollars(p: dict) -> float:
    """period_reward is in centi-cents (1/100 of a cent)."""
    try:
        return int(p.get("period_reward") or 0) / 10000.0
    except (TypeError, ValueError):
        return 0.0


def cap_dollars(p: dict) -> float | None:
    v = p.get("max_reward_per_account")
    try:
        return None if v is None else int(v) / 10000.0
    except (TypeError, ValueError):
        return None


def target_size(p: dict) -> float | None:
    v = p.get("target_size_fp", p.get("target_size"))
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def is_weather(market_ticker: str, weather_series: set | None) -> bool:
    s = series_of(market_ticker)
    if weather_series:
        return s in weather_series
    return s.startswith(WEATHER_PREFIXES)


def kind_of(market_ticker: str) -> str:
    s = series_of(market_ticker)
    if s.startswith(("KXHIGH", "HIGH")):
        return "daily high"
    if s.startswith(("KXLOW", "LOW")):
        return "daily low"
    return "other weather"


def daily_allocation(p: dict) -> dict:
    """{date: dollars}: the program's pool spread evenly over its start..end (UTC)."""
    start, end = parse_ts(p.get("start_date")), parse_ts(p.get("end_date"))
    total = pool_dollars(p)
    if not start or not end or end <= start or total <= 0:
        return {}
    span = (end - start).total_seconds()
    out, t = defaultdict(float), start
    while t < end:
        next_midnight = datetime(t.year, t.month, t.day, tzinfo=timezone.utc) + timedelta(days=1)
        seg_end = min(end, next_midnight)
        out[t.date()] += total * (seg_end - t).total_seconds() / span
        t = seg_end
    return dict(out)


def window_summary(programs: list, now: datetime, days: int = WINDOW_DAYS) -> dict:
    """Total pool per day over [now - days, now) and how many of those days had any."""
    first = (now - timedelta(days=days)).date()
    last = now.date()                        # today excluded: not over yet
    per_day = defaultdict(float)
    for p in programs:
        for d, v in daily_allocation(p).items():
            if first <= d < last:
                per_day[d] += v
    total = sum(per_day.values())
    return {"first": first, "last": last, "days": days, "total": total,
            "avg_per_day": total / days, "days_with_rewards": sum(1 for v in per_day.values() if v > 0),
            "per_day": dict(per_day)}


def verdict(summary: dict) -> str:
    avg, n = summary["avg_per_day"], summary["days_with_rewards"]
    if avg >= PASS_PER_DAY and n >= PASS_MIN_DAYS:
        return "PASS"
    if avg >= MARGINAL_PER_DAY:
        return "MARGINAL"
    return "STOP"


def book_depth(orderbook_fp: dict) -> dict:
    """Resting contracts and best prices from Kalshi's orderbook_fp (bids only, ascending).
    YES ask = 1 - best NO bid."""
    def side(levels):
        out = []
        for lvl in levels or []:
            try:
                out.append((float(lvl[0]), float(lvl[1])))
            except (TypeError, ValueError, IndexError):
                pass
        return out
    yes, no = side(orderbook_fp.get("yes_dollars")), side(orderbook_fp.get("no_dollars"))
    best_yes_bid = max((p for p, _ in yes), default=None)
    best_no_bid = max((p for p, _ in no), default=None)
    return {"yes_contracts": sum(c for _, c in yes), "no_contracts": sum(c for _, c in no),
            "yes_bid": best_yes_bid,
            "yes_ask": None if best_no_bid is None else round(1 - best_no_bid, 4)}


def pct(values: list, q: float):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))]


# --- network ------------------------------------------------------------------

def fetch_all_programs(client, page_size: int = 10000, max_pages: int = 200) -> list:
    out, cursor = [], None
    for _ in range(max_pages):
        params = {"status": "all", "type": "all", "limit": page_size}
        if cursor:
            params["cursor"] = cursor
        data = client._request("GET", "/incentive_programs", params=params)
        out.extend(data.get("incentive_programs") or [])
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return out


def fetch_weather_series(client) -> set:
    try:
        return {str(s.get("ticker", "")).upper() for s in client.get_series_list("Climate and Weather")}
    except Exception as e:                    # noqa: BLE001 - report and fall back
        print(f"  (couldn't read the weather series list: {e}; using ticker prefixes instead)")
        return set()


# --- report -------------------------------------------------------------------

def build_report(programs: list, weather_series: set, now: datetime, client=None) -> tuple[str, str]:
    lines = []
    say = lines.append
    say(f"=== Liquidity-reward check (step 1) — {now:%Y-%m-%d %H:%MZ} ===")
    say(f"Rule (fixed 2026-11-24): last {WINDOW_DAYS} days of weather liquidity pools — "
        f"PASS if >= ${PASS_PER_DAY:.0f}/day on average and rewards on >= {PASS_MIN_DAYS} days; "
        f"MARGINAL if >= ${MARGINAL_PER_DAY:.0f}/day; otherwise STOP.")
    say(f"Weather series recognised: {len(weather_series) if weather_series else 'prefix fallback'}")

    # 1. everything
    say("\n1. ALL PROGRAMS")
    say(f"  total programs: {len(programs):,}")
    starts = [parse_ts(p.get("start_date")) for p in programs]
    starts = [s for s in starts if s]
    if starts:
        say(f"  history from {min(starts):%Y-%m-%d} to {max(starts):%Y-%m-%d}")
    by = defaultdict(lambda: [0, 0.0])
    for p in programs:
        end = parse_ts(p.get("end_date"))
        status = "active/upcoming" if end and end > now else "closed"
        k = (p.get("incentive_type") or "?", status)
        by[k][0] += 1
        by[k][1] += pool_dollars(p)
    say(f"  {'type':24s}{'status':18s}{'programs':>10s}{'pools $':>14s}")
    for (t, s), (n, d) in sorted(by.items()):
        say(f"  {t:24s}{s:18s}{n:>10,}{d:>14,.0f}")

    liq = [p for p in programs if (p.get("incentive_type") or "") == "liquidity"]
    wliq = [p for p in liq if is_weather(p.get("market_ticker"), weather_series)]

    # 2. weather liquidity programs
    say("\n2. WEATHER LIQUIDITY PROGRAMS (all time)")
    say(f"  {len(wliq):,} programs on {len({p.get('market_ticker') for p in wliq}):,} markets "
        f"({len(wliq) / max(1, len(liq)):.1%} of all liquidity programs)")
    if wliq:
        pools = [pool_dollars(p) for p in wliq]
        tgts = [t for t in (target_size(p) for p in wliq) if t is not None]
        hrs = []
        for p in wliq:
            s, e = parse_ts(p.get("start_date")), parse_ts(p.get("end_date"))
            if s and e and e > s:
                hrs.append((e - s).total_seconds() / 3600)
        caps = [c for c in (cap_dollars(p) for p in wliq) if c is not None]
        dfs = [p.get("discount_factor_bps") for p in wliq if p.get("discount_factor_bps") is not None]
        say(f"  pool per program ($):   median {pct(pools, .5):,.2f}   p90 {pct(pools, .9):,.2f}   max {max(pools):,.2f}")
        if tgts:
            say(f"  target size (contracts): median {pct(tgts, .5):,.0f}   min {min(tgts):,.0f}   max {max(tgts):,.0f}")
        if hrs:
            say(f"  duration (hours):        median {pct(hrs, .5):,.1f}   min {min(hrs):,.1f}   max {max(hrs):,.1f}")
        if caps:
            say(f"  per-account cap ($):     median {pct(caps, .5):,.2f} (on {len(caps)} programs)")
        if dfs:
            say(f"  discount factor (bps):   median {pct(dfs, .5)}")
        say(f"\n  {'series':16s}{'kind':14s}{'programs':>10s}{'pools $':>12s}{'first':>12s}{'last':>12s}")
        agg = defaultdict(lambda: [0, 0.0, None, None])
        for p in wliq:
            a = agg[series_of(p.get("market_ticker"))]
            a[0] += 1
            a[1] += pool_dollars(p)
            s = parse_ts(p.get("start_date"))
            if s:
                a[2] = s if a[2] is None or s < a[2] else a[2]
                a[3] = s if a[3] is None or s > a[3] else a[3]
        for s, (n, d, f, l) in sorted(agg.items(), key=lambda kv: -kv[1][1]):
            say(f"  {s:16s}{kind_of(s):14s}{n:>10,}{d:>12,.0f}"
                f"{f.strftime('%Y-%m-%d') if f else '':>12s}{l.strftime('%Y-%m-%d') if l else '':>12s}")

    # 3. last 60 days
    summ = window_summary(wliq, now)
    say(f"\n3. WEATHER LIQUIDITY POOL PER DAY — {summ['first']} to {summ['last'] - timedelta(days=1)}")
    say(f"  total ${summ['total']:,.2f}   average ${summ['avg_per_day']:,.2f}/day   "
        f"days with any reward: {summ['days_with_rewards']} of {summ['days']}")
    if summ["per_day"]:
        weeks = defaultdict(float)
        for d, v in summ["per_day"].items():
            weeks[d - timedelta(days=d.weekday())] += v
        say("  by week (Mon):  " + "  ".join(f"{w:%m-%d} ${v:,.0f}" for w, v in sorted(weeks.items())))

    # 4. comparison
    say(f"\n4. TOP 15 SERIES BY LIQUIDITY REWARDS, LAST {WINDOW_DAYS} DAYS (all categories, for comparison)")
    top = defaultdict(float)
    first = (now - timedelta(days=WINDOW_DAYS)).date()
    for p in liq:
        for d, v in daily_allocation(p).items():
            if first <= d < now.date():
                top[series_of(p.get("market_ticker"))] += v
    for s, v in sorted(top.items(), key=lambda kv: -kv[1])[:15]:
        say(f"  {s:24s}${v:>12,.0f}   (${v / WINDOW_DAYS:,.0f}/day){'   <- weather' if is_weather(s, weather_series) else ''}")

    # 5. live competition
    active = [p for p in wliq if (parse_ts(p.get("start_date")) or now) <= now < (parse_ts(p.get("end_date")) or now)]
    say(f"\n5. ACTIVE WEATHER LIQUIDITY PROGRAMS NOW: {len(active)}")
    if active and client is not None:
        say(f"  {'market':34s}{'pool $':>9s}{'target':>8s}{'YES rest':>10s}{'NO rest':>9s}{'bid':>6s}{'ask':>6s}")
        for p in sorted(active, key=lambda p: -pool_dollars(p))[:MAX_DEPTH_CHECKS]:
            t = p.get("market_ticker")
            try:
                d = book_depth(client.get_orderbook(t))
                bid = "" if d["yes_bid"] is None else f"{d['yes_bid'] * 100:.0f}c"
                ask = "" if d["yes_ask"] is None else f"{d['yes_ask'] * 100:.0f}c"
                tg = target_size(p)
                say(f"  {t:34s}{pool_dollars(p):>9,.2f}{'' if tg is None else f'{tg:,.0f}':>8s}"
                    f"{d['yes_contracts']:>10,.0f}{d['no_contracts']:>9,.0f}{bid:>6s}{ask:>6s}")
            except Exception as e:            # noqa: BLE001
                say(f"  {t:34s}  (order book unavailable: {e})")

    v = verdict(summ)
    say(f"\nVERDICT (by the rule above): {v}")
    say({"PASS": "  Weather markets carry enough liquidity rewards to justify step 2 (the minute recorder).",
         "MARGINAL": "  Some rewards, but small. Step 2 is optional; see section 4 for categories with more.",
         "STOP": "  Too little reward on weather markets. The weather version of the liquidity idea ends here; "
                 "section 4 shows where rewards are concentrated instead."}[v])
    return "\n".join(lines), v


def main(argv=None) -> int:
    from src.scan_common import build_client
    client = build_client()
    now = datetime.now(timezone.utc)
    print("Fetching all incentive programs...", file=sys.stderr)
    programs = fetch_all_programs(client)
    weather = fetch_weather_series(client)
    report, _ = build_report(programs, weather, now, client)
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
