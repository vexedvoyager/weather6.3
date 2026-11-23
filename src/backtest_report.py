"""
Backtest report: reads data/backtest.db (written by src/backtest.py) and prints
the same measures used to judge the live bot, over the archived period.

Ground rules (fixed BEFORE any results were seen - see claude/backtest-plan.md):
  * The trading rules simulated are the v6.3 rules exactly as price_check.py
    applies them, plus the v6.4 candidates (no YES on above/below markets; no
    Austin). Nothing is tuned on this data.
  * Market dates from HOLDOUT_FROM (2026-08-01) on are a HOLDOUT, fixed in
    advance so it can't drift as more months are collected; reported last and
    separately. Read the development period first, form a view, then check it
    against the holdout.
  * One contract per bet, estimated fees, buying at the ask (as paper mode does).
    The live bot's composite-score filter (order-book depth/imbalance) is NOT
    simulated - historical candles carry no depth - so the simulation trades a
    little more often than the live bot would.

Works for daily-high (data/backtest.db) and daily-low (data/backtest_low.db)
databases; the kind is read from the database itself.

Usage: python -m src.backtest_report [--kind high|low] [--db PATH] [--holdout-from 2026-08-01]
"""
import argparse
import json
import math
import os
import sqlite3
import statistics as st
import sys
from collections import defaultdict
from datetime import datetime, timezone

import yaml

from src.fees import kalshi_fee_cents

HOLDOUT_FROM = "2026-08-01"   # Aug-Oct 2026: the 3 months before live v6.1 data begins

SEASONS = {12: "DJF", 1: "DJF", 2: "DJF", 3: "MAM", 4: "MAM", 5: "MAM",
           6: "JJA", 7: "JJA", 8: "JJA", 9: "SON", 10: "SON", 11: "SON"}


# --- simulation (unit-tested) ------------------------------------------------

def simulate_bet(r: dict, min_gap: float, min_yes_cents: int, min_volume: int,
                 block_yes_threshold: bool = False, fee_rate: float = 0.07) -> dict | None:
    """Would the bot have bought this market at the decision hour, and what
    did one contract make? Mirrors price_check.py: the gap is model P(YES)
    minus the YES ask; positive -> buy YES at the ask, negative -> buy NO at
    100 - YES bid. Returns None when no trade."""
    ask, bid = r.get("yes_ask_cents"), r.get("yes_bid_cents")
    if ask is None or r.get("outcome") not in ("yes", "no"):
        return None
    if (r.get("volume_24h") or 0) < min_volume:
        return None
    gap = r["model_prob"] - ask / 100.0
    if abs(gap) < min_gap:
        return None
    side = "yes" if gap > 0 else "no"
    if side == "yes":
        price = ask
        if price < min_yes_cents:
            return None
        if block_yes_threshold and r.get("kind") == "threshold":
            return None
    else:
        price = 100 - (bid or 0)
        if price >= 100 or price <= 0:
            return None
    fee = kalshi_fee_cents(1, price, fee_rate)
    won = r["outcome"] == side
    return {"side": side, "price": price, "fee": fee, "won": won,
            "pnl": (100 if won else 0) - price - fee, "gap": abs(gap)}


def complete_days_only(rows: list[dict]) -> tuple[list[dict], list[tuple[str, str]]]:
    """Keep only city-days with exactly one winning market. Every day's ladder of
    markets has exactly one winner; if it's missing (Run 1: Jan 16 - Feb 9 2025,
    all cities), every remaining market lost and the day scores as a free NO win."""
    by_day = defaultdict(list)
    for r in rows:
        by_day[(r["city"], r["market_date"])].append(r)
    keep, bad = [], []
    for k, rs in by_day.items():
        if sum(r["outcome"] == "yes" for r in rs) == 1:
            keep += rs
        else:
            bad.append(k)
    keep.sort(key=lambda r: (r["market_date"], r["ticker"]))
    return keep, sorted(bad)


MAX_TRADEABLE_SPREAD = 20   # cents; fixed 2026-11-21 before any lows results at real hours


def is_tradeable(r: dict) -> bool:
    """A real two-sided quote: someone bidding at least 1c and asking at most 20c more.
    Placeholder books (e.g. 1c bid / 99c ask on a market nobody trades) fail this."""
    ask, bid = r.get("yes_ask_cents"), r.get("yes_bid_cents")
    return (ask is not None and bid is not None and bid >= 1 and ask <= 99
            and ask - bid <= MAX_TRADEABLE_SPREAD)


def blend_scores(rows: list[dict], weights=(0.0, 0.1, 0.2, 0.3, 0.5, 1.0)) -> dict:
    """Brier of w*model + (1-w)*market midpoint, tradeable quotes only. If no w > 0
    beats w = 0, the market already contains everything the model knows."""
    q = [r for r in rows if is_tradeable(r)]
    if not q:
        return {}
    o = lambda r: 1.0 if r["outcome"] == "yes" else 0.0
    mid = lambda r: (r["yes_ask_cents"] + r["yes_bid_cents"]) / 200
    return {w: sum((w * r["model_prob"] + (1 - w) * mid(r) - o(r)) ** 2 for r in q) / len(q)
            for w in weights}


def blend_line(out, rows, label):
    b = blend_scores(rows)
    if not b:
        out(f"  {label:12s} (no tradeable quotes)")
        return
    best = min(b, key=b.get)
    out(f"  {label:12s} " + "  ".join(f"w={w:.1f}: {v:.4f}" for w, v in b.items())
        + f"   -> best w = {best:.1f}" + ("  (market alone)" if best == 0 else ""))


def brier(pairs) -> float | None:
    pairs = list(pairs)
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs) if pairs else None


def actual_high(rows: list[dict]) -> tuple[float | None, bool]:
    """Best estimate of a city-day's actual high from its settled markets.
    Returns (value, censored). Uses Kalshi's expiration_value when recorded;
    otherwise the winning bracket's midpoint; otherwise the edge of a winning
    above/below market (censored=True: the real value is at least that far out)."""
    for r in rows:
        try:
            if r.get("expiration_value"):
                return float(r["expiration_value"]), False
        except ValueError:
            pass
    for r in rows:
        if r["outcome"] == "yes" and r["kind"] == "bracket" and r["floor_strike"] is not None:
            return (r["floor_strike"] + r["cap_strike"]) / 2.0, False
    for r in rows:
        if r["outcome"] == "yes" and r["kind"] == "threshold":
            if r["strike_type"] in ("greater", "greater_or_equal") and r["floor_strike"] is not None:
                return r["floor_strike"] + 1, True
            if r["strike_type"] in ("less", "less_or_equal") and r["cap_strike"] is not None:
                return r["cap_strike"] - 1, True
    return None, False


# --- report --------------------------------------------------------------------

class Report:
    def __init__(self):
        self.lines = []

    def __call__(self, s=""):
        self.lines.append(s)

    def text(self):
        return "\n".join(self.lines) + "\n"


def _fmt(x, nd=3):
    return "  -  " if x is None else f"{x:.{nd}f}"


def _z(bets):
    sd = math.sqrt(sum(b["price"] * (100 - b["price"]) for b in bets)) if bets else 0
    gross = sum(b["pnl"] + b["fee"] for b in bets)
    return gross / sd if sd else 0.0


def scorecard(out, rows, label_fn, title, order=None):
    groups = defaultdict(list)
    for r in rows:
        groups[label_fn(r)].append(r)
    out(f"\n{title}")
    out(f"  {'':24s} {'markets':>7s} {'days':>5s}  {'model':>6s} {'mkt ask':>7s} {'mkt mid':>7s}"
        f"  {'winner':8s} {'day t':>6s}")
    keys = order or sorted(groups)
    for k in keys:
        rs = groups.get(k, [])
        q = [r for r in rs if r["yes_ask_cents"] is not None]
        if not q:
            continue
        o = lambda r: 1.0 if r["outcome"] == "yes" else 0.0
        bm = brier((r["model_prob"], o(r)) for r in q)
        ba = brier((r["yes_ask_cents"] / 100, o(r)) for r in q)
        # Midpoint on the SAME markets as the ask (no bid = a 0c bid). Run 1 scored the
        # midpoint only where a bid existed, which made it look worse than the ask.
        bmid = brier(((r["yes_ask_cents"] + (r["yes_bid_cents"] or 0)) / 200, o(r)) for r in q)
        # Day-level paired difference (market minus model): t > 2 means the model is reliably better.
        by_day = defaultdict(list)
        for r in q:
            by_day[(r["city"], r["market_date"])].append(r)
        diffs = [brier((r["yes_ask_cents"] / 100, o(r)) for r in d) -
                 brier((r["model_prob"], o(r)) for r in d) for d in by_day.values()]
        t = (st.mean(diffs) / (st.stdev(diffs) / math.sqrt(len(diffs)))) \
            if len(diffs) > 2 and st.stdev(diffs) > 0 else 0.0
        out(f"  {str(k):24s} {len(q):7d} {len(by_day):5d}  {_fmt(bm)} {_fmt(ba):>7s} {_fmt(bmid):>7s}"
            f"  {'MODEL' if bm < ba else 'market':8s} {t:+6.1f}")


def sim_table(out, rows, label_fn, title, rules, order=None):
    groups = defaultdict(list)
    for r in rows:
        b = simulate_bet(r, **rules)
        if b:
            b.update(city=r["city"], kind=r["kind"])
            groups[label_fn(r, b)].append(b)
    out(f"\n{title}")
    out(f"  {'':22s} {'bets':>5s} {'won':>5s} {'mkt exp':>7s} {'P&L':>8s} {'per bet':>8s} {'z':>5s}")
    total = []
    for k in (order or sorted(groups)):
        bs = groups.get(k, [])
        if not bs:
            continue
        total += bs
        pnl = sum(b["pnl"] for b in bs)
        out(f"  {str(k):22s} {len(bs):5d} {sum(b['won'] for b in bs):5d} "
            f"{sum(b['price'] for b in bs) / 100:7.1f} {pnl / 100:+8.2f} {pnl / len(bs):+7.1f}c "
            f"{_z(bs):+5.1f}")
    if total:
        pnl = sum(b["pnl"] for b in total)
        out(f"  {'TOTAL':22s} {len(total):5d} {sum(b['won'] for b in total):5d} "
            f"{sum(b['price'] for b in total) / 100:7.1f} {pnl / 100:+8.2f} "
            f"{pnl / len(total):+7.1f}c {_z(total):+5.1f}")
    return total


def build_report(db_path: str, config_path: str = "config.yaml", holdout_from: str = HOLDOUT_FROM) -> str:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    g = cfg.get("guards") or {}
    base_rules = dict(min_gap=float(g.get("min_probability_gap", 0.15)),
                      min_yes_cents=int(g.get("min_yes_price_cents", 11)),
                      min_volume=int(cfg["operations"]["min_volume_contracts"]),
                      fee_rate=float((cfg.get("fees") or {}).get("taker_rate", 0.07)))
    v64_rules = dict(base_rules, block_yes_threshold=True)
    traded = [c["name"] for c in cfg["cities"]]
    shadow = [c["name"] for c in (cfg.get("shadow_cities") or [])]
    city_order = traded + shadow

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    all_rows = [dict(r) for r in conn.execute("SELECT * FROM bt_markets ORDER BY market_date, ticker")]
    days = [dict(r) for r in conn.execute("SELECT * FROM bt_city_days")]
    rows, incomplete = complete_days_only(all_rows)
    try:
        skipped = [dict(r) for r in conn.execute("SELECT * FROM bt_skipped")]
    except sqlite3.OperationalError:      # databases from before the table existed
        skipped = []
    try:
        quotes = [dict(r) for r in conn.execute("SELECT * FROM bt_quotes")]
    except sqlite3.OperationalError:
        quotes = []
    out = Report()
    try:
        kind = (conn.execute("SELECT value FROM bt_meta WHERE key='kind'").fetchone() or ["high"])[0]
    except sqlite3.OperationalError:     # databases from before bt_meta existed are highs
        kind = "high"
    out(f"=== Weather Bot backtest report — DAILY {kind.upper()}S — "
        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%MZ} ===")
    if kind == "low":
        out("Forecast = NBM overnight-minimum column (valid 12Z on the market date; NBM's MinT window is "
            "00Z-18Z). Kalshi settles on the calendar-day low (midnight-midnight local standard time), "
            "so lows set late in the evening are outside NBM's window.")
        out("Decision (fixed 2026-11-21): 19Z NBM run of the previous day; market quote at 9 PM local "
            "standard time the evening before the market date (before any of the climate day is observed).")
    if not rows:
        out("No markets collected yet. Run the 'Run Backtest' workflow first."
            if not all_rows else "No complete city-days yet (see incomplete-day note).")
        return out.text()

    months = sorted({r["market_date"][:7] for r in rows})
    hold = {m for m in months if m >= holdout_from[:7]}
    dev = [r for r in rows if r["market_date"][:7] not in hold]
    ho = [r for r in rows if r["market_date"][:7] in hold]
    out(f"Markets: {len(rows):,} from {months[0]} to {months[-1]}  |  "
        f"development: {len(dev):,}  |  HOLDOUT ({', '.join(sorted(hold)) or 'none yet'}): {len(ho):,}")
    out("Rules simulated: v6.3 = gap >= %.0f pts vs YES ask, YES ask >= %dc, 24h volume >= %d; "
        "v6.4 = v6.3 + no YES on above/below markets."
        % (base_rules["min_gap"] * 100, base_rules["min_yes_cents"], base_rules["min_volume"]))
    out("Not simulated: the composite-score (order-book) filter. Brier: lower is better. "
        "'day t' > +2 means the model beat the market reliably; < -2 the reverse.")

    # 1. Coverage
    out("\n1. COVERAGE (city-days)")
    out(f"  {'city':14s} {'ok':>4s} {'no mkts':>7s} {'no fcst':>7s} {'wrong stn':>9s} {'error':>5s}  "
        f"{'first ok':10s} {'last ok':10s} {'quoted':>6s}  series")
    for c in city_order:
        ds = [d for d in days if d["city"] == c]
        ok = sorted(d["market_date"] for d in ds if d["status"] == "ok")
        cr = [r for r in rows if r["city"] == c]
        quoted = sum(r["yes_ask_cents"] is not None for r in cr)
        pref = sorted({d["series_prefix"] for d in ds if d["series_prefix"]})
        out(f"  {c + (' *' if c in shadow else ''):14s} {len(ok):4d} "
            f"{sum(d['status'] == 'no_markets' for d in ds):7d} "
            f"{sum(d['status'] == 'no_forecast' for d in ds):7d} "
            f"{sum(d['status'] == 'wrong_station' for d in ds):9d} "
            f"{sum(d['status'] == 'error' for d in ds):5d}  "
            f"{(ok[0] if ok else '-'):10s} {(ok[-1] if ok else '-'):10s} "
            f"{(100 * quoted / len(cr) if cr else 0):5.0f}%  {','.join(pref)}")
    out("  * = shadow city (never traded live). 'quoted' = markets with a YES ask at the decision hour.")
    out("  'wrong stn' = Kalshi's rules named a different settlement station than the city's; "
        "those days are never scored.")
    unverified = sum(1 for d in days if d["status"] == "ok" and "not named" in (d["detail"] or ""))
    if unverified:
        out(f"  {unverified} scored city-days had rules that didn't name a station (not verified).")
    wrong = [d for d in days if d["status"] == "wrong_station"]
    if wrong:
        out(f"    e.g. {wrong[0]['city']} {wrong[0]['market_date']}: {wrong[0]['detail'][:120]}")
    versions = defaultdict(list)
    for d in days:
        if d["nbm_version"]:
            versions[d["nbm_version"]].append(d["market_date"])
    for v, ds in sorted(versions.items()):
        out(f"  NBM V{v}: market dates {min(ds)} to {max(ds)}")
    if incomplete:
        dates = sorted(d for _, d in incomplete)
        out(f"  EXCLUDED: {len(incomplete)} city-days without exactly one winning market "
            f"({dates[0]} to {dates[-1]}). A missing winner would make every bet look like a "
            "NO win, so these days are left out of everything below.")
    if skipped:
        reasons = defaultdict(int)
        for k in skipped:
            reasons[k["reason"].split(":")[0]] += 1
        out(f"  Markets Kalshi returned but not scored: {len(skipped)} - "
            + ", ".join(f"{r} x{n}" for r, n in sorted(reasons.items(), key=lambda x: -x[1])[:5]))
        out(f"    example: {skipped[0]['ticker']} {skipped[0]['raw_json'][:300]}")
    errs = [d for d in days if d["status"] == "error"]
    if errs:
        out(f"  {len(errs)} city-days hit errors (re-running the same months retries them); "
            f"first: {errs[0]['city']} {errs[0]['market_date']} {errs[0]['detail'][:120]}")

    # 1b. Quote quality
    quoted = [r for r in rows if r["yes_ask_cents"] is not None]
    two_sided = [r for r in quoted if (r["yes_bid_cents"] or 0) >= 1]
    spreads = sorted(r["yes_ask_cents"] - r["yes_bid_cents"] for r in two_sided)
    out("\n1b. QUOTE QUALITY (at the decision time)")
    out(f"  quoted {len(quoted):,} of {len(rows):,} markets; two-sided (bid >= 1c) "
        f"{100 * len(two_sided) / max(len(quoted), 1):.0f}%; median spread "
        f"{spreads[len(spreads) // 2] if spreads else '-'}c; tradeable (two-sided, spread <= "
        f"{MAX_TRADEABLE_SPREAD}c) {100 * sum(map(is_tradeable, quoted)) / max(len(quoted), 1):.0f}%; "
        f"24h volume >= 10: {100 * sum((r['volume_24h'] or 0) >= 10 for r in quoted) / max(len(quoted), 1):.0f}%")
    if quoted and sum(map(is_tradeable, quoted)) < 0.5 * len(quoted):
        out("  WARNING: most quotes are not tradeable, so 'market' scores below mostly measure empty "
            "order books, not traders' estimates. Use the 'tradeable quotes only' lines.")

    # 2. Scorecards (development period)
    out("\n2. MODEL VS MARKET — DEVELOPMENT PERIOD")
    scorecard(out, dev, lambda r: r["city"], "By city:", city_order)
    scorecard(out, [r for r in dev if r["city"] != "Seattle"], lambda r: "ALL (excl Seattle)", "Overall:")
    scorecard(out, dev, lambda r: f"V{r['nbm_version']}", "By NBM version:")
    scorecard(out, dev, lambda r: SEASONS[int(r["market_date"][5:7])], "By season:",
              ["DJF", "MAM", "JJA", "SON"])
    scorecard(out, dev, lambda r: r["market_date"][:4], "By year:")
    scorecard(out, [r for r in dev if is_tradeable(r)], lambda r: r["city"],
              f"By city, TRADEABLE QUOTES ONLY (bid >= 1c, spread <= {MAX_TRADEABLE_SPREAD}c):", city_order)
    scorecard(out, [r for r in dev if is_tradeable(r) and r["city"] != "Seattle"],
              lambda r: "ALL (excl Seattle)", "Overall, TRADEABLE QUOTES ONLY:")
    out("\n2c. BLEND TEST — does the model add anything to the market? (tradeable quotes, excl Seattle)")
    out("  Brier of w x model + (1 - w) x market midpoint; lower is better.")
    blend_line(out, [r for r in dev if r["city"] != "Seattle"], "development")

    if kind == "low" and quotes:
        out("\n2d. LOWS BY QUOTE HOUR (development; tradeable quotes only)")
        out("  Hours are relative to midnight local standard time on the market date. 9 PM is the "
            "decision; 1 AM and 3 AM can include early-morning readings the model doesn't have.")
        labels = {-6: "6 PM before", -3: "9 PM before", 1: "1 AM", 3: "3 AM"}
        by_t = defaultdict(dict)
        for qrow in quotes:
            by_t[qrow["ticker"]][qrow["offset_h"]] = qrow
        dev_by_ticker = {r["ticker"]: r for r in dev}
        out(f"  {'hour':12s} {'quoted':>7s} {'tradeable':>9s} {'med spread':>10s} {'vol>=10':>7s}"
            f"  {'model':>6s} {'mkt mid':>7s}  {'day t':>6s}")
        for off in sorted({q["offset_h"] for q in quotes}):
            rs = []
            for t, r in dev_by_ticker.items():
                qq = by_t.get(t, {}).get(off)
                if qq:
                    rs.append(dict(r, yes_ask_cents=qq["yes_ask_cents"], yes_bid_cents=qq["yes_bid_cents"],
                                   volume_24h=qq["volume_24h"]))
            qd = [r for r in rs if r["yes_ask_cents"] is not None]
            tr = [r for r in qd if is_tradeable(r)]
            sp = sorted(r["yes_ask_cents"] - r["yes_bid_cents"] for r in tr)
            if not tr:
                out(f"  {labels.get(off, off):12s} {len(qd):7d} {0:9d}")
                continue
            o = lambda r: 1.0 if r["outcome"] == "yes" else 0.0
            bm = brier((r["model_prob"], o(r)) for r in tr)
            bk = brier(((r["yes_ask_cents"] + r["yes_bid_cents"]) / 200, o(r)) for r in tr)
            days_ = defaultdict(list)
            for r in tr:
                days_[(r["city"], r["market_date"])].append(r)
            diffs = [brier(((r["yes_ask_cents"] + r["yes_bid_cents"]) / 200, o(r)) for r in d) -
                     brier((r["model_prob"], o(r)) for r in d) for d in days_.values()]
            t = (st.mean(diffs) / (st.stdev(diffs) / math.sqrt(len(diffs)))) \
                if len(diffs) > 2 and st.stdev(diffs) > 0 else 0.0
            out(f"  {labels.get(off, off):12s} {len(qd):7d} {len(tr):9d} {sp[len(sp) // 2]:9d}c "
                f"{sum((r['volume_24h'] or 0) >= 10 for r in qd):7d}  {bm:.3f} {bk:7.3f}  {t:+6.1f}")

    # 3. Simulated trading (development period)
    out("\n3. SIMULATED TRADING — DEVELOPMENT PERIOD (one contract per bet, after fees)")
    sim_table(out, [r for r in dev if r["city"] in traded], lambda r, b: r["city"],
              "v6.3 rules, live cities:", base_rules, traded)
    sim_table(out, [r for r in dev if r["city"] in traded and r["city"] != "Austin"],
              lambda r, b: r["city"], "v6.4 rules (no YES on above/below), live cities minus Austin:",
              v64_rules, [c for c in traded if c != "Austin"])
    sim_table(out, [r for r in dev if r["city"] in shadow], lambda r, b: r["city"],
              "v6.3 rules, shadow cities (never traded live):", base_rules, shadow)
    sim_table(out, [r for r in dev if r["city"] in traded],
              lambda r, b: f"{b['side'].upper()} {r['kind']}", "v6.3 rules by bet type (live cities):",
              base_rules)
    sim_table(out, [r for r in dev if r["city"] in traded],
              lambda r, b: "15-25 pts" if b["gap"] < .25 else ("25-40 pts" if b["gap"] < .40 else "40+ pts"),
              "v6.3 rules by edge size (live cities):", base_rules, ["15-25 pts", "25-40 pts", "40+ pts"])
    sim_table(out, [r for r in dev if r["city"] in traded],
              lambda r, b: f"{b['side'].upper()} " + ("<=30c" if b["price"] <= 30 else
                                                     ("31-60c" if b["price"] <= 60 else ">60c")),
              "v6.3 rules by side and price (live cities):", base_rules)
    sim_table(out, [r for r in dev if r["city"] in traded], lambda r, b: r["market_date"][:4],
              "v6.3 rules by year (live cities):", base_rules)

    # 4. Forecast accuracy and bias by month
    out(f"\n4. FORECAST ACCURACY — actual {kind} minus NBM median (°F), monthly average")
    by_cd = defaultdict(list)
    for r in rows:
        by_cd[(r["city"], r["market_date"])].append(r)
    errs_by = defaultdict(list)
    cover, censored = defaultdict(list), 0
    for (c, d), rs in by_cd.items():
        a, cens = actual_high(rs)
        if a is None:
            continue
        lad = json.loads(rs[0]["ladder_json"])
        if cens:
            censored += 1
            continue
        errs_by[(c, d[:7])].append(a - lad["p50"])
        cover[c].append(lad["p10"] - 0.5 <= a <= lad["p90"] + 0.5)
    abbrev = {c: c.replace(" ", "")[:5] for c in city_order}
    out("  " + f"{'month':8s}" + "".join(f"{abbrev[c]:>7s}" for c in city_order))
    for mth in months:
        cells = []
        for c in city_order:
            e = errs_by.get((c, mth))
            cells.append(f"{st.mean(e):+7.1f}" if e else f"{'':>7s}")
        out(f"  {mth:8s}" + "".join(cells) + ("   HOLDOUT" if mth in hold else ""))
    out("  " + f"{'typical':8s}" + "".join(
        f"{st.mean(abs(x) for (cc, _), v in errs_by.items() if cc == c for x in v):7.1f}"
        if any(cc == c for cc, _ in errs_by) else f"{'':>7s}" for c in city_order) + "   (mean absolute miss)")
    out("  " + f"{'in range':8s}" + "".join(
        f"{100 * sum(cover[c]) / len(cover[c]):6.0f}%" if cover[c] else f"{'':>7s}"
        for c in city_order) + "   (actual within P10-P90; target 80%)")
    out(f"  Actual {kind} = Kalshi's recorded value, else the winning bracket's midpoint; "
        f"{censored} city-days settled in an open-ended tail were left out.")
    out("  Look for a sudden lasting jump in one column (e.g. a 2025 sensor replacement).")

    # 5. New York detail
    ny = [r for r in dev if r["city"] == "New York"]
    if ny:
        out("\n5. NEW YORK DETAIL — DEVELOPMENT PERIOD")
        scorecard(out, ny, lambda r: SEASONS[int(r["market_date"][5:7])], "By season:",
                  ["DJF", "MAM", "JJA", "SON"])
        scorecard(out, ny, lambda r: f"V{r['nbm_version']}", "By NBM version:")

        def band(r):
            lad = json.loads(r["ladder_json"])
            if r["kind"] == "bracket":
                centre = (r["floor_strike"] + r["cap_strike"]) / 2
            else:
                centre = r["floor_strike"] if r["floor_strike"] is not None else r["cap_strike"]
            d = abs(centre - lad["p50"])
            return f"{r['kind']} " + ("near (<=2F)" if d <= 2 else ("shoulder (2-5F)" if d <= 5 else "far (>5F)"))
        scorecard(out, ny, band, "By distance of the strike from the forecast median:")
        sim_table(out, ny, lambda r, b: f"{b['side'].upper()} {r['kind']}",
                  "Simulated v6.3 bets by type:", base_rules)

    # 6. Holdout
    out("\n6. HOLDOUT — " + (", ".join(sorted(hold)) if hold else
                           f"none collected yet (market dates from {holdout_from} on)"))
    if ho:
        out("  Read this LAST. If the development-period story holds here too, it's more likely real.")
        scorecard(out, ho, lambda r: r["city"], "By city:", city_order)
        scorecard(out, [r for r in ho if r["city"] != "Seattle"], lambda r: "ALL (excl Seattle)", "Overall:")
        scorecard(out, [r for r in ho if is_tradeable(r) and r["city"] != "Seattle"],
                  lambda r: "ALL (excl Seattle)", "Overall, TRADEABLE QUOTES ONLY:")
        out("\n  Blend test (tradeable quotes, excl Seattle):")
        blend_line(out, [r for r in ho if r["city"] != "Seattle"], "holdout")
        sim_table(out, [r for r in ho if r["city"] in traded], lambda r, b: r["city"],
                  "v6.3 rules, live cities:", base_rules, traded)
        sim_table(out, [r for r in ho if r["city"] in traded and r["city"] != "Austin"],
                  lambda r, b: r["city"], "v6.4 rules, live cities minus Austin:", v64_rules,
                  [c for c in traded if c != "Austin"])
    return out.text()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=("high", "low"), default="high")
    ap.add_argument("--db", default=None, help="defaults to data/backtest.db or data/backtest_low.db")
    ap.add_argument("--holdout-from", default=HOLDOUT_FROM)
    ap.add_argument("--out-dir", default="data/backtest_reports")
    args = ap.parse_args(argv)
    db_path = args.db or {"high": "data/backtest.db", "low": "data/backtest_low.db"}[args.kind]
    if not os.path.exists(db_path):
        print(f"{db_path} not found - run the backtest first.")
        return 0
    text = build_report(db_path, holdout_from=args.holdout_from)
    print(text)
    os.makedirs(args.out_dir, exist_ok=True)
    suffix = "" if args.kind == "high" else "_low"
    path = os.path.join(args.out_dir,
                        f"backtest_report{suffix}_{datetime.now(timezone.utc):%Y-%m-%dT%H%MZ}.txt")
    with open(path, "w") as f:
        f.write(text)
    print(f"(saved to {path})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
