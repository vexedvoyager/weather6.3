"""Backtest collector and report - offline tests with fake NOAA/Kalshi sources,
built on the REAL NBM bulletin fixture (run 2026-10-21T19Z, 5 live stations)."""
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src import backtest as bt
from src.backtest_report import actual_high, build_report, simulate_bet

FIXTURE = Path(__file__).parent / "fixtures" / "nbm_real_2026-10-21T19Z.txt"
RUN_DATE = date(2026, 10, 21)
MARKET_DATE = date(2026, 10, 22)
DECISION_TS = int(datetime(2026, 10, 21, 19, tzinfo=timezone.utc).timestamp())


# --- helpers --------------------------------------------------------------------

def test_parse_months_single_range_and_year_wrap():
    assert bt.parse_months("2025-03") == [(2025, 3)]
    assert bt.parse_months("2024-11..2025-02") == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]
    assert bt.parse_months("2025-01, 2025-03") == [(2025, 1), (2025, 3)]
    with pytest.raises(ValueError):
        bt.parse_months("2025-05..2025-01")


def test_prefix_candidates_cover_older_series_names():
    assert bt.prefix_candidates("KXHIGHNY") == ["KXHIGHNY", "HIGHNY"]
    assert bt.prefix_candidates("KXHIGHTPHX") == ["KXHIGHTPHX", "KXHIGHPHX", "HIGHTPHX", "HIGHPHX"]


def test_event_ticker_and_urls():
    assert bt.event_ticker("KXHIGHNY", date(2025, 11, 16)) == "KXHIGHNY-25NOV16"
    assert bt.aws_bulletin_url(date(2025, 1, 2)).endswith("blend.20250102/13/text/blend_nbptx.t13z")
    assert bt.run_id_for(date(2025, 1, 2)) == "2025-01-02T13Z"


def test_bulletin_version_from_station_header():
    assert bt.bulletin_version(" KNYC    NBM V4.3 NBP GUIDANCE   11/15/2025  1300 UTC") == "4.3"
    assert bt.bulletin_version("garbage") is None


def _candle(hour_end, ask, bid, volume="0.00", day=21):
    ts = int(datetime(2026, 10, day, hour_end, tzinfo=timezone.utc).timestamp())
    return {"end_period_ts": ts, "volume": volume,
            "yes_ask": {"close": ask}, "yes_bid": {"close": bid}}


def test_quote_never_uses_a_candle_after_the_decision():
    candles = [_candle(17, "0.30", "0.25"), _candle(19, "0.40", "0.35"), _candle(20, "0.99", "0.98")]
    q = bt.quote_at_decision(candles, DECISION_TS)
    assert (q["yes_ask"], q["yes_bid"]) == (40, 35)


def test_quote_falls_back_to_latest_earlier_candle_within_lookback():
    assert bt.quote_at_decision([_candle(17, "0.30", "0.25")], DECISION_TS)["yes_ask"] == 30
    assert bt.quote_at_decision([_candle(10, "0.30", "0.25")], DECISION_TS) is None   # too old


def test_quote_skips_candles_without_a_real_offer():
    candles = [_candle(18, "0.30", "0.00"), _candle(19, None, None)]
    q = bt.quote_at_decision(candles, DECISION_TS)
    assert q["yes_ask"] == 30 and q["yes_bid"] == 0


def test_volume_counts_only_the_24h_before_the_decision():
    candles = [_candle(18, "0.3", "0.2", "5.00"), _candle(19, "0.3", "0.2", "7.00"),
               _candle(20, "0.3", "0.2", "100.00"), _candle(18, "0.3", "0.2", "50.00", day=20)]
    assert bt.volume_before(candles, DECISION_TS) == 12


# --- end to end with fake sources ---------------------------------------------------

def _ladder(prefix, mdate, result_between):
    """A Kalshi-style 6-market day: <66, 66-67 ... 72-73, >73."""
    ev = bt.event_ticker(prefix, mdate)
    ms = [{"ticker": f"{ev}-T66", "strike_type": "less", "cap_strike": 66},
          {"ticker": f"{ev}-T73", "strike_type": "greater", "floor_strike": 73}]
    for lo in (66, 68, 70, 72):
        ms.append({"ticker": f"{ev}-B{lo + 0.5}", "strike_type": "between",
                   "floor_strike": lo, "cap_strike": lo + 1})
    for m in ms:
        m["result"] = "yes" if m["ticker"].endswith(result_between) else "no"
        m["expiration_value"] = ""
    return ms


class FakeClient:
    def __init__(self):
        self.calls = []
        # Chicago (forecast ~70F) on the current series; New York only on the OLD name.
        self.events = {"KXHIGHCHI-26OCT22": _ladder("KXHIGHCHI", MARKET_DATE, "B70.5"),
                       "HIGHNY-26OCT22": _ladder("HIGHNY", MARKET_DATE, "B66.5")}

    def _request(self, method, path, params=None):
        self.calls.append(path)
        if path in ("/historical/markets", "/markets"):
            return {"markets": self.events.get(params["event_ticker"], [])}
        if path.endswith("/candlesticks") and path.startswith("/historical/"):
            return {"candlesticks": [_candle(18, "0.20", "0.18", "30.00"),
                                     _candle(19, "0.25", "0.22", "30.00"),
                                     _candle(20, "0.95", "0.94", "30.00")]}
        return {"candlesticks": []}


class FakeResp:
    def __init__(self, status, text=""):
        self.status_code, self.text = status, text


def _fake_get(url, timeout=None):
    if "blend.20261021/19/" in url:
        return FakeResp(200, FIXTURE.read_text())
    return FakeResp(404)


CITIES = [({"name": "Chicago", "kalshi_series_prefix": "KXHIGHCHI", "expected_station": "KMDW"}, False),
          ({"name": "New York", "kalshi_series_prefix": "KXHIGHNY", "expected_station": "KNYC"}, False),
          ({"name": "Denver", "kalshi_series_prefix": "KXHIGHDEN", "expected_station": "KDEN"}, True)]


@pytest.fixture
def collected(tmp_path):
    conn = bt.open_db(str(tmp_path / "bt.db"))
    client = FakeClient()
    src = bt.Sources(client, http_get=_fake_get, pause_s=0)
    counts = bt.collect_month(conn, src, CITIES, 2026, 10, sigma=1.15, run_hour=19)
    return conn, client, src, counts, tmp_path / "bt.db"


def test_collects_a_full_ladder_with_the_bots_probabilities(collected):
    conn, *_ = collected
    rows = conn.execute("SELECT ticker, model_prob, kind, outcome, nbm_version, forecast_hour "
                        "FROM bt_markets WHERE city='Chicago'").fetchall()
    assert len(rows) == 6
    assert abs(sum(r[1] for r in rows) - 1.0) < 0.06        # whole-degree ladder sums to ~1
    assert {r[2] for r in rows} == {"bracket", "threshold"}
    assert {r[4] for r in rows} == {"5.0"} and {r[5] for r in rows} == {29}
    best = max(rows, key=lambda r: r[1])
    assert best[0].endswith("B70.5") and best[3] == "yes"   # forecast 70F; 70-71 won


def test_quotes_are_taken_at_the_decision_hour_not_after(collected):
    conn, *_ = collected
    asks = {r[0] for r in conn.execute("SELECT yes_ask_cents FROM bt_markets")}
    assert asks == {25}
    assert {r[0] for r in conn.execute("SELECT volume_24h FROM bt_markets")} == {60}


def test_falls_back_to_the_older_series_name(collected):
    conn, *_ = collected
    row = conn.execute("SELECT status, series_prefix FROM bt_city_days "
                       "WHERE city='New York' AND market_date='2026-10-22'").fetchone()
    assert row == ("ok", "HIGHNY")


def test_station_missing_from_bulletin_is_recorded_not_guessed(collected):
    conn, _, _, counts, _ = collected
    row = conn.execute("SELECT status FROM bt_city_days WHERE city='Denver' "
                       "AND market_date='2026-10-22'").fetchone()
    assert row == ("no_forecast",)
    # Every other day of the month has no bulletin in the fake archive.
    assert counts["no_bulletin"] == 30


def test_rerun_skips_done_days_and_retries_errors(collected):
    conn, client, src, _, _ = collected
    conn.execute("UPDATE bt_city_days SET status='error' WHERE city='Chicago' "
                 "AND market_date='2026-10-22'")
    conn.commit()
    before = len(client.calls)
    counts = bt.collect_month(conn, src, CITIES, 2026, 10, sigma=1.15, run_hour=19)
    assert counts["ok"] == 1                         # only Chicago's errored day was redone
    assert counts["skipped"] == 31 * 3 - 1
    assert len(client.calls) - before == 1 + 6       # one market lookup + six candle lookups


def test_identical_station_forecasts_abort_the_day(tmp_path):
    one = FIXTURE.read_text().split(" KNYC")[0]           # just the Chicago block
    fake = one + one.replace("KMDW", "KNYC") + one.replace("KMDW", "KMIA")
    cities = CITIES[:2] + [({"name": "Miami", "kalshi_series_prefix": "KXHIGHMIA",
                             "expected_station": "KMIA"}, False)]
    conn = bt.open_db(str(tmp_path / "bt.db"))
    src = bt.Sources(FakeClient(), http_get=lambda u, timeout=None: FakeResp(200, fake)
                     if "blend.20261021/19/" in u else FakeResp(404), pause_s=0)
    bt.collect_month(conn, src, cities, 2026, 10, sigma=1.15, run_hour=19)
    assert conn.execute("SELECT COUNT(*) FROM bt_markets").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM bt_city_days WHERE status='error'").fetchone()[0] == 3


# --- simulation and report ------------------------------------------------------------

RULES = dict(min_gap=0.15, min_yes_cents=11, min_volume=10)


def _row(**kw):
    r = dict(model_prob=0.50, yes_ask_cents=30, yes_bid_cents=28, outcome="yes",
             volume_24h=50, kind="bracket")
    r.update(kw)
    return r


def test_sim_buys_yes_at_the_ask_when_model_is_higher():
    b = simulate_bet(_row(), **RULES)
    assert (b["side"], b["price"], b["won"]) == ("yes", 30, True)
    assert b["pnl"] == 100 - 30 - b["fee"]


def test_sim_buys_no_at_100_minus_bid_with_gap_measured_against_the_ask():
    b = simulate_bet(_row(model_prob=0.10, yes_ask_cents=30, yes_bid_cents=28, outcome="no"), **RULES)
    assert (b["side"], b["price"], b["won"]) == ("no", 72, True)


def test_sim_respects_the_guards():
    assert simulate_bet(_row(model_prob=0.40), **RULES) is None                       # gap 10 pts
    assert simulate_bet(_row(model_prob=0.30, yes_ask_cents=10), **RULES) is None     # cheap YES
    assert simulate_bet(_row(volume_24h=5), **RULES) is None                          # thin market
    assert simulate_bet(_row(model_prob=0.0, yes_bid_cents=0, outcome="no"), **RULES) is None  # no bid
    assert simulate_bet(_row(yes_ask_cents=None), **RULES) is None                    # no quote
    assert simulate_bet(_row(kind="threshold"), **RULES, block_yes_threshold=True) is None
    assert simulate_bet(_row(kind="threshold", model_prob=0.05, outcome="no"),
                        **RULES, block_yes_threshold=True)["side"] == "no"           # NO still allowed


def test_actual_high_prefers_recorded_value_then_bracket_then_flags_tails():
    b = dict(kind="bracket", strike_type="between", floor_strike=70, cap_strike=71)
    t = dict(kind="threshold", strike_type="greater", floor_strike=73, cap_strike=None)
    assert actual_high([dict(b, outcome="yes", expiration_value="71.00")]) == (71.0, False)
    assert actual_high([dict(b, outcome="yes", expiration_value=None)]) == (70.5, False)
    assert actual_high([dict(b, outcome="no", expiration_value=None),
                        dict(t, outcome="yes", expiration_value=None)]) == (74, True)


def test_report_builds_from_collected_data(collected, tmp_path):
    *_, db_path = collected
    text = build_report(str(db_path), config_path="config.yaml", holdout_from="2026-11-01")
    for section in ("1. COVERAGE", "2. MODEL VS MARKET", "3. SIMULATED TRADING",
                    "4. FORECAST ACCURACY", "5. NEW YORK DETAIL", "6. HOLDOUT"):
        assert section in text
    assert "Chicago" in text and "HIGHNY" in text


def test_report_handles_an_empty_database(tmp_path):
    bt.open_db(str(tmp_path / "empty.db")).close()
    assert "No markets collected yet" in build_report(str(tmp_path / "empty.db"))


def test_holdout_is_fixed_by_date_not_by_how_much_was_collected(collected):
    *_, db_path = collected
    assert "HOLDOUT — 2026-10" in build_report(str(db_path), holdout_from="2026-08-01")
    assert "none collected yet" in build_report(str(db_path), holdout_from="2026-11-01")


def test_markets_that_cannot_be_scored_are_logged_with_the_reason(tmp_path):
    client = FakeClient()
    winner = next(m for m in client.events["KXHIGHCHI-26OCT22"] if m["ticker"].endswith("B70.5"))
    winner["result"] = "all_yes"                     # an unexpected result value
    conn = bt.open_db(str(tmp_path / "bt.db"))
    bt.collect_month(conn, bt.Sources(client, http_get=_fake_get, pause_s=0), CITIES[:1],
                     2026, 10, sigma=1.15, run_hour=19)
    row = conn.execute("SELECT ticker, reason FROM bt_skipped").fetchone()
    assert row == ("KXHIGHCHI-26OCT22-B70.5", "result='all_yes'")
    assert conn.execute("SELECT COUNT(*) FROM bt_markets").fetchone()[0] == 5


def test_report_drops_days_whose_winner_is_missing(tmp_path):
    from src.backtest_report import complete_days_only
    day = [dict(city="NY", market_date="2025-01-20", ticker=f"T{i}", outcome="no") for i in range(5)]
    ok = [dict(city="NY", market_date="2025-01-21", ticker=f"T{i}", outcome="yes" if i == 2 else "no")
          for i in range(6)]
    keep, bad = complete_days_only(day + ok)
    assert len(keep) == 6 and bad == [("NY", "2025-01-20")]


def test_midpoint_is_scored_on_the_same_markets_as_the_ask(tmp_path):
    from src.backtest_report import scorecard, Report
    rows = [dict(city="NY", market_date="2025-01-21", outcome="no", model_prob=0.02,
                 yes_ask_cents=4, yes_bid_cents=0),
            dict(city="NY", market_date="2025-01-21", outcome="yes", model_prob=0.6,
                 yes_ask_cents=62, yes_bid_cents=58)]
    out = Report()
    scorecard(out, rows, lambda r: r["city"], "t")
    line = out.lines[-1].split()
    ask, mid = float(line[4]), float(line[5])
    expected_mid = ((0.02 - 0) ** 2 + (0.60 - 1) ** 2) / 2
    assert abs(mid - round(expected_mid, 3)) < 1e-3 and ask != mid
