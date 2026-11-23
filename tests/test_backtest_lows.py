"""Daily-LOW backtest - offline tests built on a REAL NBM V4.2 bulletin
(run 2024-12-07T13Z, all 13 stations, captured by the Capture NBM Fixtures workflow)."""
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src import backtest as bt
from src import nbm
from src.backtest_report import build_report

FIXTURE = Path(__file__).parent / "fixtures" / "nbm_nbp_real_2024-12-07T13Z_v42.txt"
RUN_ID = "2024-12-07T13Z"
MARKET_DATE = date(2024, 12, 8)


def _parsed(station):
    return nbm.parse_station_maxt(FIXTURE.read_text(), station)


# --- forecast column ----------------------------------------------------------------

def test_low_uses_the_12z_column_on_the_market_date():
    low = bt.get_min_temp_forecast(_parsed("KNYC"), RUN_ID, MARKET_DATE)
    assert low["forecast_hour"] == 23                                  # 13Z + 23h = 12Z Dec 8
    assert low["values"] == {"p10": 33, "p25": 34, "p50": 35, "p75": 37, "p90": 39}


def test_low_and_high_come_from_different_columns():
    p = _parsed("KMDW")
    low = bt.forecast_for("low", p, RUN_ID, MARKET_DATE)
    high = bt.forecast_for("high", p, RUN_ID, MARKET_DATE)
    assert low["forecast_hour"] == 23 and high["forecast_hour"] == 35
    assert low["values"]["p50"] < high["values"]["p50"]                 # 35F night, 48F day


def test_low_column_missing_when_market_date_is_out_of_range():
    assert bt.get_min_temp_forecast(_parsed("KNYC"), RUN_ID, date(2024, 12, 30)) is None


# --- series names and station checks -------------------------------------------------

def test_low_series_candidates():
    assert bt.low_prefix_candidates({"kalshi_series_prefix": "KXHIGHNY"}) == [
        "KXLOWTNY", "KXLOWNY", "KXLOWTNYC", "KXLOWNYC"]
    assert bt.low_prefix_candidates({"kalshi_series_prefix": "KXHIGHTPHX"}) == ["KXLOWTPHX", "KXLOWPHX"]
    assert bt.low_prefix_candidates({"kalshi_series_prefix": "KXHIGHCHI",
                                     "kalshi_low_series_prefix": "KXLOWTCHI"})[0] == "KXLOWTCHI"


def test_settlement_station_read_from_rules():
    assert bt.settlement_station({"rules_primary": "...Data for CLINYC can be found at..."}) == "KNYC"
    assert bt.settlement_station({"rules_primary": "Central Park, New York"}) is None


def test_a_database_never_mixes_highs_and_lows(tmp_path):
    bt.open_db(str(tmp_path / "x.db"), "low").close()
    with pytest.raises(ValueError):
        bt.open_db(str(tmp_path / "x.db"), "high")


# --- end to end ------------------------------------------------------------------------

def _low_ladder(event, station_cli, winner):
    ms = [{"ticker": f"{event}-T31", "strike_type": "less", "cap_strike": 31},
          {"ticker": f"{event}-T38", "strike_type": "greater", "floor_strike": 38}]
    for lo in (31, 33, 35, 37):
        ms.append({"ticker": f"{event}-B{lo + 0.5}", "strike_type": "between",
                   "floor_strike": lo, "cap_strike": lo + 1})
    for m in ms:
        m["result"] = "yes" if m["ticker"].endswith(winner) else "no"
        m["rules_primary"] = f"If the minimum temperature ... Data for {station_cli} can be found ..."
    return ms


class FakeClient:
    def __init__(self):
        self.events = {
            # New York lows settle on Central Park, like the highs: OK.
            "KXLOWTNYC-24DEC08": _low_ladder("KXLOWTNYC-24DEC08", "CLINYC", "B35.5"),
            # Chicago's low series settles on O'Hare, but Chicago highs settle on Midway: refuse.
            "KXLOWTCHI-24DEC08": _low_ladder("KXLOWTCHI-24DEC08", "CLIORD", "B33.5"),
        }

    def _request(self, method, path, params=None):
        if path in ("/historical/markets", "/markets"):
            return {"markets": self.events.get(params["event_ticker"], [])}
        if path.startswith("/historical/") and path.endswith("/candlesticks"):
            # New York's climate day for Dec 8 starts 05Z Dec 8, so the quote hours are
            # -6h = 23Z Dec 7, -3h = 02Z (primary), +1h = 06Z, +3h = 08Z.
            def c(day, hour, ask, bid):
                ts = int(datetime(2024, 12, day, hour, tzinfo=timezone.utc).timestamp())
                return {"end_period_ts": ts, "volume": "20.00",
                        "yes_ask": {"close": ask}, "yes_bid": {"close": bid}}
            return {"candlesticks": [c(7, 23, "0.40", "0.35"), c(8, 2, "0.30", "0.26"),
                                     c(8, 6, "0.20", "0.18"), c(8, 8, "0.10", "0.08")]}
        return {"candlesticks": []}


class Resp:
    def __init__(self, status, text=""):
        self.status_code, self.text = status, text


CITIES = [({"name": "New York", "kalshi_series_prefix": "KXHIGHNY", "expected_station": "KNYC",
            "timezone": "America/New_York"}, False),
          ({"name": "Chicago", "kalshi_series_prefix": "KXHIGHCHI", "expected_station": "KMDW",
            "timezone": "America/Chicago"}, False),
          ({"name": "Miami", "kalshi_series_prefix": "KXHIGHMIA", "expected_station": "KMIA",
            "timezone": "America/New_York"}, False)]


@pytest.fixture
def collected(tmp_path):
    db = tmp_path / "low.db"
    conn = bt.open_db(str(db), "low")
    src = bt.Sources(FakeClient(), pause_s=0,
                     http_get=lambda url, timeout=None: Resp(200, FIXTURE.read_text())
                     if "blend.20241207/13/" in url else Resp(404))
    # The fixture is a 13Z run; real low backtests use the 19Z run (LOW_RUN_HOUR).
    counts = bt.collect_month(conn, src, CITIES, 2024, 12, sigma=1.15, kind="low", run_hour=13)
    return conn, counts, db


def _status(conn, city):
    return conn.execute("SELECT status, series_prefix, detail FROM bt_city_days "
                        "WHERE city=? AND market_date='2024-12-08'", (city,)).fetchone()


def test_new_york_lows_are_scored_from_the_overnight_minimum(collected):
    conn, counts, _ = collected
    assert _status(conn, "New York")[:2] == ("ok", "KXLOWTNYC")
    rows = conn.execute("SELECT ticker, model_prob, forecast_hour, ladder_json FROM bt_markets "
                        "WHERE city='New York'").fetchall()
    assert len(rows) == 6 and {r[2] for r in rows} == {23}
    assert abs(sum(r[1] for r in rows) - 1.0) < 0.06
    top_two = {r[0].split("-")[-1] for r in sorted(rows, key=lambda r: -r[1])[:2]}
    assert top_two == {"B33.5", "B35.5"}        # median low 35F sits between 33-34 and 35-36


def test_a_low_series_on_a_different_station_is_refused(collected):
    conn, counts, _ = collected
    status, prefix, detail = _status(conn, "Chicago")
    assert status == "wrong_station" and "KORD" in detail
    assert conn.execute("SELECT COUNT(*) FROM bt_markets WHERE city='Chicago'").fetchone()[0] == 0
    assert counts["wrong_station"] == 1


def test_city_without_low_markets_is_recorded(collected):
    conn, *_ = collected
    assert _status(conn, "Miami")[0] == "no_markets"


def test_report_labels_itself_as_lows(collected):
    *_, db = collected
    text = build_report(str(db), holdout_from="2027-01-01")
    assert "DAILY LOWS" in text and "actual low minus NBM median" in text
    assert "wrong stn" in text and "KXLOWTNYC" in text


# --- timing (revised 2026-11-21) ---------------------------------------------------------

@pytest.mark.parametrize("tz,hour", [("America/New_York", 5), ("America/Chicago", 6),
                                     ("America/Denver", 7), ("America/Phoenix", 7),
                                     ("America/Los_Angeles", 8)])
def test_climate_day_starts_at_local_standard_midnight_all_year(tz, hour):
    for d in (date(2026, 1, 10), date(2026, 7, 10)):          # winter and summer (DST)
        start = bt.climate_day_start_utc(d, tz)
        assert (start.date(), start.hour) == (d, hour)


def test_low_decision_is_9pm_local_standard_the_evening_before():
    t = bt.decision_times("low", date(2024, 12, 7), MARKET_DATE, "America/New_York")
    primary = datetime.fromtimestamp(t["primary"], tz=timezone.utc)
    assert (primary.day, primary.hour) == (8, 2)               # 02Z Dec 8 = 9 PM EST Dec 7
    assert sorted(k for k in t if k != "primary") == [-6, -3, 1, 3]


def test_high_decision_is_unchanged():
    t = bt.decision_times("high", date(2024, 12, 7), MARKET_DATE, "America/New_York")
    assert datetime.fromtimestamp(t["primary"], tz=timezone.utc) == datetime(2024, 12, 7, 19, tzinfo=timezone.utc)


def test_lows_store_the_primary_quote_and_every_quote_hour(collected):
    conn, *_ = collected
    asks = {r[0] for r in conn.execute("SELECT yes_ask_cents FROM bt_markets WHERE city='New York'")}
    assert asks == {30}                                         # 9 PM quote, never a later one
    by_offset = dict(conn.execute("SELECT offset_h, MAX(yes_ask_cents) FROM bt_quotes "
                                  "WHERE ticker LIKE 'KXLOWTNYC%' GROUP BY offset_h").fetchall())
    assert by_offset == {-6: 40, -3: 30, 1: 20, 3: 10}


def test_low_runs_default_to_the_19z_bulletin():
    seen = []
    src = bt.Sources(FakeClient(), pause_s=0,
                     http_get=lambda url, timeout=None: (seen.append(url), Resp(404))[1])
    conn = bt.open_db(":memory:", "low")
    bt.collect_month(conn, src, CITIES[:1], 2024, 12, sigma=1.15, kind="low")
    assert seen and all("/19/text/blend_nbptx.t19z" in u for u in seen)
