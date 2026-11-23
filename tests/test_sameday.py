"""Same-day study: METAR parsing, look-ahead-free reconstruction, collection and report."""
from datetime import date, datetime, timezone

import pytest

from src import backtest as bt
from src import sameday as sd
from src import sameday_report as rep

# Real-format METARs (NWS remark conventions).
WARM = ("KNYC 081851Z AUTO 25008KT 10SM CLR 13/M04 A3002 RMK AO2 SLP165 "
        "T01331044 10139 20094 58008")
COLD = "KMSP 150053Z 31012KT 10SM FEW040 M11/M15 A3041 RMK AO2 SLP322 T11061150 11017 21106 51012"
NO_RMK = "KLAX 011753Z 25010KT 10SM FEW020 21/14 A2992"


# --- METAR parsing -----------------------------------------------------------------

def test_parse_metar_reads_t_group_and_six_hour_groups():
    p = sd.parse_metar(WARM)
    assert p == {"t_group_c": 13.3, "max6_c": 13.9, "min6_c": 9.4}


def test_parse_metar_handles_negative_temperatures():
    p = sd.parse_metar(COLD)
    assert p == {"t_group_c": -10.6, "max6_c": -1.7, "min6_c": -10.6}


def test_parse_metar_ignores_groups_outside_remarks_and_other_remark_codes():
    assert sd.parse_metar(NO_RMK) == {"t_group_c": None, "max6_c": None, "min6_c": None}
    # SLP165, 58008 (pressure tendency) and P0012 (precip) must not be read as temperatures.
    p = sd.parse_metar("KNYC 081851Z 13/M04 A3002 RMK AO2 SLP165 P0012 58008 T01331044")
    assert p["max6_c"] is None and p["min6_c"] is None and p["t_group_c"] == 13.3


@pytest.mark.parametrize("c,f", [(13.9, 57), (21.7, 71), (-1.7, 29), (22.5, 73), (0.0, 32), (-17.8, 0)])
def test_celsius_to_whole_fahrenheit_rounds_half_up(c, f):
    assert sd.c_to_f_whole(c) == f


def test_parse_iem_csv():
    text = ("station,valid,tmpf,metar\n"
            f"NYC,2025-01-08 18:51,55.94,{WARM}\n"
            "NYC,2025-01-08 19:51,M,KNYC 081951Z 12/M04 RMK AO2\n"
            "NYC,bad-date,50.0,x\n")
    rows = sd.parse_iem_csv(text, "KNYC")
    assert len(rows) == 2
    assert rows[0]["valid_ts"] == int(datetime(2025, 1, 8, 18, 51, tzinfo=timezone.utc).timestamp())
    assert rows[0]["tmpf"] == 55.94 and rows[0]["max6_c"] == 13.9
    assert rows[1]["tmpf"] is None
    assert sd.parse_iem_csv("#comment only\n", "KNYC") == []


# --- reconstruction -----------------------------------------------------------------

DAY = date(2025, 1, 8)
START = int(bt.climate_day_start_utc(DAY, "America/New_York").timestamp())     # 05Z Jan 8
END = START + 86400


def ob(hour_utc, minute, f=None, tg=None, max6=None, day=8):
    ts = int(datetime(2025, 1, day, hour_utc, minute, tzinfo=timezone.utc).timestamp())
    return {"valid_ts": ts, "tmpf": f, "t_group_c": tg, "max6_c": max6}


OBS = sorted([
    ob(4, 51, f=60),                    # before the climate day starts: ignored
    ob(11, 51, max6=20.0),              # 6h window 05:51-11:51 is inside the day (68F)
    ob(5, 51, max6=25.0),               # 6h window starts BEFORE the day (77F): ignored
    ob(17, 51, tg=13.3),                # 56F
    ob(18, 51, tg=13.9, max6=14.4),     # 57F, 6h max 58F
    ob(19, 51, tg=12.2),                # 54F
    ob(21, 51, tg=30.0),                # 86F but only usable after 22:01Z
], key=lambda o: o["valid_ts"])


def test_running_max_uses_only_the_climate_day_and_usable_reports():
    t = int(datetime(2025, 1, 8, 20, 2, tzinfo=timezone.utc).timestamp())       # ~3 PM EST
    r, cur = rep.state_at(OBS, START, END, t)
    assert (r, cur) == (68, 54)        # 6h max at 11:51Z wins; the 21:51Z report isn't out yet
    t_early = int(datetime(2025, 1, 8, 20, 0, tzinfo=timezone.utc).timestamp())
    assert rep.state_at(OBS, START, END, t_early)[1] == 57   # 19:51Z report not usable until 20:01Z


def test_a_report_counts_only_ten_minutes_after_it_is_made():
    t = int(datetime(2025, 1, 8, 22, 0, tzinfo=timezone.utc).timestamp())
    assert rep.state_at(OBS, START, END, t)[0] == 68                            # 21:51 + 10 min > 22:00
    t2 = int(datetime(2025, 1, 8, 22, 2, tzinfo=timezone.utc).timestamp())
    assert rep.state_at(OBS, START, END, t2) == (86, 86)


def test_bracket_logic():
    b = {"kind": "bracket", "strike_type": "between", "floor_strike": 56, "cap_strike": 57}
    above = {"kind": "threshold", "strike_type": "greater", "floor_strike": 59, "cap_strike": None}
    below = {"kind": "threshold", "strike_type": "less", "cap_strike": 54, "floor_strike": None}
    assert rep.bracket_contains(b, 57) and not rep.bracket_contains(b, 58)
    assert rep.bracket_contains(above, 60) and not rep.bracket_contains(above, 59)
    assert rep.bracket_contains(below, 53) and not rep.bracket_contains(below, 54)
    assert rep.wholly_below(b, 59) and not rep.wholly_below(b, 58)              # 2F margin
    assert rep.wholly_below(below, 55) and not rep.wholly_below(above, 99)


def test_quote_is_latest_not_later_and_not_stale():
    s = [(100, 10, 12), (3700, 20, 22), (7300, 30, 32)]
    assert rep.quote_at(s, 7299)[2] == 22
    assert rep.quote_at(s, 7300)[2] == 32
    assert rep.quote_at(s, 7300 + 2 * 3600 + 1) is None      # latest quote over 2h old
    assert rep.quote_at(s, 50) is None


# --- collection -----------------------------------------------------------------------

class Resp:
    def __init__(self, status, text=""):
        self.status_code, self.text = status, text


def test_collect_obs_stores_rows_and_resumes(tmp_path):
    calls = []

    def get(url, params=None, timeout=None):
        calls.append(dict(params))
        return Resp(200, f"station,valid,tmpf,metar\nNYC,2025-01-08 18:51,55.9,{WARM}\n")
    conn = sd.open_db(str(tmp_path / "s.db"))
    c1 = sd.collect_obs(conn, get, ["KNYC"], 2025, 1, sleep=lambda s: None)
    assert c1["rows"] == 1 and calls[0]["station"] == "NYC"
    c2 = sd.collect_obs(conn, get, ["KNYC"], 2025, 1, sleep=lambda s: None)
    assert c2["skipped"] == 1 and len(calls) == 1


def test_collect_obs_failure_is_reported_not_fatal(tmp_path, monkeypatch):
    monkeypatch.setattr(sd.time, "sleep", lambda s: None)
    conn = sd.open_db(str(tmp_path / "s.db"))
    c = sd.collect_obs(conn, lambda *a, **k: Resp(503), ["KNYC"], 2025, 1, sleep=lambda s: None)
    assert c["failed"] == 1
    assert conn.execute("SELECT COUNT(*) FROM obs_done").fetchone()[0] == 0    # retried next run


class FakeClient:
    def __init__(self):
        self.n = 0

    def _request(self, method, path, params=None):
        self.n += 1
        if path.startswith("/historical/") and path.endswith("/candlesticks"):
            ts = params["start_ts"] + 7200
            return {"candlesticks": [{"end_period_ts": ts, "volume": "5.00",
                                      "yes_ask": {"close": "0.70"}, "yes_bid": {"close": "0.66"}}]}
        return {"candlesticks": []}


def test_collect_quotes_and_resume(tmp_path):
    conn = sd.open_db(str(tmp_path / "s.db"))
    client = FakeClient()
    src = bt.Sources(client, pause_s=0)
    mk = [{"ticker": "KXHIGHNY-25JAN08-B56.5", "market_date": "2025-01-08", "series_prefix": "KXHIGHNY"}]
    assert sd.collect_quotes(conn, src, mk)["candles"] == 1
    assert conn.execute("SELECT yes_bid_cents, yes_ask_cents FROM quotes").fetchone() == (66, 70)
    before = client.n
    assert sd.collect_quotes(conn, src, mk)["skipped"] == 1 and client.n == before


# --- report end to end -------------------------------------------------------------------

def test_report_end_to_end(tmp_path):
    # A New York day: high so far 57F by 3 PM, temperature falling; 56-57 bracket wins.
    bdb = bt.open_db(str(tmp_path / "b.db"))
    ladder = [("T52", "threshold", "less", None, 52, "no"), ("B52.5", "bracket", "between", 52, 53, "no"),
              ("B54.5", "bracket", "between", 54, 55, "no"), ("B56.5", "bracket", "between", 56, 57, "yes"),
              ("B58.5", "bracket", "between", 58, 59, "no"), ("T59", "threshold", "greater", 59, None, "no")]
    for suf, kind, st, fl, cap, outc in ladder:
        bdb.execute("INSERT INTO bt_markets (ticker, city, is_shadow, series_prefix, market_date, run_id,"
                    " ladder_json, kind, strike_type, floor_strike, cap_strike, model_prob, outcome, collected_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"KXHIGHNY-25JAN08-{suf}", "New York", 0, "KXHIGHNY", "2025-01-08", "x", "{}",
                     kind, st, fl, cap, 0.1, outc, "now"))
    bdb.commit()
    sdb = sd.open_db(str(tmp_path / "s.db"))
    for o in OBS[:-1]:                                   # drop the bogus 86F report
        sdb.execute("INSERT INTO obs (station, valid_ts, tmpf, t_group_c, max6_c) VALUES (?,?,?,?,?)",
                    ("KNYC", o["valid_ts"], o["tmpf"], o["t_group_c"], o["max6_c"]))
    # The 68F 6-hour group makes the bracket logic use 68; replace it with a realistic 57F max.
    sdb.execute("UPDATE obs SET max6_c=13.9 WHERE max6_c=20.0")
    for h in range(15, 30):
        ts = START + h * 3600 - 5 * 3600
        for suf, *_ in ladder:
            ask, bid = (80, 76) if suf == "B56.5" else (3, 1)
            sdb.execute("INSERT INTO quotes VALUES (?,?,?,?,?)", (f"KXHIGHNY-25JAN08-{suf}", ts, bid, ask, 10))
    sdb.commit()
    text = rep.build_report(str(tmp_path / "s.db"), str(tmp_path / "b.db"))
    assert "C. DATA CHECK" in text and "A. THE HIGH-SO-FAR BRACKET" in text and "VERDICT" in text
    assert "New York" in text and "City-days with observations and intraday quotes: 1" in text


def test_whole_degree_report_never_raises_running_max():
    # NWS FAQ: 5-minute METARs carry whole degrees C, which can read 1-2F above the
    # ASOS value once converted back. Such a report sets the current temp only.
    obs = sorted([ob(17, 51, tg=13.3),            # 56F, exact
                  ob(18, 5, f=58.0)],             # whole-C report, no T-group
                 key=lambda o: o["valid_ts"])
    ts = [o["valid_ts"] for o in obs]
    t = int(datetime(2025, 1, 8, 19, 0, tzinfo=timezone.utc).timestamp())
    run_max, current = rep.state_at(obs, START, END, t, ts)
    assert run_max == 56 and current == 58
