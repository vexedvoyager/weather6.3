import gzip
import os
import shutil
from datetime import datetime, timezone

from src import minute_recorder as mr
from src import minute_report as rp

T0 = int(datetime(2026, 12, 2, 15, 0, tzinfo=timezone.utc).timestamp())     # in the development week


# --- recorder helpers -----------------------------------------------------------

def test_series_filter():
    assert mr.is_daily_temp_series("KXHIGHNY") and mr.is_daily_temp_series("KXLOWTCHI")
    assert not mr.is_daily_temp_series("KXLOWTPERIOD")
    assert not mr.is_daily_temp_series("KXTEMPMIAH") and not mr.is_daily_temp_series("KXHIGHNY-26DEC02")


def test_quote_from_market_derives_ask_and_handles_empty_sides():
    q = mr.quote_from_market({"yes_bid_dollars": "0.4200", "no_bid_dollars": "0.5500", "yes_bid_size_fp": "120.00"})
    assert q["bid"] == 42 and q["ask"] == 45 and q["bid_size"] == 120.0 and q["ask_size"] is None
    e = mr.quote_from_market({"yes_bid_dollars": "0.0000", "no_bid_dollars": "0.0000"})
    assert e["bid"] is None and e["ask"] is None


def test_ladder_gap_and_stray():
    assert mr.ladder_gap({"a": {"bid": 10, "ask": 12}, "b": {"bid": 80, "ask": 84}}) == (96, 90)
    assert mr.ladder_gap({"a": {"bid": None, "ask": 12}}) is None
    hist = [50.0] * 25
    assert mr.is_stray(hist, 48, 44) == "ask" and mr.is_stray(hist, 56, 60) == "bid"
    assert mr.is_stray(hist, 48, 52) is None and mr.is_stray([50.0] * 5, 48, 30) is None


class FakeClient:
    """Six-leg ladder whose asks sum to 90c at minutes 2 and 3 (a ladder gap)."""

    def __init__(self):
        self.minute = 0
        self.orderbooks = 0

    def get_series_list(self, category):
        return [{"ticker": "KXHIGHNY"}, {"ticker": "KXTEMPNYCH"}]

    def get_markets_by_series(self, series, status="open"):
        gap = self.minute in (2, 3)
        out = []
        for i in range(6):
            ask = 15 if gap else 17              # 6 x 15 = 90c during the gap, 6 x 17 = 102c otherwise
            out.append({"ticker": f"KXHIGHNY-26DEC02-B{40 + 2 * i}.5", "event_ticker": "KXHIGHNY-26DEC02",
                        "strike_type": "between", "floor_strike": 40 + 2 * i, "cap_strike": 41 + 2 * i,
                        "close_time": "2026-12-03T05:00:00Z", "rules_primary": "CLINYC",
                        "yes_bid_dollars": "0.1400", "no_bid_dollars": f"{(100 - ask) / 100:.4f}"})
        return out

    def get_orderbook(self, ticker):
        self.orderbooks += 1
        return {"yes_dollars": [["0.14", "50.00"]], "no_dollars": [["0.85", "30.00"]]}


def record(tmp_path, minutes=5):
    conn = mr.open_chunk(str(tmp_path / "c.db"))
    client = FakeClient()
    clock = {"t": float(T0)}

    def now():
        return clock["t"]

    def sleep(s):
        clock["t"] += max(s, 1)
        client.minute = int((clock["t"] - T0) // 60)

    stats = mr.Recorder(client, conn, now, sleep).run(minutes, stop_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
    conn.close()
    return stats, client


def test_recorder_writes_changes_polls_and_depth(tmp_path):
    stats, client = record(tmp_path)
    import sqlite3
    c = sqlite3.connect(str(tmp_path / "c.db"))
    assert stats["minutes"] == 5
    assert c.execute("SELECT COUNT(DISTINCT series) FROM polls").fetchone()[0] == 1      # KXTEMP excluded
    # minute 0 writes 6 rows; minutes 2 (gap starts) and 4 (gap ends) change all 6; others unchanged
    assert c.execute("SELECT COUNT(*) FROM quotes").fetchone()[0] == 18
    assert c.execute("SELECT COUNT(*) FROM depth WHERE reason='ladder'").fetchone()[0] == 12
    assert c.execute("SELECT station FROM meta LIMIT 1").fetchone()[0] == "KNYC"


def test_station_looked_up_once_per_event_when_rules_missing(tmp_path):
    class NoRules(FakeClient):
        def __init__(self):
            super().__init__()
            self.lookups = 0

        def get_markets_by_series(self, series, status="open"):
            ms = super().get_markets_by_series(series, status)
            for m in ms:
                m.pop("rules_primary", None)
            return ms

        def get_market(self, ticker):
            self.lookups += 1
            return {"rules_primary": "settles on the CLINYC report"}
    conn = mr.open_chunk(str(tmp_path / "n.db"))
    client = NoRules()
    clock = {"t": float(T0)}
    mr.Recorder(client, conn, lambda: clock["t"], lambda s: clock.__setitem__("t", clock["t"] + max(s, 1))).run(
        3, stop_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert client.lookups == 1
    assert {r[0] for r in conn.execute("SELECT station FROM meta")} == {"KNYC"}


def test_recorder_stops_at_stop_time(tmp_path):
    conn = mr.open_chunk(str(tmp_path / "s.db"))
    stats = mr.Recorder(FakeClient(), conn, lambda: float(T0), lambda s: None).run(
        10, stop_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    assert stats.get("minutes", 0) == 0


# --- report helpers ------------------------------------------------------------

def test_fee_period_and_levels():
    assert abs(rp.fee_c(50) - 1.75) < 1e-9
    assert rp.period_of(T0) == "dev"
    assert rp.period_of(int(datetime(2026, 12, 9, tzinfo=timezone.utc).timestamp())) == "hold"
    assert rp.period_of(int(datetime(2026, 11, 28, tzinfo=timezone.utc).timestamp())) is None
    assert rp.level_size_at([["0.84", "30.00"], ["0.80", "5"]], 84) == 30.0


def test_certain_side():
    br = {"strike_type": "between", "floor_strike": 40, "cap_strike": 41}
    assert rp.certain_side(br, 43) == "no" and rp.certain_side(br, 42) is None
    gt = {"strike_type": "greater", "floor_strike": 45}
    assert rp.certain_side(gt, 48) == "yes" and rp.certain_side(gt, 47) is None
    lt = {"strike_type": "less", "cap_strike": 40}
    assert rp.certain_side(lt, 41) == "no" and rp.certain_side(lt, 40) is None


def test_episodes_break_on_gaps():
    mins = [0, 60, 120, 240, 300]
    assert rp.episodes(mins, [True, True, False, True, True]) == [(0, 2), (3, 2)]
    assert rp.episodes(mins, [False, True, True, True, False]) == [(1, 2), (3, 1)]   # gap at 120->240


def test_verdict():
    assert rp.verdict([0.6] * 20, [0.1])[0] is True
    assert rp.verdict([0.6] * 19, [0.1])[0] is False
    assert rp.verdict([0.6] * 25, [-0.1])[0] is False


def test_end_to_end_ladder_gap_is_caught(tmp_path):
    record(tmp_path)
    os.makedirs(tmp_path / "chunks")
    with open(tmp_path / "c.db", "rb") as f, gzip.open(tmp_path / "chunks" / "chunk_x.db.gz", "wb") as g:
        shutil.copyfileobj(f, g)
    db, n = rp.load(str(tmp_path / "chunks"))
    d = rp.Data(db)
    opps = [o for o in rp.ladder_opps(d) if o["side"] == "yes"]
    assert len(opps) == 1
    o = opps[0]
    # asks 6 x 15c = 90c; fee 0.07 x .15 x .85 = 0.89c each -> ~4.6c per set; 30 contracts at the ask
    assert o["caught"] and o["minutes"] == 2 and o["size"] == 30
    assert abs(o["per_set"] - (10 - 6 * rp.fee_c(15))) < 1e-9
    text = rp.build(db, n, "health")
    assert "STALENESS BY HOUR" in text and "health mode" in text


def test_stray_then_settlement():
    o = [{"ticker": "X", "ts": T0, "caught": True, "side": "yes", "price": 40, "size": 10, "minutes": 2}]
    rp.settle_strays(o, {"X": "yes"})
    assert abs(o[0]["profit"] - (60 - rp.fee_c(40)) / 100 * 10) < 1e-9


def test_final_report_runs_and_finds_slow_reaction(tmp_path):
    record(tmp_path)
    os.makedirs(tmp_path / "chunks")
    with open(tmp_path / "c.db", "rb") as f, gzip.open(tmp_path / "chunks" / "chunk_x.db.gz", "wb") as g:
        shutil.copyfileobj(f, g)
    db, n = rp.load(str(tmp_path / "chunks"))
    # 40F at 08:51Z, then a new high of 44F public 1 minute before the recorder's first minute:
    # the 40-41F bracket is now >= 2F below the high, so its NO is certain while YES is still bid 14c.
    obs = [{"valid_ts": T0 - 6 * 3600, "t_group_c": 4.4, "max6_c": None, "tmpf": None},
           {"valid_ts": T0 - 360, "t_group_c": 6.7, "max6_c": None, "tmpf": None}]
    slow = rp.slow_reaction_opps(rp.Data(db), {"KNYC": obs}, {"KXHIGHNY": "America/New_York"})
    hit = [o for o in slow if o["ticker"].endswith("B40.5")]
    assert len(hit) == 1 and hit[0]["side"] == "no" and hit[0]["price"] == 86 and hit[0]["caught"]
    text = rp.build(db, n, "final", {}, {"KNYC": obs}, {"KXHIGHNY": "America/New_York"})
    assert "A. LADDER GAPS" in text and "C. SLOW REACTION" in text and "VERDICT" in text


# --- check D: forecast bulletins ---------------------------------------------------

REAL = open(os.path.join(os.path.dirname(__file__), "fixtures", "nbm_real_2026-10-21T19Z.txt")).read()


class Resp:
    def __init__(self, code, text=""):
        self.status_code, self.text = code, text


def test_due_runs_and_extract_forecasts():
    now = datetime(2026, 10, 21, 20, 30, tzinfo=timezone.utc)
    assert [r[0] for r in mr.due_runs(now)] == ["2026-10-21T19Z", "2026-10-21T13Z"]   # 07Z is 13.5 h old: outside 8 h
    earlier = datetime(2026, 10, 21, 14, 0, tzinfo=timezone.utc)
    assert [r[0] for r in mr.due_runs(earlier)] == ["2026-10-21T13Z", "2026-10-21T07Z"]
    rows = mr.extract_forecasts(REAL, "2026-10-21T19Z", ["KMDW"], datetime(2026, 10, 21).date())
    high = [r for r in rows if r[1] == "high" and r[2] == "2026-10-22"][0]
    low = [r for r in rows if r[1] == "low" and r[2] == "2026-10-22"][0]
    assert high[5] == 70 and low[5] == 52          # p50: Chicago high 70F, low 52F (matches the real bulletin)


def test_watcher_release_minute_retry_and_at_start(tmp_path):
    conn = mr.open_chunk(str(tmp_path / "w.db"))
    live, body = {"2026-10-21T19Z"}, {"text": REAL}

    def head(url):
        return Resp(200 if any(f"/{r[11:13]}/text/" in url and r[:10].replace('-', '') in url for r in live) else 404)

    w = mr.ForecastWatcher(conn, ["KMDW"], http_head=head, http_get=lambda url: Resp(200, body["text"]))
    t1 = int(datetime(2026, 10, 21, 20, 30, tzinfo=timezone.utc).timestamp())
    assert w.poll(t1) == 1 and w.poll(t1 + 60) == 0           # found in the run's first minute; read once
    # 01Z appears at t2 but is still half-uploaded (doesn't parse): keep its first minute, retry...
    live.add("2026-10-22T01Z")
    body["text"] = REAL[:200]
    t2 = int(datetime(2026, 10, 22, 2, 41, tzinfo=timezone.utc).timestamp())
    assert w.poll(t2) == 0 and w.poll(t2 + 60) == 0
    # ...and after 5 minutes accept it anyway, still dated t2.
    assert w.poll(t2 + 300) == 1
    rows = conn.execute("SELECT run_id, first_seen_ts, at_start FROM bulletins ORDER BY first_seen_ts").fetchall()
    assert rows == [("2026-10-21T19Z", t1, 1), ("2026-10-22T01Z", t2, 0)]
    assert conn.execute("SELECT COUNT(*) FROM forecasts WHERE run_id='2026-10-21T19Z'").fetchone()[0] > 0


def test_watcher_falls_back_when_head_is_refused(tmp_path):
    conn = mr.open_chunk(str(tmp_path / "h.db"))
    w = mr.ForecastWatcher(conn, ["KMDW"], http_head=lambda url: Resp(405),
                           http_get=lambda url: Resp(200, REAL) if "/19/text/" in url else Resp(404))
    t = int(datetime(2026, 10, 21, 20, 30, tzinfo=timezone.utc).timestamp())
    assert w.poll(t) == 1


def test_ladder_trigger_needs_after_fee_profit():
    legs = {i: {"bid": 17, "ask": 19} for i in range(6)}            # bids sum 102c: profitable only before fees
    assert mr.ladder_gap(legs)[1] > 100 and mr.ladder_profit(legs) < 0
    legs = {i: {"bid": 14, "ask": 15} for i in range(6)}            # asks sum 90c: profitable after fees
    assert mr.ladder_profit(legs) > 0


def test_stop_signal_ends_run_promptly(tmp_path):
    conn = mr.open_chunk(str(tmp_path / "x.db"))
    clock = {"t": float(T0)}
    rec = mr.Recorder(FakeClient(), conn, lambda: clock["t"], None)

    def sleep(s):
        clock["t"] += s
        if clock["t"] >= T0 + 90:
            rec.stop_requested = True
    rec.sleep = sleep
    stats = rec.run(30, stop_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert stats["minutes"] == 2 and clock["t"] < T0 + 92       # stopped within a second of the signal


def test_forecast_reaction_end_to_end(tmp_path):
    """A bulletin moves Chicago's p50 high from 41F to 45F. The 44-45F bracket's ask is 30c
    one minute later and its bid is 50c thirty minutes later: a slow market, caught by D."""
    import sqlite3
    path = tmp_path / "d.db"
    conn = mr.open_chunk(str(path))
    ev = "KXHIGHCHI-26DEC02"
    for k in range(6):
        conn.execute("INSERT INTO meta VALUES (?,?,?,?,?,?,?,?)", (f"{ev}-B{40 + 2 * k}.5", ev, "KXHIGHCHI",
                     "between", 40 + 2 * k, 41 + 2 * k, "2026-12-03T06:00:00Z", "KMDW"))
    seen = T0
    for m in range(-10, 40):
        ts = seen + m * 60
        conn.execute("INSERT INTO polls VALUES (?,?,?)", (ts, "KXHIGHCHI", 6))
    def q(ts, t, b, a):
        conn.execute("INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?)", (ts, t, b, a, 40.0, 25.0, 0, 0))
    new_b, old_b = f"{ev}-B44.5", f"{ev}-B40.5"
    q(seen - 600, new_b, 20, 22); q(seen + 60, new_b, 28, 30); q(seen + 60 + 1800, new_b, 50, 52)
    q(seen - 600, old_b, 40, 42); q(seen + 60, old_b, 38, 40); q(seen + 60 + 1800, old_b, 20, 22)
    conn.execute("INSERT INTO bulletins VALUES ('2026-12-02T07Z', ?, 0, '')", (seen - 6 * 3600,))
    conn.execute("INSERT INTO bulletins VALUES ('2026-12-02T13Z', ?, 0, '')", (seen,))
    conn.execute("INSERT INTO forecasts VALUES ('2026-12-02T07Z','KMDW','high','2026-12-02',39,40,41,42,43)")
    conn.execute("INSERT INTO forecasts VALUES ('2026-12-02T13Z','KMDW','high','2026-12-02',43,44,45,46,47)")
    conn.commit(); conn.close()
    os.makedirs(tmp_path / "ch")
    with open(path, "rb") as f, gzip.open(tmp_path / "ch" / "chunk_d.db.gz", "wb") as g:
        shutil.copyfileobj(f, g)
    db, n = rp.load(str(tmp_path / "ch"))
    opps, speed = rp.forecast_reaction_opps(rp.Data(db))
    legs = {o["leg"]: o for o in opps}
    assert set(legs) == {"toward", "away"}
    assert abs(legs["toward"]["per_contract"] - (50 - 30 - rp.fee_c(30) - rp.fee_c(50))) < 1e-9
    assert legs["toward"]["size"] == 25.0                      # resting at the ask (top-of-book size)
    assert abs(legs["away"]["per_contract"] - (38 - 22 - rp.fee_c(62) - rp.fee_c(78))) < 1e-9
    assert speed and all(0 <= s < 0.5 for s in speed)          # most of the move came after we could act
    text = rp.build(db, n, "final", {}, None, None)
    assert "D. SLOW REACTION TO A NEW FORECAST" in text


def test_slow_reaction_ignores_leftover_1c_bids(tmp_path):
    class OneCent(FakeClient):
        def get_markets_by_series(self, series, status="open"):
            ms = super().get_markets_by_series(series, status)
            for m in ms:
                m["yes_bid_dollars"] = "0.0100"
            return ms
    conn = mr.open_chunk(str(tmp_path / "o.db"))
    clock = {"t": float(T0)}
    mr.Recorder(OneCent(), conn, lambda: clock["t"], lambda s: clock.__setitem__("t", clock["t"] + max(s, 1))).run(
        5, stop_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
    conn.close()
    os.makedirs(tmp_path / "ch")
    with open(tmp_path / "o.db", "rb") as f, gzip.open(tmp_path / "ch" / "chunk_o.db.gz", "wb") as g:
        shutil.copyfileobj(f, g)
    db, _ = rp.load(str(tmp_path / "ch"))
    obs = [{"valid_ts": T0 - 6 * 3600, "t_group_c": 4.4, "max6_c": None, "tmpf": None},
           {"valid_ts": T0 - 360, "t_group_c": 6.7, "max6_c": None, "tmpf": None}]
    slow = rp.slow_reaction_opps(rp.Data(db), {"KNYC": obs}, {"KXHIGHNY": "America/New_York"})
    hit = [o for o in slow if o["ticker"].endswith("B40.5")]
    assert len(hit) == 1 and hit[0]["price"] == 99 and not hit[0]["caught"]     # 1c - 0.07c fee < 1c net


def test_watcher_tries_the_second_address_pattern(tmp_path):
    conn = mr.open_chunk(str(tmp_path / "t.db"))
    w = mr.ForecastWatcher(conn, ["KMDW"],
                           http_head=lambda url: Resp(200 if "/t19z/text/" in url else 404),
                           http_get=lambda url: Resp(200, REAL))
    t = int(datetime(2026, 10, 21, 20, 30, tzinfo=timezone.utc).timestamp())
    assert w.poll(t) == 1
    assert "/t19z/text/" in conn.execute("SELECT url FROM bulletins").fetchone()[0]


def test_direct_ask_field_used_when_no_bid_missing():
    q = mr.quote_from_market({"yes_bid_dollars": "0.4200", "yes_ask_dollars": "0.4600"})
    assert q["bid"] == 42 and q["ask"] == 46
    q = mr.quote_from_market({"yes_bid_dollars": "0.4200", "yes_ask_dollars": "0.4600", "no_bid_dollars": "0.5500"})
    assert q["ask"] == 45                                   # NO bid present: derived ask wins (same book)
    q = mr.quote_from_market({"yes_bid_size_fp": "10.00", "yes_ask_size_fp": "7.00", "yes_bid_dollars": "0.1"})
    assert q["bid_size"] == 10 and q["ask_size"] == 7       # Kalshi's documented size fields


def test_warmup_mode_scores_only_days_before_dec_1(tmp_path):
    import sqlite3
    path = tmp_path / "w.db"
    conn = mr.open_chunk(str(path))
    warm = int(datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc).timestamp())
    ev = "KXHIGHNY-26NOV27"
    for k in range(6):
        conn.execute("INSERT INTO meta VALUES (?,?,?,?,?,?,?,?)", (f"{ev}-B{40 + 2 * k}.5", ev, "KXHIGHNY",
                     "between", 40 + 2 * k, 41 + 2 * k, "2026-11-28T05:00:00Z", "KNYC"))
    for m in range(4):
        conn.execute("INSERT INTO polls VALUES (?,?,?)", (warm + m * 60, "KXHIGHNY", 6))
    for k in range(6):                                      # asks 6 x 15c = 90c for 3 minutes: a ladder gap
        conn.execute("INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?)", (warm, f"{ev}-B{40 + 2 * k}.5", 14, 15, 5, 5, 0, 0))
    conn.commit(); conn.close()
    os.makedirs(tmp_path / "ch")
    with open(path, "rb") as f, gzip.open(tmp_path / "ch" / "chunk_w.db.gz", "wb") as g:
        shutil.copyfileobj(f, g)
    db, n = rp.load(str(tmp_path / "ch"))
    warm_text = rp.build(db, n, "warmup", {}, None, None)
    assert "WARM-UP CHECK" in warm_text and "no verdict in warm-up mode" in warm_text
    assert "warmup seen     1   acted on     1" in warm_text            # the gap is found and acted on
    final_text = rp.build(db, n, "final", {}, None, None)
    assert "dev    seen     0" in final_text and "VERDICT: no type passes" in final_text   # never scored
