from datetime import date, datetime, timezone

from src import check_incentives as ci

NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)


def prog(ticker, start, end, reward_cc, typ="liquidity", target="300.00", **kw):
    return {"id": ticker + start, "market_ticker": ticker, "incentive_type": typ,
            "start_date": start, "end_date": end, "period_reward": reward_cc,
            "target_size_fp": target, "paid_out": False, **kw}


def test_units_and_parsing():
    p = prog("KXHIGHNY-26NOV20-B50.5", "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z", 250000,
             max_reward_per_account=50000)
    assert ci.pool_dollars(p) == 25.0            # centi-cents -> dollars
    assert ci.cap_dollars(p) == 5.0
    assert ci.target_size(p) == 300.0
    assert ci.series_of(p["market_ticker"]) == "KXHIGHNY"


def test_weather_classification_prefers_series_list():
    assert ci.is_weather("KXHIGHNY-26NOV20-B50.5", {"KXHIGHNY"})
    assert not ci.is_weather("KXBTC-26NOV20-T100000", {"KXHIGHNY"})
    assert ci.is_weather("KXLOWTCHI-26NOV20-B30.5", set())        # prefix fallback
    assert ci.kind_of("KXLOWTCHI") == "daily low" and ci.kind_of("KXHIGHMIA") == "daily high"


def test_daily_allocation_splits_across_midnight():
    p = prog("KXHIGHNY-X", "2026-11-20T18:00:00Z", "2026-11-21T06:00:00Z", 120000)   # $12 over 12h
    a = ci.daily_allocation(p)
    assert abs(a[date(2026, 11, 20)] - 6.0) < 1e-9 and abs(a[date(2026, 11, 21)] - 6.0) < 1e-9
    assert ci.daily_allocation(prog("X", "2026-11-21T00:00:00Z", "2026-11-20T00:00:00Z", 1)) == {}


def test_window_excludes_today_and_old_days():
    ps = [prog("KXHIGHNY-A", "2026-11-23T00:00:00Z", "2026-11-24T00:00:00Z", 600000),   # $60 yesterday
          prog("KXHIGHNY-B", "2026-11-24T00:00:00Z", "2026-11-25T00:00:00Z", 999999),   # today: excluded
          prog("KXHIGHNY-C", "2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z", 999999)]   # too old
    s = ci.window_summary(ps, NOW, days=60)
    assert abs(s["total"] - 60.0) < 1e-9 and s["days_with_rewards"] == 1
    assert abs(s["avg_per_day"] - 1.0) < 1e-9


def test_verdict_thresholds():
    assert ci.verdict({"avg_per_day": 60, "days_with_rewards": 45}) == "PASS"
    assert ci.verdict({"avg_per_day": 60, "days_with_rewards": 10}) == "MARGINAL"   # bursty: not PASS
    assert ci.verdict({"avg_per_day": 12, "days_with_rewards": 60}) == "MARGINAL"
    assert ci.verdict({"avg_per_day": 3, "days_with_rewards": 60}) == "STOP"


def test_book_depth_from_orderbook_fp():
    d = ci.book_depth({"yes_dollars": [["0.40", "100.00"], ["0.45", "50.00"]],
                       "no_dollars": [["0.50", "20.00"], ["0.52", "30.00"]]})
    assert d["yes_contracts"] == 150 and d["no_contracts"] == 50
    assert d["yes_bid"] == 0.45 and d["yes_ask"] == 0.48
    assert ci.book_depth({})["yes_ask"] is None


class FakeClient:
    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def _request(self, method, path, params=None, json_body=None):
        self.calls.append(dict(params or {}))
        return self.pages[len(self.calls) - 1]

    def get_orderbook(self, ticker):
        return {"yes_dollars": [["0.30", "500.00"]], "no_dollars": [["0.60", "400.00"]]}


def test_pagination_follows_cursor():
    c = FakeClient([{"incentive_programs": [{"id": 1}], "next_cursor": "abc"},
                    {"incentive_programs": [{"id": 2}], "next_cursor": ""}])
    assert [p["id"] for p in ci.fetch_all_programs(c)] == [1, 2]
    assert c.calls[0]["status"] == "all" and c.calls[1]["cursor"] == "abc"


def test_report_end_to_end():
    ps = []
    for i in range(50):                                   # $80/day on 50 recent days -> PASS
        d = date.fromordinal(NOW.date().toordinal() - 1 - i)
        ps.append(prog(f"KXHIGHNY-{i}", f"{d}T00:00:00Z", f"{d}T23:59:59Z", 800000))
    ps.append(prog("KXHIGHNY-LIVE", "2026-11-24T00:00:00Z", "2026-11-25T00:00:00Z", 100000))
    ps.append(prog("KXBTC-1", "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z", 5000000))
    ps.append(prog("KXHIGHNY-V", "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z", 5000, typ="volume"))
    report, v = ci.build_report(ps, {"KXHIGHNY"}, NOW, FakeClient([]))
    assert v == "PASS"
    assert "KXHIGHNY-LIVE" in report and "<- weather" in report and "VERDICT" in report
    _, v2 = ci.build_report(ps[-2:], {"KXHIGHNY"}, NOW)  # no weather liquidity at all
    assert v2 == "STOP"
