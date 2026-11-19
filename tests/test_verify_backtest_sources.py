from datetime import date

from src.verify_backtest_sources import aws_bulletin_url, event_ticker, run_id_for


def test_aws_url_matches_noaa_archive_layout():
    assert aws_bulletin_url(date(2025, 11, 15)) == (
        "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20251115/13/text/blend_nbptx.t13z")


def test_aws_url_pads_run_hour():
    assert aws_bulletin_url(date(2025, 1, 2), 1).endswith("blend.20250102/01/text/blend_nbptx.t01z")


def test_run_id_matches_bot_format():
    assert run_id_for(date(2025, 11, 15)) == "2025-11-15T13Z"


def test_event_ticker_matches_live_tickers():
    # Live trades look like KXHIGHNY-26NOV18-B64.5; the event part is KXHIGHNY-26NOV18.
    assert event_ticker("KXHIGHNY", date(2026, 11, 18)) == "KXHIGHNY-26NOV18"
    assert event_ticker("HIGHNY", date(2025, 5, 6)) == "HIGHNY-25MAY06"
