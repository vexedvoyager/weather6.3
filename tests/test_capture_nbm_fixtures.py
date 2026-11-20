from pathlib import Path

import pytest

from src.capture_nbm_fixtures import (DEFAULT_RUNS, bulletin_url, extract, parse_runs,
                                      row_labels)

FIXTURE = Path(__file__).parent / "fixtures" / "nbm_real_2026-10-21T19Z.txt"


def test_urls_follow_noaa_archive_layout():
    assert bulletin_url("nbhtx", "2024-12-07", 13) == (
        "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20241207/13/text/blend_nbhtx.t13z")
    assert bulletin_url("nbstx", "2026-01-02", 1).endswith("blend.20260102/01/text/blend_nbstx.t01z")


def test_parse_runs_accepts_the_default_and_rejects_bad_dates():
    assert parse_runs(DEFAULT_RUNS) == [("2024-12-07", 13), ("2025-11-15", 13), ("2026-11-16", 13)]
    assert parse_runs("2025-01-05T19Z") == [("2025-01-05", 19)]
    with pytest.raises(ValueError):
        parse_runs("2025-02-30T13")


def test_extract_keeps_only_requested_stations_exactly_as_published():
    text = FIXTURE.read_text()
    found, missing = extract(text, ["KNYC", "KLAX", "KDEN"])
    assert set(found) == {"KNYC", "KLAX"} and missing == ["KDEN"]
    assert found["KNYC"][0].strip().startswith("KNYC") and "NBM V5.0" in found["KNYC"][0]
    assert all(line in text.splitlines() for line in found["KNYC"])     # unaltered lines
    assert not any(line.strip().startswith("KLAX") for line in found["KNYC"][1:])


def test_row_labels_lists_the_elements_in_a_block():
    found, _ = extract(FIXTURE.read_text(), ["KNYC"])
    labels = row_labels(found["KNYC"])
    assert "UTC" in labels and "TXNP5" in labels
    assert labels.index("UTC") < labels.index("TXNP5")
