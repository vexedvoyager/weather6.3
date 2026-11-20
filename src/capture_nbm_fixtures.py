"""
Stage 1 of the calendar-day high work (see "claude/backtest-plan.md" in the
Weather Bot Claude project): capture REAL NBM bulletin excerpts to build and
test the hourly-bulletin parser against - never an assumed layout.

For each requested run it downloads, from NOAA's AWS archive:
  * blend_nbhtx  - hourly bulletin (NBH): temperature for each hour, ~1 day ahead
  * blend_nbstx  - short-range bulletin (NBS): every 3 hours, ~3 days ahead
  * blend_nbptx  - probabilistic bulletin (NBP): what the bot already reads
and keeps ONLY the blocks for the stations in config.yaml (a few KB each),
written exactly as NOAA published them.

Read-only: no Kalshi access, no trading, no database. Run via Actions ->
"Capture NBM Fixtures"; download the "nbm-fixtures" artifact and upload it to Claude.
"""
import argparse
import os
import re
import sys
from datetime import datetime, timezone

import requests
import yaml

from src import nbm

AWS_BASE = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
PRODUCTS = {
    "nbhtx": "hourly (NBH)",
    "nbstx": "short-range (NBS)",
    "nbptx": "probabilistic (NBP)",
}
# Dec 7 2024: the worst winter warm miss in backtest Run 1 (Dec 8 highs: Chicago +9F,
# New York +6F above the forecast median) - the cold-front test case. Then one run from
# each later NBM version: V4.3 (Nov 2025) and V5.0 (current).
DEFAULT_RUNS = "2024-12-07T13,2025-11-15T13,2026-11-16T13"
_LABEL_RE = re.compile(r"^\s*([A-Z][A-Z0-9]{1,5})\b")


def bulletin_url(product: str, run_date: str, run_hour: int) -> str:
    """product 'nbhtx', run_date 'YYYY-MM-DD' -> NOAA AWS archive URL."""
    ymd = run_date.replace("-", "")
    return f"{AWS_BASE}/blend.{ymd}/{run_hour:02d}/text/blend_{product}.t{run_hour:02d}z"


def parse_runs(spec: str) -> list[tuple[str, int]]:
    """'2024-12-07T13,2025-11-15T13' -> [('2024-12-07', 13), ('2025-11-15', 13)]"""
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        d, h = part.upper().rstrip("Z").split("T")
        datetime.strptime(d, "%Y-%m-%d")          # validates the date
        out.append((d, int(h)))
    return out


def row_labels(block: list[str]) -> list[str]:
    """The element labels down the left side of a station block (TMP, TSD, ...),
    in order, skipping the header line."""
    labels = []
    for line in block[1:]:
        m = _LABEL_RE.match(line)
        if m and m.group(1) not in labels:
            labels.append(m.group(1))
    return labels


def extract(text: str, stations: list[str]) -> tuple[dict, list[str]]:
    """{station: block_lines} for the stations found, and the list not found."""
    found, missing = {}, []
    for st in stations:
        block = nbm.get_station_block(text, st)
        if block:
            found[st] = block
        else:
            missing.append(st)
    return found, missing


def load_stations(config_path: str = "config.yaml") -> list[tuple[str, str]]:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    cities = list(cfg["cities"]) + list(cfg.get("shadow_cities") or [])
    return [(c["name"], c["expected_station"]) for c in cities]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", default=DEFAULT_RUNS, help="e.g. 2024-12-07T13,2026-11-16T13")
    ap.add_argument("--out-dir", default="nbm_fixtures")
    args = ap.parse_args(argv)

    stations = load_stations()
    codes = [s for _, s in stations]
    os.makedirs(args.out_dir, exist_ok=True)
    summary = [f"NBM fixture capture - {datetime.now(timezone.utc):%Y-%m-%d %H:%MZ}",
               f"Stations: {', '.join(f'{n} {s}' for n, s in stations)}", ""]

    for run_date, run_hour in parse_runs(args.runs):
        for product, label in PRODUCTS.items():
            url = bulletin_url(product, run_date, run_hour)
            head = f"=== {label}  run {run_date}T{run_hour:02d}Z ==="
            try:
                r = requests.get(url, timeout=180)
            except requests.RequestException as e:
                summary += [head, f"  DOWNLOAD FAILED: {e}", ""]
                continue
            if r.status_code != 200:
                summary += [head, f"  HTTP {r.status_code} for {url}", ""]
                continue
            found, missing = extract(r.text, codes)
            name = f"{product}_{run_date}T{run_hour:02d}Z.txt"
            with open(os.path.join(args.out_dir, name), "w") as f:
                for st in codes:
                    if st in found:
                        f.write("\n".join(found[st]) + "\n")
            summary.append(head)
            summary.append(f"  file: {name}  ({len(r.text):,} chars downloaded; "
                           f"{len(found)}/{len(codes)} stations kept)")
            if missing:
                summary.append(f"  NOT FOUND: {', '.join(missing)}")
            sample = found.get("KNYC") or next(iter(found.values()), None)
            if sample:
                summary.append(f"  rows: {' '.join(row_labels(sample))}")
                summary.append("  first lines of " + sample[0].split()[0] + ":")
                summary += ["    | " + line for line in sample[:6]]
            summary.append("")

    text = "\n".join(summary) + "\n"
    print(text)
    with open(os.path.join(args.out_dir, "SUMMARY.txt"), "w") as f:
        f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
