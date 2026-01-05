import os
import sys
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests


def fetch(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def main() -> int:
    now = datetime.now(timezone.utc)
    url = "https://www.espn.com/mens-college-basketball/bpi"

    html = fetch(url)
    tables = pd.read_html(StringIO(html))

    target = None
    for t in tables:
        cols = [str(c).strip().upper() for c in t.columns]
        if "TEAM" in cols and any("BPI" == c or "BPI" in c for c in cols):
            target = t
            break
    if target is None:
        raise RuntimeError("Could not find BPI table with TEAM and BPI columns")

    upper_map = {str(c).strip().upper(): c for c in target.columns}
    team_col = upper_map["TEAM"]

    bpi_rank_col = None
    for c in target.columns:
        s = str(c).strip().upper().replace(" ", "")
        if s in ["RK", "RANK", "BPIRK", "BPIRANK", "BPI_RK", "BPI_RANK"]:
            bpi_rank_col = c
            break
    if bpi_rank_col is None:
        for c in target.columns:
            s = str(c).strip().upper()
            if "RK" in s and ("BPI" in s or s == "RK"):
                bpi_rank_col = c
                break
    if bpi_rank_col is None:
        raise RuntimeError("Could not locate a rank column on ESPN BPI table")

    out = pd.DataFrame(
        {
            "bpi_name": target[team_col].astype(str).str.strip(),
            "bpi": pd.to_numeric(target[bpi_rank_col], errors="coerce").astype("Int64"),
            "source_url": url,
            "updated_at_utc": now.isoformat().replace("+00:00", "Z"),
        }
    ).dropna(subset=["bpi_name", "bpi"])

    os.makedirs("data_raw", exist_ok=True)
    out.to_csv("data_raw/bpi.csv", index=False)
    print(f"Wrote {len(out)} rows -> data_raw/bpi.csv")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"update_bpi_rank failed: {e}", file=sys.stderr)
        raise
