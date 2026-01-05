import os
import sys
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests


def season_year(now_utc: datetime) -> int:
    y = now_utc.year
    return y + 1 if now_utc.month >= 9 else y


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
    yr = season_year(now)
    url = f"https://www.warrennolan.com/basketball/{yr}/sos-rpi-predict"

    html = fetch(url)
    tables = pd.read_html(StringIO(html))

    target = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "rank" in cols and "team" in cols:
            target = t
            break
    if target is None:
        raise RuntimeError("Could not find WarrenNolan SOS table with Rank + Team columns")

    col_map = {str(c).strip().lower(): c for c in target.columns}
    out = pd.DataFrame(
        {
            "sos_name": target[col_map["team"]].astype(str).str.strip(),
            "sos": pd.to_numeric(target[col_map["rank"]], errors="coerce").astype("Int64"),
            "source_url": url,
            "updated_at_utc": now.isoformat().replace("+00:00", "Z"),
        }
    ).dropna(subset=["sos_name", "sos"])

    os.makedirs("data_raw", exist_ok=True)
    out.to_csv("data_raw/sos.csv", index=False)
    print(f"Wrote {len(out)} rows -> data_raw/sos.csv")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"update_sos_rank failed: {e}", file=sys.stderr)
        raise
