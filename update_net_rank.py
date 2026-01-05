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
    url = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"

    html = fetch(url)
    tables = pd.read_html(StringIO(html))

    target = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "rank" in cols and ("school" in cols or "team" in cols):
            target = t
            break
    if target is None:
        raise RuntimeError("Could not find NET table with Rank + School/Team columns")

    col_map = {str(c).strip().lower(): c for c in target.columns}
    school_col = col_map.get("school") or col_map.get("team")

    out = pd.DataFrame(
        {
            "net_name": target[school_col].astype(str).str.strip(),
            "net": pd.to_numeric(target[col_map["rank"]], errors="coerce").astype("Int64"),
            "source_url": url,
            "updated_at_utc": now.isoformat().replace("+00:00", "Z"),
        }
    ).dropna(subset=["net_name", "net"])

    os.makedirs("data_raw", exist_ok=True)
    out.to_csv("data_raw/net.csv", index=False)
    print(f"Wrote {len(out)} rows -> data_raw/net.csv")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"update_net_rank failed: {e}", file=sys.stderr)
        raise
