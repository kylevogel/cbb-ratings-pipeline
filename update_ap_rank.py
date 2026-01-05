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
    url = "https://www.espn.com/mens-college-basketball/rankings"

    html = fetch(url)
    tables = pd.read_html(StringIO(html))

    target = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        score += 2 if ("rk" in cols or "rank" in cols) else 0
        score += 2 if ("team" in cols or "school" in cols) else 0
        score += 1 if "record" in cols else 0
        if score > best_score:
            best_score = score
            target = t
    if target is None or best_score < 3:
        raise RuntimeError("Could not find AP-style table on ESPN rankings page")

    col_map = {str(c).strip().lower(): c for c in target.columns}
    rk_col = col_map.get("rk") or col_map.get("rank")
    team_col = col_map.get("team") or col_map.get("school")

    out = pd.DataFrame(
        {
            "ap_name": target[team_col].astype(str).str.strip(),
            "ap": pd.to_numeric(target[rk_col], errors="coerce").astype("Int64"),
            "source_url": url,
            "updated_at_utc": now.isoformat().replace("+00:00", "Z"),
        }
    ).dropna(subset=["ap_name", "ap"])

    os.makedirs("data_raw", exist_ok=True)
    out.to_csv("data_raw/ap.csv", index=False)
    print(f"Wrote {len(out)} rows -> data_raw/ap.csv")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"update_ap_rank failed: {e}", file=sys.stderr)
        raise
