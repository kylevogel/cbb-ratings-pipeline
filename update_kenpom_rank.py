from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
import os
import pandas as pd
import requests


URL = "https://kenpom.com/index.php?y=2026"


def main() -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }

    cookie = os.getenv("KENPOM_COOKIE", "").strip()
    if cookie:
        headers["Cookie"] = cookie

    try:
        r = requests.get(URL, headers=headers, timeout=45)
    except Exception as e:
        print(f"KenPom request failed: {e}")
        return

    if r.status_code in (401, 403):
        print(f"KenPom blocked the request (HTTP {r.status_code}). Skipping update.")
        return

    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))

    t = None
    for df in tables:
        cols = [str(c).strip().lower() for c in df.columns]
        if "rk" in cols or "rank" in cols:
            t = df
            break

    if t is None:
        print("Could not find KenPom table.")
        return

    cols = {str(c).strip().lower(): c for c in t.columns}

    team_col = None
    for k in ["team", "school"]:
        if k in cols:
            team_col = cols[k]
            break
    if team_col is None:
        team_col = t.columns[0]

    rank_col = None
    for k in ["rk", "rank"]:
        if k in cols:
            rank_col = cols[k]
            break

    wl_col = None
    for k in ["w-l", "wl"]:
        if k in cols:
            wl_col = cols[k]
            break

    if rank_col is None:
        print("Could not locate KenPom rank column.")
        return

    keep = [team_col, rank_col] + ([wl_col] if wl_col is not None else [])
    out = t[keep].copy()
    out.columns = ["Team", "KenPom_Rank"] + (["W-L"] if wl_col is not None else [])

    out["Team"] = out["Team"].astype(str).str.strip()
    out["KenPom_Rank"] = pd.to_numeric(out["KenPom_Rank"], errors="coerce")
    out = out.dropna(subset=["KenPom_Rank"])
    out["KenPom_Rank"] = out["KenPom_Rank"].astype(int)

    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    out.insert(0, "snapshot_date", now_et.strftime("%Y-%m-%d"))

    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)

    path = data_raw / "KenPom_Rank.csv"
    out.to_csv(path, index=False)

    print(path.name)
    print(",".join(out.columns.tolist()))
    print(out.head(5).to_csv(index=False).strip())


if __name__ == "__main__":
    main()
