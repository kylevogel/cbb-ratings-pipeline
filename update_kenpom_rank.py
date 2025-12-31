from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO

import pandas as pd
import requests


URL = "https://kenpom.com/index.php?y=2026"


def _pick_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        cols = []
        for c in list(t.columns):
            if isinstance(c, tuple):
                c = " ".join(str(x) for x in c if str(x) != "nan").strip()
            cols.append(str(c).strip())
        low = [c.lower().strip() for c in cols]

        has_team = any("team" in c for c in low)
        has_rank = any(c in {"rk", "rank"} or c.startswith("rk ") or c.endswith(" rk") for c in low) or any("rk" == c for c in low)
        has_wl = any("w-l" in c or "wl" == c.replace("-", "").replace(" ", "") for c in low)

        if has_team and (has_rank or "rk" in low or "rank" in low) and has_wl:
            t.columns = cols
            return t

    for t in tables:
        cols = []
        for c in list(t.columns):
            if isinstance(c, tuple):
                c = " ".join(str(x) for x in c if str(x) != "nan").strip()
            cols.append(str(c).strip())
        low = [c.lower().strip() for c in cols]
        if any("team" in c for c in low) and any("w-l" in c for c in low):
            t.columns = cols
            return t

    raise RuntimeError("Could not find KenPom table with Team and W-L.")


def main() -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(URL, headers=headers, timeout=45)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = _pick_table(tables)

    cols_low = {str(c).lower().strip(): c for c in t.columns}

    team_col = None
    for k, v in cols_low.items():
        if "team" in k:
            team_col = v
            break
    if team_col is None:
        raise RuntimeError("Could not locate Team column.")

    rank_col = None
    for k in ["rk", "rank"]:
        if k in cols_low:
            rank_col = cols_low[k]
            break
    if rank_col is None:
        for c in t.columns:
            cl = str(c).lower().strip()
            if cl in {"rk", "rank"}:
                rank_col = c
                break
    if rank_col is None:
        raise RuntimeError("Could not locate rank column.")

    wl_col = None
    for c in t.columns:
        cl = str(c).lower().strip()
        if "w-l" in cl or cl.replace("-", "").replace(" ", "") == "wl":
            wl_col = c
            break
    if wl_col is None:
        raise RuntimeError("Could not locate W-L column.")

    out = t[[team_col, rank_col, wl_col]].copy()
    out.columns = ["Team", "KenPom", "Record"]

    out["Team"] = out["Team"].astype(str).str.strip()
    out["KenPom"] = pd.to_numeric(out["KenPom"], errors="coerce")
    out = out.dropna(subset=["KenPom"])
    out["KenPom"] = out["KenPom"].astype(int)

    out["Record"] = out["Record"].astype(str).str.strip()

    out = out.sort_values("KenPom").reset_index(drop=True)

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
