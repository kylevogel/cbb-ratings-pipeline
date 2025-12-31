from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
import pandas as pd
import requests


URL = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"


def _pick_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        cols = []
        for c in list(t.columns):
            if isinstance(c, tuple):
                c = " ".join([str(x) for x in c if str(x) != "nan"]).strip()
            cols.append(str(c).strip())
        low = [c.lower().strip() for c in cols]
        if "rank" in low and "team" in low:
            t.columns = cols
            return t
    raise RuntimeError("Could not find SOS table with columns including 'Rank' and 'Team'.")


def main() -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(URL, headers=headers, timeout=45)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = _pick_table(tables)

    cols_low = {c.lower().strip(): c for c in t.columns}
    team_col = cols_low.get("team")
    rank_col = cols_low.get("rank")
    if team_col is None or rank_col is None:
        raise RuntimeError("Found table but could not locate 'Team' and 'Rank' columns.")

    out = t[[team_col, rank_col]].copy()
    out.columns = ["Team", "SOS"]

    out["Team"] = out["Team"].astype(str).str.strip()
    out["SOS"] = pd.to_numeric(out["SOS"], errors="coerce")
    out = out.dropna(subset=["SOS"])
    out["SOS"] = out["SOS"].astype(int)

    out = out.sort_values("SOS").reset_index(drop=True)

    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    out.insert(0, "snapshot_date", now_et.strftime("%Y-%m-%d"))

    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)

    path = data_raw / "SOS_Rank.csv"
    out.to_csv(path, index=False)

    print(path.name)
    print(",".join(out.columns.tolist()))
    print(out.head(5).to_csv(index=False).strip())


if __name__ == "__main__":
    main()
