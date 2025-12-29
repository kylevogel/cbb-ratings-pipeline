import pandas as pd
import requests
from datetime import datetime
from pathlib import Path

AP_URL = "https://www.ncaa.com/rankings/basketball-men/d1/associated-press"

def main():
    r = requests.get(AP_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("No tables found on NCAA AP rankings page")

    df = tables[0].copy()
    df.columns = [str(c).strip() for c in df.columns]

    rank_col = None
    team_col = None
    for c in df.columns:
        lc = c.lower()
        if rank_col is None and "rank" in lc:
            rank_col = c
        if team_col is None and (lc in {"school", "team"} or "school" in lc):
            team_col = c

    if rank_col is None:
        rank_col = df.columns[0]
    if team_col is None:
        team_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

    out = df[[rank_col, team_col]].rename(columns={rank_col: "AP_Rank", team_col: "Team"})

    out["Team"] = (
        out["Team"].astype(str)
        .str.replace(r"\s*\(\d+\)\s*$", "", regex=True)
        .str.strip()
    )

    out["AP_Rank"] = (
        out["AP_Rank"].astype(str)
        .str.extract(r"(\d+)", expand=False)
    )
    out["AP_Rank"] = pd.to_numeric(out["AP_Rank"], errors="coerce").astype("Int64")

    out_dir = Path("data_raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "AP_Rank.csv"
    out.to_csv(out_path, index=False)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Wrote {out_path} ({len(out)} rows) at {stamp}")

if __name__ == "__main__":
    main()
