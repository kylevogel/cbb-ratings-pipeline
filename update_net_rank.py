import pandas as pd
import requests
from pathlib import Path

URL = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"

def main():
    root = Path(__file__).resolve().parent
    out_path = root / "data_raw" / "NET_Rank.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    r = requests.get(
        URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    r.raise_for_status()

    tables = pd.read_html(r.text)
    df = None
    for t in tables:
        cols = {c.lower().strip(): c for c in t.columns}
        if "rank" in cols and "school" in cols and "record" in cols:
            df = t.rename(columns={cols["rank"]: "NET_Rank", cols["school"]: "Team", cols["record"]: "Record"}).copy()
            break

    if df is None or df.empty:
        raise RuntimeError("Could not find NET rankings table with Rank/School/Record columns.")

    df["Team"] = df["Team"].astype(str).str.strip()
    df["Record"] = df["Record"].astype(str).str.strip()
    df["NET_Rank"] = pd.to_numeric(df["NET_Rank"], errors="coerce")
    df = df.dropna(subset=["NET_Rank"])
    df["NET_Rank"] = df["NET_Rank"].astype(int)

    df = df[["Team", "NET_Rank", "Record"]].drop_duplicates(subset=["Team"], keep="first")
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
