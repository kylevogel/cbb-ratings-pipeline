from pathlib import Path
from datetime import date
import pandas as pd
import requests

URL = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"

def pick_net_table(tables):
    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        score = int("Rank" in cols) + int("School" in cols) + int("Record" in cols)
        if score > best_score:
            best_score = score
            best = t
    if best is None or best_score < 2:
        raise RuntimeError("Could not find NET table on the NCAA page.")
    return best

def main():
    root = Path(__file__).resolve().parent
    out_path = root / "data_raw" / "NET_Rank.csv"

    headers = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(URL, headers=headers, timeout=30).text
    tables = pd.read_html(html)
    df = pick_net_table(tables).copy()
    df.columns = [str(c).strip() for c in df.columns]

    out = pd.DataFrame({
        "snapshot_date": date.today().isoformat(),
        "Team": df["School"].astype(str),
        "NET_Rank": pd.to_numeric(df["Rank"], errors="coerce"),
    }).dropna(subset=["NET_Rank"])

    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows to {out_path}")

if __name__ == "__main__":
    main()
