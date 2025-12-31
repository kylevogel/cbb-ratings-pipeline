import pandas as pd
import requests
from pathlib import Path

URL = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"

def _pick_table(tables):
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "team" in cols and "rank" in cols:
            return t
    return None

def main():
    r = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    t = _pick_table(tables)
    if t is None:
        raise RuntimeError("Could not find a table with Team and Rank on the WarrenNolan page")

    df = t.copy()
    df.columns = [str(c).strip() for c in df.columns]

    df = df[["Team", "Rank"]].rename(columns={"Rank": "SOS"}).copy()
    df["snapshot_date"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    df["SOS"] = pd.to_numeric(df["SOS"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["SOS"])

    out_path = Path("data_raw") / "SOS_Rank.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df[["snapshot_date", "Team", "SOS"]].to_csv(out_path, index=False)

if __name__ == "__main__":
    main()
