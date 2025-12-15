from pathlib import Path
from datetime import date
from io import StringIO
import pandas as pd
import requests
import time

BASE_URL = "https://www.espn.com/mens-college-basketball/bpi/_/dir/asc/view/bpi/sort/bpi"

def flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in tup if x and "Unnamed" not in str(x)]).strip()
            for tup in df.columns
        ]
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
    return df

def has_team_table(df: pd.DataFrame) -> bool:
    cols = [str(c).strip().lower() for c in df.columns]
    return "team" in cols

def has_bpi_rk_table(df: pd.DataFrame) -> bool:
    cols = [str(c).strip().lower() for c in df.columns]
    return any(("bpi" in c and "rk" in c) for c in cols)

def extract_page(url: str, headers: dict) -> pd.DataFrame | None:
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        return None

    try:
        tables = pd.read_html(StringIO(r.text))
    except ValueError:
        return None

    tables = [flatten_cols(t) for t in tables]

    team_candidates = [t for t in tables if has_team_table(t)]
    rk_candidates = [t for t in tables if has_bpi_rk_table(t)]

    team_df = None
    rk_df = None

    for t in team_candidates:
        for rk in rk_candidates:
            if len(t) == len(rk):
                team_df = t.reset_index(drop=True)
                rk_df = rk.reset_index(drop=True)
                break
        if team_df is not None:
            break

    if team_df is None or rk_df is None:
        return None

    merged = pd.concat([team_df, rk_df], axis=1)

    team_col = next((c for c in merged.columns if str(c).strip().lower() == "team"), None)
    rank_col = next((c for c in merged.columns if ("bpi" in str(c).lower() and "rk" in str(c).lower())), None)
    if team_col is None or rank_col is None:
        return None

    out = pd.DataFrame({
        "Team": merged[team_col].astype(str).str.strip(),
        "BPI_Rank": pd.to_numeric(merged[rank_col], errors="coerce"),
    }).dropna(subset=["BPI_Rank"])

    return out

def main():
    root = Path(__file__).resolve().parent
    out_path = root / "data_raw" / "BPI_Rank.csv"

    headers = {"User-Agent": "Mozilla/5.0"}
    seen = set()
    rows = []

    # loop pages until no new teams are found
    for page in range(1, 50):  # 50 is a safe cap
        url = BASE_URL if page == 1 else f"{BASE_URL}/page/{page}"
        df = extract_page(url, headers)
        if df is None or df.empty:
            break

        new_this_page = 0
        for _, r in df.iterrows():
            team = r["Team"]
            if team not in seen:
                seen.add(team)
                rows.append(r)
                new_this_page += 1

        if new_this_page == 0:
            break

        time.sleep(0.4)  # be polite to ESPN

    if not rows:
        raise RuntimeError("Could not extract any BPI ranks from ESPN.")

    out = pd.DataFrame(rows)
    out.insert(0, "snapshot_date", date.today().isoformat())
    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows to {out_path}")

if __name__ == "__main__":
    main()
