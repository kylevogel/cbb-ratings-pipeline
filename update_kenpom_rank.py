from pathlib import Path
from datetime import date
from io import StringIO
import pandas as pd
import requests

URL = "https://kenpom.com/"

def flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in tup if x and "Unnamed" not in str(x)]).strip()
            for tup in df.columns
        ]
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
    return df

def normalize_colname(name: str) -> str:
    # If it looks like "Team Team Team" or "Rk Rk Rk", collapse to the first token.
    parts = name.split()
    if len(parts) >= 2 and all(p == parts[0] for p in parts):
        return parts[0]
    # Also handle "Team Team Team Team Team" type cases where it is mostly repeats
    if len(parts) >= 3 and parts.count(parts[0]) >= len(parts) - 1:
        return parts[0]
    return name

def find_team_and_rank_cols(df: pd.DataFrame):
    norm_map = {}
    for c in df.columns:
        norm = normalize_colname(str(c)).strip().lower()
        norm_map.setdefault(norm, c)

    # Team column
    team_col = None
    for norm, c in norm_map.items():
        if norm == "team" or norm.startswith("team"):
            team_col = c
            break

    # Rank column
    rk_col = None
    for norm, c in norm_map.items():
        if norm in {"rk", "rank"} or norm.startswith("rk"):
            rk_col = c
            break

    return team_col, rk_col

def pick_kenpom_table(tables):
    best = None
    best_rows = -1
    for t in tables:
        t = flatten_cols(t)
        team_col, rk_col = find_team_and_rank_cols(t)
        if team_col is not None and rk_col is not None:
            # prefer the big full ratings table
            if len(t) > best_rows:
                best = t
                best_rows = len(t)
    if best is None:
        raise RuntimeError(f"Could not find a KenPom table with Team and Rk. Saw: {[tbl.columns.tolist() for tbl in tables]}")
    return best

def main():
    root = Path(__file__).resolve().parent
    out_path = root / "data_raw" / "KenPom_Rank.csv"
    out_path.parent.mkdir(exist_ok=True)

    headers = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(URL, headers=headers, timeout=30).text

    tables = pd.read_html(StringIO(html))
    df = pick_kenpom_table(tables)

    team_col, rk_col = find_team_and_rank_cols(df)
    if team_col is None or rk_col is None:
        raise RuntimeError(f"Could not identify Team or Rk column. Columns: {df.columns.tolist()}")

    out = pd.DataFrame({
        "snapshot_date": date.today().isoformat(),
        "Team": df[team_col].astype(str).str.strip(),
        "KenPom_Rank": pd.to_numeric(df[rk_col], errors="coerce"),
    }).dropna(subset=["KenPom_Rank"])

    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows to {out_path}")

if __name__ == "__main__":
    main()
