from pathlib import Path
import re
import pandas as pd
import requests

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace("’", "'")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _variants(cell: str) -> list[str]:
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p]
    return parts if parts else []

def _load_alias_maps(root: Path):
    alias_path = root / "team_alias.csv"
    alias_df = pd.read_csv(alias_path, dtype=str, keep_default_na=False).fillna("")
    exact = {}
    norm = {}

    for _, row in alias_df.iterrows():
        std = (row.get("standard_name") or "").strip()
        if not std:
            continue

        cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
        for c in cols:
            val = row.get(c, "")
            for v in _variants(val):
                if v not in exact:
                    exact[v] = std
                nv = _norm(v)
                if nv and nv not in norm:
                    norm[nv] = std

    return exact, norm

def _clean_team_text(x: str) -> str:
    s = (x or "").strip()
    s = re.sub(r"\s+\(\d+\)\s*$", "", s)
    s = re.sub(r"\s+\d+\-\d+.*$", "", s)
    s = re.sub(r"\s+\d+\-\d+\s*$", "", s)
    return s.strip()

def _fetch_bpi_table() -> pd.DataFrame:
    url = "https://www.espn.com/mens-college-basketball/bpi"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("No tables found on ESPN BPI page")
    return tables[0]

def main():
    root = Path(__file__).resolve().parent
    exact_map, norm_map = _load_alias_maps(root)

    df = _fetch_bpi_table()
    cols = [str(c) for c in df.columns]

    team_col = None
    for c in cols:
        if "team" in c.lower():
            team_col = c
            break
    if team_col is None:
        team_col = cols[0]

    teams_raw = df[team_col].astype(str).tolist()
    snapshot_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    out_rows = []
    unmatched = []

    rank = 0
    for t in teams_raw:
        t2 = _clean_team_text(t)
        if not t2 or t2.lower() in {"nan", "none"}:
            continue
        rank += 1

        std = exact_map.get(t2)
        if std is None:
            std = norm_map.get(_norm(t2))

        if std is None:
            unmatched.append({"snapshot_date": snapshot_date, "Team": t2, "BPI": rank})
            continue

        out_rows.append({"snapshot_date": snapshot_date, "Team": std, "BPI": rank})

    out_df = pd.DataFrame(out_rows)
    out_path = root / "data_raw" / "BPI_Rank.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)

    unmatched_df = pd.DataFrame(unmatched)
    unmatched_path = root / "unmatched_bpi_teams.csv"
    unmatched_df.to_csv(unmatched_path, index=False)

    print("BPI_Rank.csv")
    print(unmatched_path.name)
    if not unmatched_df.empty:
        print(unmatched_df.head(25).to_csv(index=False).strip())

if __name__ == "__main__":
    main()
