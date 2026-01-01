from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
import re
import difflib
import pandas as pd
import requests
import time

BASE_URL = "https://www.espn.com/mens-college-basketball/bpi/_/dir/asc/view/bpi/sort/bpi"

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[\.\(\)\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def _load_alias_map(root: Path) -> tuple[dict[str, str], list[str]]:
    alias_path = root / "data_raw" / "team_alias.csv"
    if not alias_path.exists():
        alias_path = root / "team_alias.csv"
    df = pd.read_csv(alias_path, dtype=str).fillna("")
    cols = [c for c in df.columns if c != "standard_name"]

    m: dict[str, str] = {}
    standards: list[str] = []

    for _, r in df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        standards.append(std)
        m[_norm(std)] = std

        for c in cols:
            val = str(r.get(c, "")).strip()
            if not val:
                continue
            for piece in [p.strip() for p in val.split("|") if p.strip()]:
                m[_norm(piece)] = std

    return m, standards

def _map_team(name: str, alias_map: dict[str, str], standards: list[str]) -> str:
    k = _norm(name)
    if k in alias_map:
        return alias_map[k]

    guess = difflib.get_close_matches(name, standards, n=1, cutoff=0.92)
    if guess:
        return guess[0]

    guess2 = difflib.get_close_matches(_norm(name), [_norm(x) for x in standards], n=1, cutoff=0.92)
    if guess2:
        inv = { _norm(x): x for x in standards }
        return inv.get(guess2[0], name)

    return name

def main():
    root = Path(__file__).resolve().parent
    out_path = root / "data_raw" / "BPI_Rank.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    alias_map, standards = _load_alias_map(root)

    headers = {"User-Agent": "Mozilla/5.0"}
    seen = set()
    rows = []

    for page in range(1, 60):
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

        time.sleep(0.4)

    if not rows:
        raise RuntimeError("Could not extract any BPI ranks from ESPN.")

    out = pd.DataFrame(rows)
    out["Team"] = out["Team"].map(lambda x: _map_team(str(x), alias_map, standards))
    out["BPI_Rank"] = pd.to_numeric(out["BPI_Rank"], errors="coerce")
    out = out.dropna(subset=["BPI_Rank"]).copy()
    out["BPI_Rank"] = out["BPI_Rank"].astype(int)

    out = out.sort_values("BPI_Rank")
    out = out.groupby("Team", as_index=False)["BPI_Rank"].min()
    out = out.sort_values("BPI_Rank").reset_index(drop=True)

    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    out.insert(0, "snapshot_date", now_et.strftime("%Y-%m-%d"))

    out.to_csv(out_path, index=False)

    unmatched = sorted(set(out.loc[~out["Team"].isin(standards), "Team"].tolist()))
    if unmatched:
        (root / "data_raw" / "unmatched_bpi.txt").write_text("\n".join(unmatched) + "\n", encoding="utf-8")

    print(f"Wrote {len(out)} rows to {out_path}")

if __name__ == "__main__":
    main()
