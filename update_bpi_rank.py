import re
import sys
import time
from typing import Optional, Tuple, List

import pandas as pd
import requests

OUT_PATH = "data_raw/bpi.csv"


def _clean_team(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s)).strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s).strip()
    s = re.sub(r"^[A-Z]{2,6}\s+", "", s).strip()
    return s


def _pick_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("team" in c for c in cols) and any("bpi" in c for c in cols):
            return t
    return None


def _infer_cols(df: pd.DataFrame) -> Tuple[str, str, Optional[str]]:
    cols = list(df.columns)

    team_col = None
    for c in cols:
        if "team" in str(c).strip().lower():
            team_col = c
            break
    if team_col is None:
        raise RuntimeError("Could not identify TEAM column in BPI table")

    rank_col = None
    for c in cols:
        lc = str(c).strip().lower()
        if lc in {"rk", "rank"}:
            rank_col = c
            break
        if "bpi" in lc and "rk" in lc:
            rank_col = c
            break

    bpi_col = None
    for c in cols:
        if str(c).strip().lower() == "bpi":
            bpi_col = c
            break
        if "bpi" in str(c).strip().lower() and "rk" not in str(c).strip().lower():
            bpi_col = c

    if rank_col is None:
        raise RuntimeError("Could not identify BPI rank column in BPI table")

    return team_col, rank_col, bpi_col


def _fetch_page(page: int) -> pd.DataFrame:
    if page == 1:
        url = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi"
    else:
        url = f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{page}"

    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    tables = pd.read_html(r.text)
    t = _pick_table(tables)
    if t is None:
        return pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])

    team_col, rank_col, bpi_col = _infer_cols(t)

    out = pd.DataFrame()
    out["bpi_name"] = t[team_col].map(_clean_team)
    out["bpi_rank"] = pd.to_numeric(t[rank_col], errors="coerce").astype("Int64")
    if bpi_col is not None:
        out["bpi"] = pd.to_numeric(t[bpi_col], errors="coerce")
    else:
        out["bpi"] = pd.NA

    out = out.dropna(subset=["bpi_name"]).drop_duplicates(subset=["bpi_name"])
    out = out[out["bpi_name"].astype(str).str.len() > 0]
    return out.reset_index(drop=True)


def main() -> int:
    all_rows = []
    seen = set()

    for page in range(1, 30):
        try:
            df = _fetch_page(page)
        except Exception as e:
            print(f"update_bpi_rank warning on page {page}: {e}", file=sys.stderr)
            break

        if df.empty:
            break

        df = df[~df["bpi_name"].isin(seen)]
        if df.empty:
            break

        for n in df["bpi_name"].tolist():
            seen.add(n)

        all_rows.append(df)

        time.sleep(0.3)

        if len(seen) >= 365:
            break

    if all_rows:
        out = pd.concat(all_rows, ignore_index=True)
        out = out.dropna(subset=["bpi_rank"])
        out = out.sort_values("bpi_rank", kind="stable")
    else:
        out = pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])

    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(out)} rows -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
