import time
from io import StringIO

import pandas as pd
import requests


OUT_PATH = "data_raw/bpi.csv"


def pick_bpi_table(tables):
    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        score += 2 if any("team" in c for c in cols) else 0
        score += 2 if any("bpi" in c for c in cols) else 0
        score += 1 if any(c in ["rk", "rank"] or "rk" in c for c in cols) else 0
        if score > best_score:
            best_score = score
            best = t
    return best if best_score >= 3 else None


def infer_cols(df):
    cols = list(df.columns)
    team_col = None
    rk_col = None
    bpi_col = None

    for c in cols:
        if "team" in str(c).lower():
            team_col = c
            break

    for c in cols:
        lc = str(c).strip().lower()
        if lc in ["rk", "rank"]:
            rk_col = c
            break
        if "rk" in lc and "bpi" in lc:
            rk_col = c

    for c in cols:
        lc = str(c).strip().lower()
        if lc == "bpi":
            bpi_col = c
            break
        if "bpi" in lc and "rk" not in lc:
            bpi_col = c

    return team_col, rk_col, bpi_col


def fetch_page(page):
    if page == 1:
        url = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi"
    else:
        url = f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{page}"

    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = pick_bpi_table(tables)
    if t is None:
        return pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])

    team_col, rk_col, bpi_col = infer_cols(t)
    if team_col is None or rk_col is None:
        return pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])

    out = pd.DataFrame()
    out["bpi_name"] = t[team_col].astype(str).str.strip()
    out["bpi_rank"] = pd.to_numeric(t[rk_col], errors="coerce").astype("Int64")
    out["bpi"] = pd.to_numeric(t[bpi_col], errors="coerce") if bpi_col is not None else pd.NA
    out = out.dropna(subset=["bpi_name", "bpi_rank"])
    out = out.drop_duplicates(subset=["bpi_rank"])
    return out.reset_index(drop=True)


def main():
    all_rows = []
    seen_ranks = set()

    for page in range(1, 40):
        df = fetch_page(page)
        if df.empty:
            break

        df = df[~df["bpi_rank"].isin(seen_ranks)]
        if df.empty:
            break

        for r in df["bpi_rank"].tolist():
            seen_ranks.add(int(r))

        all_rows.append(df)

        if len(seen_ranks) >= 365:
            break

        time.sleep(0.25)

    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])
    out = out.sort_values("bpi_rank", kind="stable")
    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(out)} rows -> {OUT_PATH}")


if __name__ == "__main__":
    main()
