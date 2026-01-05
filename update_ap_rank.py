from io import StringIO

import pandas as pd
import requests


OUT_PATH = "data_raw/ap.csv"


def pick_ap_table(tables):
    best = None
    best_count = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if not (any(c in ["rk", "rank"] for c in cols) and any("team" in c for c in cols)):
            continue
        rk_col = None
        team_col = None
        for c in t.columns:
            if str(c).strip().lower() in ["rk", "rank"]:
                rk_col = c
            if "team" in str(c).strip().lower():
                team_col = c
        if rk_col is None or team_col is None:
            continue
        rks = pd.to_numeric(t[rk_col], errors="coerce").dropna()
        count_1_25 = int(((rks >= 1) & (rks <= 25)).sum())
        if count_1_25 > best_count:
            best_count = count_1_25
            best = t
    return best


def main():
    url = "https://www.espn.com/mens-college-basketball/rankings"
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = pick_ap_table(tables)

    if t is None:
        out = pd.DataFrame(columns=["ap_name", "ap_rank"])
    else:
        rk_col = None
        team_col = None
        for c in t.columns:
            if str(c).strip().lower() in ["rk", "rank"]:
                rk_col = c
            if "team" in str(c).strip().lower():
                team_col = c

        out = pd.DataFrame()
        out["ap_rank"] = pd.to_numeric(t[rk_col], errors="coerce").astype("Int64")
        out["ap_name"] = t[team_col].astype(str).str.strip()
        out = out.dropna(subset=["ap_rank", "ap_name"])
        out = out[out["ap_rank"].between(1, 25)]
        out = out.drop_duplicates(subset=["ap_rank"]).sort_values("ap_rank", kind="stable")

    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(out)} rows -> {OUT_PATH}")


if __name__ == "__main__":
    main()
