#!/usr/bin/env python3
"""
Scrape ESPN BPI rankings (full D1) from ESPN Brazil pages.
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
from io import StringIO

import pandas as pd
import requests


def _clean_team_name(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)

    if re.fullmatch(r"[A-Z]{2,5}", s):
        return s

    m = re.match(r"^(.*?)([A-Z]{2,5})$", s)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        if left and left != right:
            s = left

    m2 = re.match(r"^(.*)\s+([A-Z]{2,5})$", s)
    if m2:
        left = m2.group(1).strip()
        right = m2.group(2).strip()
        if left and left != right:
            s = left

    s = re.sub(r"\s*\(\d+\-\d+\)\s*$", "", s).strip()
    return s


def _pick_cols(df: pd.DataFrame):
    cols = list(df.columns)
    cols_l = [str(c).strip().lower() for c in cols]

    rank_col = None
    team_col = None

    for i, c in enumerate(cols_l):
        if c in ("rk", "rank"):
            rank_col = cols[i]
            break
    if rank_col is None:
        for i, c in enumerate(cols_l):
            if "rk" in c or "rank" in c:
                rank_col = cols[i]
                break
    if rank_col is None:
        rank_col = cols[0]

    for i, c in enumerate(cols_l):
        if c == "team" or "team" in c:
            team_col = cols[i]
            break
    if team_col is None and len(cols) >= 2:
        team_col = cols[1]
    if team_col is None:
        team_col = cols[0]

    return rank_col, team_col


def _parse_page(html: str) -> pd.DataFrame | None:
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None

    best = None
    best_score = -1

    for t in tables:
        if t is None or t.empty or len(t.columns) < 2:
            continue

        rank_col, team_col = _pick_cols(t)

        tmp = t[[rank_col, team_col]].copy()
        tmp.columns = ["bpi_rank", "team_bpi"]

        tmp["bpi_rank"] = pd.to_numeric(tmp["bpi_rank"], errors="coerce")
        tmp = tmp.dropna(subset=["bpi_rank"])
        tmp["bpi_rank"] = tmp["bpi_rank"].astype(int)

        tmp["team_bpi"] = tmp["team_bpi"].astype(str).map(_clean_team_name)
        tmp = tmp[tmp["team_bpi"].str.len() > 1]

        tmp = tmp[(tmp["bpi_rank"] >= 1) & (tmp["bpi_rank"] <= 400)]
        tmp = tmp.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")

        score = len(tmp)
        if score > best_score:
            best_score = score
            best = tmp

    if best is None or best.empty:
        return None
    return best


def scrape_bpi_rankings():
    base = "https://www.espn.com.br/basquete/universitario-masculino/bpi/_/vs-division/overview/pagina/{page}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    all_rows = []
    seen_ranks = set()

    for page in range(1, 15):
        url = base.format(page=page)
        print(f"Fetching BPI page {page}: {url}")

        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        df = _parse_page(resp.text)
        if df is None or df.empty:
            print(f"Could not parse BPI table on page {page}")
            break

        added = 0
        for _, r in df.iterrows():
            rk = int(r["bpi_rank"])
            tm = str(r["team_bpi"]).strip()
            if rk not in seen_ranks and tm:
                seen_ranks.add(rk)
                all_rows.append({"bpi_rank": rk, "team_bpi": tm})
                added += 1

        print(f"  Parsed {len(df)} rows, added {added} new ranks (total now {len(all_rows)})")

        if len(all_rows) >= 365:
            break
        if added == 0:
            break

    if not all_rows:
        return None

    out = pd.DataFrame(all_rows).drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")
    return out


def main():
    print("Fetching ESPN BPI rankings (full D1)...")
    df = None
    try:
        df = scrape_bpi_rankings()
    except Exception as e:
        print(f"Failed to fetch BPI rankings: {e}")

    os.makedirs("data_raw", exist_ok=True)

    if df is not None and not df.empty and len(df) >= 350:
        df.to_csv("data_raw/bpi_rankings.csv", index=False)
        print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")
        return

    pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv("data_raw/bpi_rankings.csv", index=False)
    print("Failed to fetch full BPI (expected ~365). Wrote empty data_raw/bpi_rankings.csv")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
