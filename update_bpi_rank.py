#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
import requests
import pandas as pd


BASE_URL = "https://www.espn.com/mens-college-basketball/bpi"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _page_url(page: int) -> str:
    if page <= 1:
        return BASE_URL
    return f"{BASE_URL}/_/view/bpi/page/{page}"


def _pick_bpi_table(tables):
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        has_team = any("team" in c for c in cols)
        has_bpi = any("bpi" in c for c in cols)
        if has_team and has_bpi and len(t) > 0:
            return t
    return None


def _find_col(df, candidates):
    cols = list(df.columns)
    cols_l = [str(c).strip().lower() for c in cols]
    for cand in candidates:
        for i, c in enumerate(cols_l):
            if c == cand or c.replace(" ", "") == cand.replace(" ", ""):
                return cols[i]
    for cand in candidates:
        for i, c in enumerate(cols_l):
            if cand in c:
                return cols[i]
    return None


def scrape_bpi_rankings(max_pages: int = 25) -> pd.DataFrame | None:
    headers = {"User-Agent": UA}

    rows = []
    seen_team = set()
    seen_rank = set()

    for page in range(1, max_pages + 1):
        url = _page_url(page)

        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"Request failed on page {page}: {e}")
            break

        try:
            tables = pd.read_html(r.text)
        except Exception as e:
            print(f"read_html failed on page {page}: {e}")
            break

        t = _pick_bpi_table(tables)
        if t is None or t.empty:
            break

        rank_col = _find_col(t, ["rk", "rank", "#", "rnk"])
        team_col = _find_col(t, ["team"])

        if team_col is None:
            print(f"Could not find Team column on page {page}")
            break

        new_count = 0
        for _, rec in t.iterrows():
            team = rec.get(team_col)
            if pd.isna(team):
                continue

            team = str(team).strip()
            if not team or team.lower() == "team":
                continue

            rank_val = None
            if rank_col is not None and pd.notna(rec.get(rank_col)):
                s = re.sub(r"[^\d]", "", str(rec.get(rank_col)))
                if s.isdigit():
                    rank_val = int(s)

            if rank_val is None:
                continue

            if team in seen_team:
                continue
            if rank_val in seen_rank:
                continue

            rows.append({"bpi_rank": rank_val, "team_bpi": team})
            seen_team.add(team)
            seen_rank.add(rank_val)
            new_count += 1

        if new_count == 0:
            break

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df = df.dropna()
    df["bpi_rank"] = pd.to_numeric(df["bpi_rank"], errors="coerce")
    df = df.dropna()
    df["bpi_rank"] = df["bpi_rank"].astype(int)
    df = df.sort_values("bpi_rank").drop_duplicates(subset=["bpi_rank"])
    return df


def main():
    print("Fetching ESPN BPI rankings (all pages)...")
    df = scrape_bpi_rankings()

    if df is not None and not df.empty:
        os.makedirs("data_raw", exist_ok=True)
        df.to_csv("data_raw/bpi_rankings.csv", index=False)
        print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")
        print("Top 10:")
        print(df.head(10).to_string(index=False))
        print("Bottom 10:")
        print(df.tail(10).to_string(index=False))
    else:
        print("Failed to fetch BPI rankings")


if __name__ == "__main__":
    main()
