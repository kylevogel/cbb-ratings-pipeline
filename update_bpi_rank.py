#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings (all pages).
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
from io import StringIO

import pandas as pd
import requests


BASE_URL = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{}"


def _pick_rank_team_table(tables):
    """
    ESPN returns multiple tables sometimes. Pick the one that looks like the BPI table:
    - Has a Team-ish column
    - Has a Rank/RK-ish column OR first column is numeric ranks
    """
    for df in tables:
        if df is None or df.empty:
            continue

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([str(x) for x in col if str(x) != "nan"]).strip() for col in df.columns]
        else:
            df.columns = [str(c).strip() for c in df.columns]

        cols_l = [c.lower() for c in df.columns]

        team_candidates = [c for c in df.columns if "team" in c.lower()]
        if not team_candidates:
            continue

        rank_candidates = [
            c for c in df.columns
            if c.lower() in {"rk", "rank", "rnk"} or "rk" == c.lower().strip() or c.lower().strip() == "rank"
        ]

        # If there is no explicit rank col, we can still accept if first col looks numeric
        if rank_candidates:
            return df, rank_candidates[0], team_candidates[0]

        first_col = df.columns[0]
        sample = df[first_col].astype(str).head(10).str.extract(r"(\d+)")[0]
        if sample.notna().sum() >= 6:  # enough numeric-looking entries
            return df, first_col, team_candidates[0]

    return None, None, None


def scrape_bpi_rankings_all_pages(max_pages=25):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    all_rows = []
    seen_ranks = set()

    for page in range(1, max_pages + 1):
        url = BASE_URL.format(page)
        print(f"Fetching BPI page {page}: {url}")

        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        # IMPORTANT: avoid pandas FutureWarning by wrapping with StringIO
        try:
            tables = pd.read_html(StringIO(resp.text))
        except ValueError:
            tables = []

        df, rank_col, team_col = _pick_rank_team_table(tables)
        if df is None:
            # If ESPN changes markup, this will trigger
            print(f"Could not locate BPI table on page {page}")
            break

        # Clean rank + team
        ranks = (
            df[rank_col]
            .astype(str)
            .str.extract(r"(\d+)")[0]
        )
        teams = df[team_col].astype(str).str.strip()

        page_rows = []
        for r, t in zip(ranks, teams):
            if pd.isna(r) or not str(r).isdigit():
                continue
            r_int = int(r)
            if r_int <= 0 or r_int > 400:
                continue
            if not t or t.lower() in {"nan", "none"}:
                continue

            # Deduplicate across pages
            if r_int in seen_ranks:
                continue

            seen_ranks.add(r_int)
            page_rows.append({"bpi_rank": r_int, "team_bpi": t})

        if not page_rows:
            # No new ranks found -> we're done
            print(f"No new BPI rows found on page {page}; stopping.")
            break

        all_rows.extend(page_rows)

        # If we already got basically all D1 teams, we can stop early
        if len(seen_ranks) >= 360:
            break

    if not all_rows:
        return None

    out = pd.DataFrame(all_rows).drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")
    return out


def main():
    print("Fetching ESPN BPI rankings (all pages)...")

    try:
        df = scrape_bpi_rankings_all_pages()
    except Exception as e:
        print(f"Failed to fetch BPI rankings: {e}")
        df = None

    os.makedirs("data_raw", exist_ok=True)

    if df is None or df.empty or len(df) < 300:
        # Overwrite stale file so you don't silently keep the old “top 50” forever
        pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv("data_raw/bpi_rankings.csv", index=False)
        print("Failed to fetch full BPI (expected ~365). Wrote empty data_raw/bpi_rankings.csv")
        raise SystemExit(1)

    df.to_csv("data_raw/bpi_rankings.csv", index=False)
    print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")


if __name__ == "__main__":
    main()
