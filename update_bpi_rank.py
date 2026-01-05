#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
import requests
import pandas as pd


def _clean_team_name(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s*\(\d+-\d+\)\s*$", "", s)
    s = re.sub(r"^\d+\s+", "", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def scrape_bpi_rankings():
    url = "https://www.espn.com/mens-college-basketball/bpi"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    tables = pd.read_html(resp.text)

    best = None
    best_score = -1

    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        cols_lower = [c.lower() for c in cols]

        has_bpi_rk = any("bpi rk" in c for c in cols_lower)
        has_team = any(c == "team" or "team" in c for c in cols_lower)

        if not has_bpi_rk:
            continue

        score = 0
        if has_team:
            score += 2
        if any("conf" in c for c in cols_lower):
            score += 1
        if len(t) >= 200:
            score += 2

        if score > best_score:
            best = t
            best_score = score

    if best is None or best.empty:
        return None

    df = best.copy()
    df.columns = [str(c).strip() for c in df.columns]
    cols_lower_map = {str(c).strip().lower(): str(c).strip() for c in df.columns}

    bpi_rk_col = None
    for k, v in cols_lower_map.items():
        if "bpi rk" in k:
            bpi_rk_col = v
            break

    team_col = None
    for k, v in cols_lower_map.items():
        if k == "team" or ("team" in k and team_col is None):
            team_col = v

    if bpi_rk_col is None:
        return None

    if team_col is None:
        team_col = df.columns[0]

    out = df[[team_col, bpi_rk_col]].copy()
    out.columns = ["team_bpi", "bpi_rank"]

    out["bpi_rank"] = pd.to_numeric(out["bpi_rank"], errors="coerce")
    out = out.dropna(subset=["bpi_rank"])
    out["bpi_rank"] = out["bpi_rank"].astype(int)

    out["team_bpi"] = out["team_bpi"].apply(_clean_team_name)
    out = out.dropna(subset=["team_bpi"])
    out = out[out["team_bpi"].astype(str).str.len() > 1]

    out = out.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)

    if len(out) < 200:
        return None

    return out


def main():
    print("Fetching ESPN BPI rankings...")
    df = None
    try:
        df = scrape_bpi_rankings()
    except Exception as e:
        print(f"Error scraping BPI: {e}")

    if df is not None and not df.empty:
        os.makedirs("data_raw", exist_ok=True)
        df.to_csv("data_raw/bpi_rankings.csv", index=False)
        print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")
    else:
        print("Failed to fetch BPI rankings")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
