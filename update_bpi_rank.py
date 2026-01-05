#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


def _current_season_end_year_utc() -> int:
    now = datetime.utcnow()
    return now.year + 1 if now.month >= 10 else now.year


def _pick_bpi_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(" ", strip=True).upper() for th in table.find_all("th")]
        if any("BPI RK" in h for h in headers) or any("BPI" == h for h in headers):
            return table
    return None


def scrape_bpi_rankings(season_end_year: int | None = None) -> pd.DataFrame | None:
    season = season_end_year or _current_season_end_year_utc()
    url = f"https://www.espn.com/mens-college-basketball/bpi/_/season/{season}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    table = _pick_bpi_table(soup)
    if table is None:
        return None

    header_cells = table.find_all("th")
    header_text = [h.get_text(" ", strip=True).upper() for h in header_cells]
    bpi_rk_idx = None
    for i, h in enumerate(header_text):
        if "BPI RK" in h or h == "RK":
            bpi_rk_idx = i
            break

    rows = []
    for tr in table.find_all("tr"):
        a = tr.find("a", href=re.compile(r"/mens-college-basketball/team/_/id/"))
        if a is None:
            continue

        team_name = a.get_text(" ", strip=True)
        if not team_name:
            continue

        tds = tr.find_all(["td", "th"])
        cell_text = [td.get_text(" ", strip=True) for td in tds]

        rank_val = None
        if bpi_rk_idx is not None and bpi_rk_idx < len(cell_text):
            candidate = cell_text[bpi_rk_idx]
            if re.fullmatch(r"\d{1,3}", candidate or ""):
                rank_val = int(candidate)

        if rank_val is None:
            nums = [int(x) for x in cell_text if re.fullmatch(r"\d{1,3}", x or "")]
            if nums:
                rank_val = nums[-1]

        if rank_val is None:
            continue

        rows.append({"bpi_rank": rank_val, "team_bpi": team_name})

    if not rows:
        return None

    df = pd.DataFrame(rows).drop_duplicates(subset=["team_bpi"]).sort_values("bpi_rank")
    return df.reset_index(drop=True)


def main():
    print("Fetching ESPN BPI rankings...")
    try:
        df = scrape_bpi_rankings()
    except Exception as e:
        print(f"Error scraping BPI: {e}")
        return

    if df is None or df.empty:
        print("Failed to fetch BPI rankings")
        return

    if len(df) < 300:
        print(f"Warning: only scraped {len(df)} teams (expected ~360+). ESPN page may have changed.")

    os.makedirs("data_raw", exist_ok=True)
    df.to_csv("data_raw/bpi_rankings.csv", index=False)
    print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")


if __name__ == "__main__":
    main()
