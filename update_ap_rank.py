#!/usr/bin/env python3
"""
Scrape AP Top 25 men's college basketball poll from AP News hub page.
Outputs: data_raw/ap_rankings.csv
"""

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


def scrape_ap_poll():
    url = "https://apnews.com/hub/ap-top-25-college-basketball-poll"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        tables = soup.find_all("table")
        target = None
        for t in tables:
            th_text = " ".join([th.get_text(" ", strip=True) for th in t.find_all("th")]).lower()
            if "rank" in th_text and "teams" in th_text:
                target = t
                break

        if target is None:
            print("Could not find AP rankings table on AP News page")
            return None

        rows = []
        for tr in target.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            rank_text = cells[0].get_text(" ", strip=True)
            rank_text = re.sub(r"[^\d]", "", rank_text)
            if not rank_text.isdigit():
                continue
            rank = int(rank_text)

            team_cell = cells[1]
            team = ""
            for a in team_cell.find_all("a"):
                txt = a.get_text(" ", strip=True)
                if txt:
                    team = txt
                    break

            if not team:
                raw = team_cell.get_text(" ", strip=True)
                raw = re.sub(r"\s+", " ", raw).strip()
                team = re.split(r"\s+\d+\s*-\s*\d+\b", raw)[0].strip()

            if team:
                rows.append({"ap_rank": rank, "team_ap": team})

        if not rows:
            print("No AP poll rows parsed")
            return None

        df = pd.DataFrame(rows).drop_duplicates(subset=["ap_rank"]).sort_values("ap_rank")
        return df.reset_index(drop=True)

    except Exception as e:
        print(f"Error scraping AP poll from AP News: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("Fetching AP Poll rankings from AP News...")
    df = scrape_ap_poll()

    os.makedirs("data_raw", exist_ok=True)

    if df is not None and not df.empty:
        df.to_csv("data_raw/ap_rankings.csv", index=False)
        print(f"Saved {len(df)} AP Poll teams to data_raw/ap_rankings.csv")
        print("Top 10:")
        for _, row in df.head(10).iterrows():
            print(f"  {int(row['ap_rank'])}: {row['team_ap']}")
    else:
        print("Failed to fetch AP Poll rankings - creating empty file")
        pd.DataFrame(columns=["ap_rank", "team_ap"]).to_csv("data_raw/ap_rankings.csv", index=False)


if __name__ == "__main__":
    main()
