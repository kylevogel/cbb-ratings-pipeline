"""
Scrape AP Poll rankings from AP News hub page.
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
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    start_idx = None
    for i, ln in enumerate(lines):
        if "ap top 25" in ln.lower() and "men" in ln.lower() and "poll" in ln.lower():
            start_idx = i
            break

    if start_idx is None:
        return None

    rows = []
    i = start_idx

    stop_markers = {
        "others receiving votes",
        "dropout",
        "dropped out",
        "trend",
        "points",
    }

    while i < len(lines) - 1:
        ln = lines[i]

        if any(m in ln.lower() for m in stop_markers) and rows:
            break

        if re.fullmatch(r"\d{1,2}", ln):
            rk = int(ln)
            if 1 <= rk <= 25:
                team = lines[i + 1] if i + 1 < len(lines) else ""
                team = re.sub(r"\s*\(\d+-\d+\)\s*$", "", team).strip()
                if team:
                    rows.append({"ap_rank": rk, "team_ap": team})
                i += 2
                continue

        i += 1

    if not rows:
        return None

    df = pd.DataFrame(rows).drop_duplicates(subset=["ap_rank"]).sort_values("ap_rank").reset_index(drop=True)

    if len(df) < 20:
        return None

    return df


def main():
    print("Fetching AP Poll rankings from AP News...")
    df = None
    try:
        df = scrape_ap_poll()
    except Exception as e:
        print(f"Error fetching AP Poll: {e}")

    os.makedirs("data_raw", exist_ok=True)

    if df is not None and not df.empty:
        df.to_csv("data_raw/ap_rankings.csv", index=False)
        print(f"Saved {len(df)} AP Poll teams to data_raw/ap_rankings.csv")
    else:
        print("Failed to fetch AP Poll rankings")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
