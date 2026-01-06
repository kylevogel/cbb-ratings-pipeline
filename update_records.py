"""
Fetch team records from NCAA NET rankings table (includes all D1 teams).
Outputs: data_raw/team_records.csv
Columns: team_net, record
"""

import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
import os


def fetch_records_from_net():
    url = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            print("Could not find NET table on NCAA page")
            return None

        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            team = cells[1].get_text(strip=True)
            record_text = cells[2].get_text(strip=True)

            m = re.search(r"(\d+)\s*-\s*(\d+)", record_text)
            if not m:
                continue

            record = f"{m.group(1)}-{m.group(2)}"

            if team:
                rows.append({"team_net": team, "record": record})

        if not rows:
            print("No records parsed from NET table")
            return None

        df = pd.DataFrame(rows).drop_duplicates(subset=["team_net"])
        return df

    except Exception as e:
        print(f"Error fetching records from NCAA NET page: {e}")
        return None


def main():
    print("Fetching team records from NCAA NET rankings page...")
    df = fetch_records_from_net()

    if df is not None and not df.empty:
        os.makedirs("data_raw", exist_ok=True)
        df.to_csv("data_raw/team_records.csv", index=False)
        print(f"Saved {len(df)} team records to data_raw/team_records.csv")
        print(df.head(10).to_string(index=False))
    else:
        print("Failed to fetch team records")


if __name__ == "__main__":
    main()
