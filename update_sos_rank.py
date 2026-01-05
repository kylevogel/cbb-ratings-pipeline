#!/usr/bin/env python3
"""
Scrape Strength of Schedule rankings from Warren Nolan.
Outputs: data_raw/sos_rankings.csv
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

def scrape_sos_rankings():
    """Scrape SOS rankings from Warren Nolan."""
    url = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Find the main data table
        table = soup.find('table')
        
        if table:
            for tr in table.find_all('tr')[1:]:  # Skip header row
                cells = tr.find_all('td')
                if len(cells) >= 3:
                    # Column 0: Team name (with link)
                    # Column 1: SOS value
                    # Column 2: Rank
                    team_cell = cells[0]
                    team_link = team_cell.find('a')
                    if team_link:
                        team = team_link.get_text(strip=True)
                    else:
                        team = team_cell.get_text(strip=True)
                    
                    # Get rank from column 2
                    rank_text = cells[2].get_text(strip=True)
                    
                    if team and rank_text.isdigit():
                        rank = int(rank_text)
                        if 1 <= rank <= 400:
                            rows.append({
                                'sos_rank': rank,
                                'team_sos': team
                            })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['sos_rank']).sort_values('sos_rank')
            return df
        else:
            print("No SOS data found in table")
            return None
            
    except Exception as e:
        print(f"Error scraping SOS: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("Fetching SOS rankings from Warren Nolan...")
    df = scrape_sos_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/sos_rankings.csv', index=False)
        print(f"Saved {len(df)} SOS rankings to data_raw/sos_rankings.csv")
    else:
        print("Failed to fetch SOS rankings")


if __name__ == "__main__":
    main()
