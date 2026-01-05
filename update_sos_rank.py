#!/usr/bin/env python3
"""
Scrape Strength of Schedule rankings from Warren Nolan.
Outputs: data_raw/sos_rankings.csv
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re

def scrape_sos_rankings():
    """Scrape SOS rankings from Warren Nolan."""
    # Correct URL for 2025-26 season
    url = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Find the main table
        tables = soup.find_all('table')
        
        for table in tables:
            # Find all rows
            for tr in table.find_all('tr')[1:]:  # Skip header
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 2:
                    # Get rank (first column)
                    rank_text = cells[0].get_text(strip=True)
                    
                    # Get team name (second column, look for link)
                    team_cell = cells[1]
                    team_link = team_cell.find('a')
                    if team_link:
                        team = team_link.get_text(strip=True)
                    else:
                        team = team_cell.get_text(strip=True)
                    
                    # Clean rank
                    rank = re.sub(r'[^\d]', '', rank_text)
                    
                    if rank and team and rank.isdigit():
                        rank_int = int(rank)
                        if 1 <= rank_int <= 400:
                            rows.append({
                                'sos_rank': rank_int,
                                'team_sos': team
                            })
            
            if len(rows) > 100:  # Found the right table
                break
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['sos_rank']).sort_values('sos_rank')
            return df
        else:
            print("No SOS data found on page")
            return None
            
    except Exception as e:
        print(f"Error scraping SOS: {e}")
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
