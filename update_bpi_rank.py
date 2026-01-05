#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re

def scrape_bpi_rankings():
    """Scrape BPI rankings from ESPN."""
    url = "https://www.espn.com/mens-college-basketball/bpi"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Find all team links - ESPN lists teams in BPI order
        team_links = soup.find_all('a', href=re.compile(r'/mens-college-basketball/team/_/id/'))
        
        seen_teams = set()
        rank = 1
        for link in team_links:
            team_name = link.get_text(strip=True)
            if team_name and team_name not in seen_teams and len(team_name) > 2:
                seen_teams.add(team_name)
                rows.append({
                    'bpi_rank': rank,
                    'team_bpi': team_name
                })
                rank += 1
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            print("No BPI data found")
            return None
            
    except Exception as e:
        print(f"Error scraping BPI: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("Fetching ESPN BPI rankings...")
    df = scrape_bpi_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/bpi_rankings.csv', index=False)
        print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")
    else:
        print("Failed to fetch BPI rankings")


if __name__ == "__main__":
    main()
