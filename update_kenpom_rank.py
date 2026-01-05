#!/usr/bin/env python3
"""
Scrape KenPom rankings.
Note: KenPom requires subscription for full data. This scrapes publicly available rankings.
Outputs: data_raw/kenpom_rankings.csv
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re

def scrape_kenpom_rankings():
    """Scrape KenPom rankings from the public page."""
    url = "https://kenpom.com/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the main ratings table
        table = soup.find('table', {'id': 'ratings-table'})
        if not table:
            # Try alternate method
            tables = soup.find_all('table')
            for t in tables:
                if t.find('th') and 'Team' in t.get_text():
                    table = t
                    break
        
        if not table:
            print("Could not find KenPom table")
            return None
        
        rows = []
        tbody = table.find('tbody')
        if tbody:
            trs = tbody.find_all('tr')
        else:
            trs = table.find_all('tr')[1:]  # Skip header
        
        for tr in trs:
            cells = tr.find_all(['td', 'th'])
            if len(cells) >= 2:
                rank_text = cells[0].get_text(strip=True)
                team_cell = cells[1]
                
                # Get team name
                team_link = team_cell.find('a')
                if team_link:
                    team = team_link.get_text(strip=True)
                else:
                    team = team_cell.get_text(strip=True)
                
                # Clean rank
                rank = re.sub(r'[^\d]', '', rank_text)
                
                if rank and team:
                    rows.append({
                        'kenpom_rank': int(rank),
                        'team_kenpom': team
                    })
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            print("No KenPom data found")
            return None
            
    except Exception as e:
        print(f"Error scraping KenPom: {e}")
        return None


def main():
    print("Fetching KenPom rankings...")
    df = scrape_kenpom_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/kenpom_rankings.csv', index=False)
        print(f"Saved {len(df)} KenPom rankings to data_raw/kenpom_rankings.csv")
    else:
        print("Failed to fetch KenPom rankings")


if __name__ == "__main__":
    main()
