#!/usr/bin/env python3
"""
Scrape AP Poll rankings from ESPN.
Outputs: data_raw/ap_rankings.csv
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re

def scrape_ap_poll():
    """Scrape AP Poll rankings from ESPN."""
    url = "https://www.espn.com/mens-college-basketball/rankings"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Look for the AP Top 25 table
        # ESPN uses various table structures, try multiple approaches
        tables = soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text().lower()
            if 'ap' in table_text or 'associated press' in table_text or len(tables) == 1:
                for tr in table.find_all('tr')[1:]:  # Skip header
                    cells = tr.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        # First cell is usually rank
                        rank_text = cells[0].get_text(strip=True)
                        
                        # Second cell is usually team
                        team_cell = cells[1]
                        team_link = team_cell.find('a')
                        if team_link:
                            team = team_link.get_text(strip=True)
                        else:
                            team = team_cell.get_text(strip=True)
                        
                        # Clean the rank
                        rank = re.sub(r'[^\d]', '', rank_text)
                        
                        # Clean team name (remove record in parentheses)
                        team = re.sub(r'\s*\(\d+-\d+\)\s*$', '', team)
                        team = re.sub(r'\s*\d+-\d+\s*$', '', team)
                        
                        if rank and team and rank.isdigit():
                            rank_int = int(rank)
                            if 1 <= rank_int <= 25:
                                rows.append({
                                    'ap_rank': rank_int,
                                    'team_ap': team
                                })
                
                if rows:
                    break
        
        # Alternative: try to find div-based layout
        if not rows:
            divs = soup.find_all('div', class_=re.compile('Table'))
            for div in divs:
                for row_div in div.find_all('tr'):
                    cells = row_div.find_all(['td', 'div'])
                    if len(cells) >= 2:
                        rank_text = cells[0].get_text(strip=True)
                        team = cells[1].get_text(strip=True)
                        
                        rank = re.sub(r'[^\d]', '', rank_text)
                        team = re.sub(r'\s*\(\d+-\d+\)\s*$', '', team)
                        
                        if rank and team and rank.isdigit():
                            rank_int = int(rank)
                            if 1 <= rank_int <= 25:
                                rows.append({
                                    'ap_rank': rank_int,
                                    'team_ap': team
                                })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['ap_rank']).sort_values('ap_rank')
            return df
        else:
            print("No AP Poll data found")
            return None
            
    except Exception as e:
        print(f"Error scraping AP Poll: {e}")
        return None


def main():
    print("Fetching AP Poll rankings...")
    df = scrape_ap_poll()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/ap_rankings.csv', index=False)
        print(f"Saved {len(df)} AP Poll rankings to data_raw/ap_rankings.csv")
    else:
        print("Failed to fetch AP Poll rankings - creating empty file")
        os.makedirs('data_raw', exist_ok=True)
        pd.DataFrame(columns=['ap_rank', 'team_ap']).to_csv('data_raw/ap_rankings.csv', index=False)


if __name__ == "__main__":
    main()
