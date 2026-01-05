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
        
        # Find all tables
        tables = soup.find_all('table')
        
        # The first table should be AP Poll
        if tables:
            table = tables[0]  # AP Poll is first table
            
            for tr in table.find_all('tr')[1:]:  # Skip header
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 2:
                    # First cell is rank
                    rank_text = cells[0].get_text(strip=True)
                    rank = re.sub(r'[^\d]', '', rank_text)
                    
                    # Second cell contains team - find the LAST link with team URL
                    # ESPN structure: [Logo] [ShortName] [FullName]
                    team_cell = cells[1]
                    team_links = team_cell.find_all('a', href=re.compile(r'/mens-college-basketball/team/'))
                    
                    team = None
                    if team_links:
                        # Get the last link text (usually the full team name)
                        for link in team_links:
                            link_text = link.get_text(strip=True)
                            # Skip short abbreviations (less than 4 chars)
                            if len(link_text) >= 3:
                                team = link_text
                    
                    if not team:
                        # Fallback: get all text and clean it
                        team = team_cell.get_text(strip=True)
                    
                    # Clean team name
                    # Remove records like (15-3) or 7-0
                    team = re.sub(r'\s*\(\d+-\d+\)\s*', '', team)
                    team = re.sub(r'\s+\d+-\d+\s*$', '', team)
                    # Remove vote counts
                    team = re.sub(r'\s*\(\d+\)\s*', '', team)
                    
                    # Handle duplicate names like "ARIZ Arizona"
                    # Take the longer version if there's a space
                    parts = team.split()
                    if len(parts) >= 2:
                        # Check if first part is an abbreviation (all caps, short)
                        if parts[0].isupper() and len(parts[0]) <= 4:
                            team = ' '.join(parts[1:])
                    
                    if rank and team and rank.isdigit():
                        rank_int = int(rank)
                        if 1 <= rank_int <= 25:
                            # Clean common suffixes that might remain
                            team = team.strip()
                            if team:
                                rows.append({
                                    'ap_rank': rank_int,
                                    'team_ap': team
                                })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['ap_rank']).sort_values('ap_rank')
            print(f"Found {len(df)} AP ranked teams:")
            for _, row in df.iterrows():
                print(f"  {row['ap_rank']}: {row['team_ap']}")
            return df
        else:
            print("No AP Poll data found")
            return None
            
    except Exception as e:
        print(f"Error scraping AP Poll: {e}")
        import traceback
        traceback.print_exc()
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
