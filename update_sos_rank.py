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
    url = "https://www.warrennolan.com/basketball/2025/sos-projected"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Find the main table
        table = soup.find('table')
        if not table:
            tables = soup.find_all('table')
            if tables:
                table = tables[0]
        
        if table:
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
                    
                    if rank and team:
                        rows.append({
                            'sos_rank': int(rank),
                            'team_sos': team
                        })
        
        # Alternative: try to parse from div structure
        if not rows:
            # Look for data in divs
            data_divs = soup.find_all('div', class_=re.compile('data|table|row', re.I))
            for div in data_divs:
                text = div.get_text()
                # Try to extract rank and team
                match = re.search(r'(\d+)\s+([A-Za-z\s&\'-]+)', text)
                if match:
                    rank = int(match.group(1))
                    team = match.group(2).strip()
                    if 1 <= rank <= 365 and team:
                        rows.append({
                            'sos_rank': rank,
                            'team_sos': team
                        })
        
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


def scrape_sos_current():
    """Try current SOS (non-projected) as fallback."""
    url = "https://www.warrennolan.com/basketball/2025/sos"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        table = soup.find('table')
        
        if table:
            for tr in table.find_all('tr')[1:]:
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 2:
                    rank_text = cells[0].get_text(strip=True)
                    team_cell = cells[1]
                    team_link = team_cell.find('a')
                    team = team_link.get_text(strip=True) if team_link else team_cell.get_text(strip=True)
                    
                    rank = re.sub(r'[^\d]', '', rank_text)
                    
                    if rank and team:
                        rows.append({
                            'sos_rank': int(rank),
                            'team_sos': team
                        })
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        return None
        
    except Exception as e:
        print(f"Error scraping current SOS: {e}")
        return None


def main():
    print("Fetching SOS rankings from Warren Nolan...")
    df = scrape_sos_rankings()
    
    if df is None or df.empty:
        print("Trying current SOS as fallback...")
        df = scrape_sos_current()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/sos_rankings.csv', index=False)
        print(f"Saved {len(df)} SOS rankings to data_raw/sos_rankings.csv")
    else:
        print("Failed to fetch SOS rankings")


if __name__ == "__main__":
    main()
