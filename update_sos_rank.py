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
    url = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        try:
            tables = pd.read_html(response.text)
            for df in tables:
                cols = [str(c).lower() for c in df.columns]
                if any('team' in c for c in cols) and any('rank' in c for c in cols):
                    team_col = None
                    rank_col = None
                    for c in df.columns:
                        c_lower = str(c).lower()
                        if 'team' in c_lower:
                            team_col = c
                        elif c_lower == 'rank':
                            rank_col = c
                    
                    if team_col and rank_col:
                        result = df[[team_col, rank_col]].copy()
                        result.columns = ['team_sos', 'sos_rank']
                        result = result.dropna()
                        result['sos_rank'] = pd.to_numeric(result['sos_rank'], errors='coerce')
                        result = result.dropna()
                        result['sos_rank'] = result['sos_rank'].astype(int)
                        result = result[result['sos_rank'] > 0]
                        result = result.drop_duplicates(subset=['sos_rank'])
                        result = result.sort_values('sos_rank')
                        if len(result) > 100:
                            return result
        except Exception as e:
            print(f"pandas read_html failed: {e}")
        
        # Fallback to BeautifulSoup parsing
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        table = soup.find('table')
        if table:
            all_rows = table.find_all('tr')
            for tr in all_rows[1:]:
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 3:
                    team_cell = cells[0]
                    team_link = team_cell.find('a')
                    if team_link:
                        team = team_link.get_text(strip=True)
                    else:
                        team = team_cell.get_text(strip=True)
                    
                    rank_text = cells[2].get_text(strip=True)
                    
                    rank_clean = re.sub(r'[^\d]', '', rank_text)
                    
                    if team and rank_clean and rank_clean.isdigit():
                        rank = int(rank_clean)
                        if 1 <= rank <= 400:
                            rows.append({
                                'sos_rank': rank,
                                'team_sos': team
                            })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['sos_rank']).sort_values('sos_rank')
            return df
        
        print("No SOS data found")
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
