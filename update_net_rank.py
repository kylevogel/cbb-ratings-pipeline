"""
Scrape NCAA NET rankings from the official NCAA website.
Outputs: data_raw/net_rankings.csv
"""

import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
import os

def scrape_net_rankings():
    """Scrape NET rankings from NCAA website."""
    url = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table = soup.find('table')
        if not table:
            print("Could not find NET rankings table")
            return None
        
        rows = []
        for tr in table.find_all('tr')[1:]:
            cells = tr.find_all(['td', 'th'])
            if len(cells) >= 2:
                rank = cells[0].get_text(strip=True)
                team = cells[1].get_text(strip=True)
                
                rank = re.sub(r'[^\d]', '', rank)
                
                if rank and team:
                    rows.append({
                        'net_rank': int(rank),
                        'team_net': team
                    })
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            print("No NET rankings data found")
            return None
            
    except Exception as e:
        print(f"Error scraping NET rankings: {e}")
        return None


def main():
    print("Fetching NET rankings...")
    df = scrape_net_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/net_rankings.csv', index=False)
        print(f"Saved {len(df)} NET rankings to data_raw/net_rankings.csv")
    else:
        print("Failed to fetch NET rankings")


if __name__ == "__main__":
    main()
