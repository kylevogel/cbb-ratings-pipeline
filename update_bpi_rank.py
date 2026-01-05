#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import requests
import pandas as pd
import os
import re

def scrape_bpi_rankings():
    """Scrape BPI rankings from ESPN API."""
    
    # ESPN BPI API endpoint
    url = "https://site.web.api.espn.com/apis/fitt/v3/sports/basketball/mens-college-basketball/powerindex"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    params = {
        'region': 'us',
        'lang': 'en',
        'limit': 400
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        rows = []
        teams = data.get('teams', [])
        
        for team in teams:
            team_info = team.get('team', {})
            team_name = team_info.get('displayName', team_info.get('name', ''))
            
            # Get BPI rank
            bpi_rank = team.get('rank', None)
            
            if team_name and bpi_rank:
                rows.append({
                    'bpi_rank': int(bpi_rank),
                    'team_bpi': team_name
                })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.sort_values('bpi_rank').reset_index(drop=True)
            return df
        else:
            print("No BPI data found in API response")
            return None
            
    except Exception as e:
        print(f"Error fetching BPI from API: {e}")
        # Try alternate scraping method
        return scrape_bpi_fallback()


def scrape_bpi_fallback():
    """Fallback scraping from ESPN website."""
    url = "https://www.espn.com/mens-college-basketball/bpi"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        from bs4 import BeautifulSoup
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        # Look for table rows with team data
        for tr in soup.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if len(cells) >= 2:
                rank_text = cells[0].get_text(strip=True)
                team_text = cells[1].get_text(strip=True)
                
                rank = re.sub(r'[^\d]', '', rank_text)
                
                if rank and team_text and not team_text.lower() in ['team', 'rank']:
                    rows.append({
                        'bpi_rank': int(rank),
                        'team_bpi': team_text
                    })
        
        if rows:
            df = pd.DataFrame(rows)
            return df
        return None
        
    except Exception as e:
        print(f"Error in BPI fallback scraping: {e}")
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
