#!/usr/bin/env python3
"""
Scrape AP Poll rankings from ESPN Rankings API.
Outputs: data_raw/ap_rankings.csv
"""

import requests
import pandas as pd
import os

def scrape_ap_poll():
    """Scrape AP Poll rankings from ESPN Rankings API."""
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/rankings"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        rows = []
        
        # Find AP Poll in rankings array (id="1" is AP, id="2" is Coaches)
        rankings = data.get('rankings', [])
        
        ap_poll = None
        for ranking in rankings:
            name = ranking.get('name', '').lower()
            # Look for AP Poll specifically
            if 'ap' in name or 'associated press' in name:
                ap_poll = ranking
                break
        
        # If no AP poll found, use the first ranking (usually Coaches, but better than nothing)
        if not ap_poll and rankings:
            # Try to find any poll with ranks
            for ranking in rankings:
                if ranking.get('ranks'):
                    ap_poll = ranking
                    print(f"Using {ranking.get('name', 'Unknown')} poll (AP not found)")
                    break
        
        if ap_poll:
            ranks = ap_poll.get('ranks', [])
            for rank_entry in ranks:
                rank = rank_entry.get('current')
                team_data = rank_entry.get('team', {})
                
                # Get team location (e.g., "Michigan", "Arizona", "Iowa State")
                team_name = team_data.get('location', '')
                
                # Fallback to nickname if location not available
                if not team_name:
                    team_name = team_data.get('nickname', '')
                
                if rank and team_name:
                    rows.append({
                        'ap_rank': rank,
                        'team_ap': team_name
                    })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.sort_values('ap_rank')
            print(f"Found {len(df)} ranked teams:")
            for _, row in df.head(10).iterrows():
                print(f"  {row['ap_rank']}: {row['team_ap']}")
            return df
        else:
            print("No AP Poll data found in API response")
            return None
            
    except Exception as e:
        print(f"Error fetching AP Poll from API: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("Fetching AP Poll rankings from ESPN API...")
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
