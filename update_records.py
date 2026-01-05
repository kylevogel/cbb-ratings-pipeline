#!/usr/bin/env python3
"""
Fetch team records from ESPN API.
Outputs: data_raw/team_records.csv
"""

import requests
import pandas as pd
import os

def fetch_team_records():
    """Fetch team records from ESPN API."""
    
    # ESPN API for all D1 teams with records
    # groups=50 is Division I basketball
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    params = {
        'limit': 400,
        'groups': 50  # D1 basketball
    }
    
    all_teams = []
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        sports = data.get('sports', [])
        for sport in sports:
            leagues = sport.get('leagues', [])
            for league in leagues:
                teams = league.get('teams', [])
                for team_data in teams:
                    team = team_data.get('team', {})
                    
                    # Get different name formats
                    display_name = team.get('displayName', '')  # "Arizona Wildcats"
                    short_name = team.get('shortDisplayName', '')  # "Arizona"
                    location = team.get('location', '')  # "Arizona"
                    nickname = team.get('nickname', '')  # "Arizona"
                    
                    # Use location as the primary name (matches our alias file better)
                    team_name = location if location else short_name
                    
                    # Get record
                    record_str = '0-0'
                    record_obj = team.get('record', {})
                    if isinstance(record_obj, dict):
                        items = record_obj.get('items', [])
                        if items:
                            record_str = items[0].get('summary', '0-0')
                        elif record_obj.get('summary'):
                            record_str = record_obj.get('summary', '0-0')
                    elif isinstance(record_obj, str):
                        record_str = record_obj
                    
                    if team_name:
                        all_teams.append({
                            'team_espn': team_name,
                            'display_name': display_name,
                            'record': record_str if record_str else '0-0'
                        })
        
    except Exception as e:
        print(f"Error with ESPN teams API: {e}")
        import traceback
        traceback.print_exc()
    
    if all_teams:
        df = pd.DataFrame(all_teams)
        df = df.drop_duplicates(subset=['team_espn'])
        return df
    
    return None


def main():
    print("Fetching team records from ESPN...")
    df = fetch_team_records()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/team_records.csv', index=False)
        print(f"Saved {len(df)} team records to data_raw/team_records.csv")
        print(f"Sample records:")
        print(df.head(10).to_string())
    else:
        print("Failed to fetch team records")


if __name__ == "__main__":
    main()
