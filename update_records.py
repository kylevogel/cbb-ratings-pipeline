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
    
    # ESPN API for team standings/records
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    
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
                    team_name = team.get('displayName', team.get('name', ''))
                    
                    # Get record from the team object
                    record_obj = team.get('record', {})
                    
                    # Try different record formats
                    if isinstance(record_obj, dict):
                        items = record_obj.get('items', [])
                        if items:
                            record_str = items[0].get('summary', '')
                        else:
                            record_str = record_obj.get('summary', '')
                    elif isinstance(record_obj, str):
                        record_str = record_obj
                    else:
                        record_str = ''
                    
                    if team_name:
                        all_teams.append({
                            'team_espn': team_name,
                            'record': record_str if record_str else '0-0'
                        })
        
    except Exception as e:
        print(f"Error with ESPN teams API: {e}")
    
    # If that didn't work, try standings endpoint
    if not all_teams:
        try:
            standings_url = "https://site.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/standings"
            response = requests.get(standings_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            children = data.get('children', [])
            for child in children:
                standings = child.get('standings', {}).get('entries', [])
                for entry in standings:
                    team = entry.get('team', {})
                    team_name = team.get('displayName', '')
                    
                    stats = entry.get('stats', [])
                    wins = 0
                    losses = 0
                    for stat in stats:
                        if stat.get('name') == 'wins':
                            wins = int(stat.get('value', 0))
                        elif stat.get('name') == 'losses':
                            losses = int(stat.get('value', 0))
                    
                    if team_name:
                        all_teams.append({
                            'team_espn': team_name,
                            'record': f"{wins}-{losses}"
                        })
                        
        except Exception as e:
            print(f"Error with standings API: {e}")
    
    # Try scoreboard API as another fallback
    if not all_teams:
        try:
            from fetch_records_from_scoreboard import fetch_all_records
            all_teams = fetch_all_records()
        except:
            pass
    
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
    else:
        print("Failed to fetch team records")


if __name__ == "__main__":
    main()
