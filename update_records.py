#!/usr/bin/env python3
"""
Fetch team records from ESPN API.
Uses standings endpoint for all D1 team records.
Outputs: data_raw/team_records.csv
"""

import requests
import pandas as pd
import os

def fetch_from_standings():
    """Fetch records from ESPN standings endpoint."""
    # Try standings endpoint
    url = "https://site.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/standings"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            teams = []
            
            # Parse standings data
            children = data.get('children', [])
            for conf in children:
                standings = conf.get('standings', {})
                entries = standings.get('entries', [])
                for entry in entries:
                    team_data = entry.get('team', {})
                    team_name = team_data.get('location', team_data.get('displayName', ''))
                    
                    # Get record from stats
                    stats = entry.get('stats', [])
                    record = '0-0'
                    for stat in stats:
                        if stat.get('name') == 'overall':
                            record = stat.get('displayValue', '0-0')
                            break
                    
                    if team_name:
                        teams.append({
                            'team_espn': team_name,
                            'display_name': team_data.get('displayName', team_name),
                            'record': record
                        })
            
            if teams:
                return pd.DataFrame(teams)
    except Exception as e:
        print(f"Standings endpoint error: {e}")
    
    return None


def fetch_from_rankings():
    """Fetch records for ranked teams from rankings API."""
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/rankings"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        teams = []
        rankings = data.get('rankings', [])
        
        for ranking in rankings:
            # Get all ranked teams
            for rank_entry in ranking.get('ranks', []):
                team_data = rank_entry.get('team', {})
                team_name = team_data.get('location', '')
                record = rank_entry.get('recordSummary', '0-0')
                
                if team_name:
                    teams.append({
                        'team_espn': team_name,
                        'display_name': team_data.get('displayName', team_name),
                        'record': record if record else '0-0'
                    })
            
            # Also check 'others' receiving votes
            for other in ranking.get('others', []):
                team_data = other.get('team', {})
                team_name = team_data.get('location', '')
                record = other.get('recordSummary', '0-0')
                
                if team_name:
                    teams.append({
                        'team_espn': team_name,
                        'display_name': team_data.get('displayName', team_name),
                        'record': record if record else '0-0'
                    })
        
        if teams:
            df = pd.DataFrame(teams)
            df = df.drop_duplicates(subset=['team_espn'])
            return df
            
    except Exception as e:
        print(f"Rankings endpoint error: {e}")
    
    return None


def fetch_from_scoreboard():
    """Fetch records from scoreboard/teams endpoint."""
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    params = {
        'limit': 400,
        'groups': 50  # D1 basketball
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        teams = []
        sports = data.get('sports', [])
        
        for sport in sports:
            leagues = sport.get('leagues', [])
            for league in leagues:
                for team_data in league.get('teams', []):
                    team = team_data.get('team', {})
                    team_name = team.get('location', team.get('shortDisplayName', ''))
                    display_name = team.get('displayName', team_name)
                    
                    # This endpoint doesn't have records, so we'll need to merge
                    if team_name:
                        teams.append({
                            'team_espn': team_name,
                            'display_name': display_name,
                            'record': ''  # Will be filled from rankings
                        })
        
        if teams:
            return pd.DataFrame(teams)
            
    except Exception as e:
        print(f"Teams endpoint error: {e}")
    
    return None


def main():
    print("Fetching team records from ESPN...")
    
    # Try standings first (best for records)
    df_standings = fetch_from_standings()
    if df_standings is not None and len(df_standings) > 100:
        print(f"Got {len(df_standings)} teams from standings")
        df = df_standings
    else:
        # Fall back to combining teams list with rankings data
        df_teams = fetch_from_scoreboard()
        df_rankings = fetch_from_rankings()
        
        if df_teams is not None:
            print(f"Got {len(df_teams)} teams from teams endpoint")
            
            if df_rankings is not None:
                print(f"Got {len(df_rankings)} teams with records from rankings")
                
                # Create lookup from rankings
                records_lookup = dict(zip(df_rankings['team_espn'], df_rankings['record']))
                
                # Apply records to teams
                df_teams['record'] = df_teams['team_espn'].map(records_lookup).fillna('')
                
            df = df_teams
        elif df_rankings is not None:
            df = df_rankings
        else:
            df = None
    
    if df is not None and not df.empty:
        df = df.drop_duplicates(subset=['team_espn'])
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/team_records.csv', index=False)
        
        # Count how many have records
        has_record = (df['record'] != '') & (df['record'] != '0-0')
        print(f"Saved {len(df)} team records to data_raw/team_records.csv")
        print(f"Teams with actual records: {has_record.sum()}")
        print("Sample records:")
        print(df[has_record].head(10).to_string())
    else:
        print("Failed to fetch team records")


if __name__ == "__main__":
    main()
