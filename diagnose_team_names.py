"""
Team Name Diagnostic Tool for CBB Ratings Pipeline

This script identifies team name mismatches across different ranking sources
and suggests alias mappings to fix "NR" (Not Rated) issues.

Usage:
    python diagnose_team_names.py

Output:
    - Prints unmatched teams from each ranking source
    - Suggests alias mappings for team_alias.csv
    - Creates diagnostic CSV files for review
"""

import pandas as pd
import os
from collections import defaultdict
from difflib import SequenceMatcher

def similar(a, b):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def load_data_safely(filepath, source_name):
    """Load CSV file safely with error handling"""
    try:
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            print(f"✓ Loaded {source_name}: {len(df)} teams")
            return df
        else:
            print(f"⚠ File not found: {filepath}")
            return None
    except Exception as e:
        print(f"✗ Error loading {source_name}: {e}")
        return None

def normalize_team_name(name):
    """Basic normalization for comparison"""
    if pd.isna(name):
        return ""
    return str(name).strip().lower()

def find_closest_matches(target_name, candidate_names, threshold=0.6):
    """Find closest matching team names above threshold"""
    matches = []
    normalized_target = normalize_team_name(target_name)
    
    for candidate in candidate_names:
        normalized_candidate = normalize_team_name(candidate)
        similarity = similar(normalized_target, normalized_candidate)
        if similarity >= threshold:
            matches.append((candidate, similarity))
    
    return sorted(matches, key=lambda x: x[1], reverse=True)

def main():
    print("=" * 80)
    print("CBB RATINGS PIPELINE - TEAM NAME DIAGNOSTIC TOOL")
    print("=" * 80)
    print()
    
    # Define file paths
    data_raw_dir = "data_raw"
    data_processed_dir = "data_processed"
    
    files = {
        'games': os.path.join(data_processed_dir, 'games_with_ranks.csv'),
        'espn_games': os.path.join(data_raw_dir, 'espn_games.csv'),
        'net': os.path.join(data_raw_dir, 'net_rankings.csv'),
        'kenpom': os.path.join(data_raw_dir, 'kenpom_rankings.csv'),
        'bpi': os.path.join(data_raw_dir, 'bpi_rankings.csv'),
        'ap': os.path.join(data_raw_dir, 'ap_rankings.csv'),
        'sos': os.path.join(data_raw_dir, 'sos_rankings.csv')
    }
    
    # Load all data sources
    print("LOADING DATA SOURCES")
    print("-" * 80)
    
    data = {}
    for key, filepath in files.items():
        data[key] = load_data_safely(filepath, key.upper())
    
    print()
    
    # Load team alias if it exists
    team_alias = None
    if os.path.exists('team_alias.csv'):
        team_alias = pd.read_csv('team_alias.csv')
        print(f"✓ Loaded team_alias.csv: {len(team_alias)} mappings")
    else:
        print("⚠ team_alias.csv not found")
    
    print()
    print("=" * 80)
    print("ANALYZING TEAM NAME MISMATCHES")
    print("=" * 80)
    print()
    
    # Get base team list from ESPN/games
    base_teams = set()
    if data['espn_games'] is not None:
        team_cols = [col for col in data['espn_games'].columns if 'team' in col.lower()]
        for col in team_cols:
            base_teams.update(data['espn_games'][col].dropna().unique())
    
    if data['games'] is not None:
        if 'Team' in data['games'].columns:
            base_teams.update(data['games']['Team'].dropna().unique())
        if 'Opponent' in data['games'].columns:
            base_teams.update(data['games']['Opponent'].dropna().unique())
    
    print(f"Found {len(base_teams)} unique teams in game data (ESPN baseline)")
    print()
    
    # Analyze each ranking source
    unmatched_summary = {}
    suggested_aliases = []
    
    ranking_sources = {
        'NET': ('net', 'Team'),
        'KenPom': ('kenpom', 'Team'),
        'BPI': ('bpi', 'Team'),
        'AP Poll': ('ap', 'Team'),
        'SoS': ('sos', 'Team')
    }
    
    for source_name, (data_key, team_col) in ranking_sources.items():
        print(f"\n{source_name} ANALYSIS")
        print("-" * 80)
        
        if data[data_key] is None:
            print(f"Skipping {source_name} - data not available")
            continue
        
        df = data[data_key]
        
        # Check if team column exists
        if team_col not in df.columns:
            print(f"Available columns: {list(df.columns)}")
            team_col = df.columns[0]  # Use first column as fallback
            print(f"Using column: {team_col}")
        
        ranking_teams = set(df[team_col].dropna().unique())
        print(f"Teams in {source_name}: {len(ranking_teams)}")
        
        # Find unmatched teams (in rankings but not in base game data)
        unmatched = ranking_teams - base_teams
        
        if len(unmatched) > 0:
            print(f"\n⚠ {len(unmatched)} unmatched teams in {source_name}:")
            unmatched_summary[source_name] = list(unmatched)
            
            for unmatched_team in sorted(unmatched)[:20]:  # Show first 20
                print(f"  • {unmatched_team}")
                
                # Find closest matches in base teams
                matches = find_closest_matches(unmatched_team, base_teams, threshold=0.6)
                if matches:
                    best_match = matches[0]
                    print(f"    → Possible match: '{best_match[0]}' (similarity: {best_match[1]:.2f})")
                    
                    # Suggest alias
                    suggested_aliases.append({
                        'source': source_name,
                        'ranking_name': unmatched_team,
                        'suggested_espn_name': best_match[0],
                        'similarity': best_match[1]
                    })
            
            if len(unmatched) > 20:
                print(f"  ... and {len(unmatched) - 20} more")
        else:
            print(f"✓ All teams matched!")
    
    # Generate suggested aliases CSV
    if suggested_aliases:
        print()
        print("=" * 80)
        print("SUGGESTED ALIASES")
        print("=" * 80)
        print()
        
        suggestions_df = pd.DataFrame(suggested_aliases)
        suggestions_df = suggestions_df.sort_values('similarity', ascending=False)
        
        output_file = 'suggested_team_aliases.csv'
        suggestions_df.to_csv(output_file, index=False)
        print(f"✓ Saved {len(suggestions_df)} suggestions to: {output_file}")
        print()
        print("Top 10 suggestions:")
        print(suggestions_df.head(10).to_string(index=False))
        print()
        print("Review this file and add confirmed mappings to team_alias.csv")
    
    # Check for teams in games showing NR
    if data['games'] is not None:
        print()
        print("=" * 80)
        print("TEAMS WITH 'NR' RANKINGS IN PROCESSED DATA")
        print("=" * 80)
        print()
        
        df_games = data['games']
        rank_columns = [col for col in df_games.columns if 'Rank' in col or 'rank' in col]
        
        teams_with_nr = set()
        for col in rank_columns:
            if col in df_games.columns:
                nr_teams = df_games[df_games[col] == 'NR']['Team'].unique()
                if len(nr_teams) > 0:
                    print(f"\n{col}: {len(nr_teams)} teams showing 'NR'")
                    teams_with_nr.update(nr_teams)
        
        if teams_with_nr:
            print(f"\nTotal unique teams with any 'NR': {len(teams_with_nr)}")
            print("Sample teams (first 15):")
            for team in sorted(list(teams_with_nr))[:15]:
                print(f"  • {team}")
    
    print()
    print("=" * 80)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Review 'suggested_team_aliases.csv'")
    print("2. Verify the suggested matches are correct")
    print("3. Add confirmed mappings to 'team_alias.csv'")
    print("4. Re-run your pipeline with: python update_all.py")
    print()

if __name__ == "__main__":
    main()
