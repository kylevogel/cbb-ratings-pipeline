"""
Team Name Diagnostic Tool for CBB Ratings Pipeline - Enhanced Version

This script identifies team name mismatches across different ranking sources
and suggests alias mappings to fix "NR" (Not Rated) issues.

This version can work with existing processed data and also run the data
collection if needed.

Usage:
    python diagnose_team_names.py [--collect]
    
    --collect: Run data collection scripts first (optional)
"""

import pandas as pd
import os
import sys
import subprocess
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
            print(f"✓ Loaded {source_name}: {len(df)} rows")
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

def run_data_collection():
    """Run the data collection scripts"""
    print()
    print("=" * 80)
    print("RUNNING DATA COLLECTION SCRIPTS")
    print("=" * 80)
    print()
    
    scripts = [
        'update_net_rank.py',
        'update_kenpom_rank.py',
        'update_bpi_rank.py',
        'update_ap_rank.py',
        'update_sos_rank.py'
    ]
    
    for script in scripts:
        if os.path.exists(script):
            print(f"Running {script}...")
            try:
                subprocess.run(['python', script], check=True)
                print(f"✓ {script} completed")
            except subprocess.CalledProcessError as e:
                print(f"✗ {script} failed: {e}")
        else:
            print(f"⚠ Script not found: {script}")
    
    print()

def analyze_processed_data(df_games):
    """Analyze the processed games data for NR issues"""
    print("=" * 80)
    print("ANALYZING PROCESSED GAMES DATA")
    print("=" * 80)
    print()
    
    # Display column names
    print("Available columns:")
    for col in df_games.columns:
        print(f"  • {col}")
    print()
    
    # Find ranking columns
    rank_columns = [col for col in df_games.columns if 'rank' in col.lower() or 'Rank' in col]
    
    if not rank_columns:
        print("⚠ No ranking columns found in processed data")
        return {}
    
    print(f"Found {len(rank_columns)} ranking columns:")
    for col in rank_columns:
        print(f"  • {col}")
    print()
    
    # Analyze NR values
    nr_analysis = {}
    
    for col in rank_columns:
        nr_mask = df_games[col].astype(str).str.upper() == 'NR'
        nr_count = nr_mask.sum()
        
        if nr_count > 0:
            nr_teams = df_games[nr_mask]['Team'].unique()
            nr_analysis[col] = {
                'count': nr_count,
                'teams': sorted(nr_teams)
            }
            
            print(f"\n{col}:")
            print(f"  Games with 'NR': {nr_count}")
            print(f"  Unique teams affected: {len(nr_teams)}")
            print(f"  Sample teams (first 10):")
            for team in nr_teams[:10]:
                print(f"    - {team}")
            if len(nr_teams) > 10:
                print(f"    ... and {len(nr_teams) - 10} more")
    
    if not nr_analysis:
        print("✓ No 'NR' values found in ranking columns!")
    
    return nr_analysis

def compare_with_rankings(base_teams, data_raw_dir):
    """Compare base teams with ranking sources"""
    print()
    print("=" * 80)
    print("COMPARING WITH RANKING SOURCES")
    print("=" * 80)
    print()
    
    ranking_files = {
        'NET': 'NET_Rank.csv',
        'KenPom': 'KenPom_Rank.csv',
        'BPI': 'BPI_Rank.csv',
        'AP Poll': 'AP_Rank.csv',
        'SoS': 'SOS_Rank.csv'
    }
    
    all_suggestions = []
    
    for source_name, filename in ranking_files.items():
        filepath = os.path.join(data_raw_dir, filename)
        
        print(f"\n{source_name} ANALYSIS")
        print("-" * 80)
        
        if not os.path.exists(filepath):
            print(f"⚠ File not found: {filepath}")
            continue
        
        df_rank = pd.read_csv(filepath)
        print(f"Loaded: {len(df_rank)} teams")
        print(f"Columns: {list(df_rank.columns)}")
        
        # Try to identify team name column
        team_col = None
        for col in ['Team', 'team', 'TeamName', 'School', 'school']:
            if col in df_rank.columns:
                team_col = col
                break
        
        if team_col is None:
            team_col = df_rank.columns[0]
            print(f"Using first column as team name: {team_col}")
        
        ranking_teams = set(df_rank[team_col].dropna().unique())
        print(f"Unique teams: {len(ranking_teams)}")
        
        # Find teams in rankings but not in base data
        unmatched = ranking_teams - base_teams
        
        if unmatched:
            print(f"\n⚠ {len(unmatched)} teams in {source_name} not found in game data:")
            
            for unmatched_team in sorted(unmatched)[:15]:
                print(f"  • '{unmatched_team}'")
                
                # Find closest matches
                matches = find_closest_matches(unmatched_team, base_teams, threshold=0.5)
                if matches:
                    best = matches[0]
                    print(f"    → Suggested: '{best[0]}' (similarity: {best[1]:.2f})")
                    all_suggestions.append({
                        'source': source_name,
                        'ranking_name': unmatched_team,
                        'suggested_espn_name': best[0],
                        'similarity': best[1]
                    })
            
            if len(unmatched) > 15:
                print(f"  ... and {len(unmatched) - 15} more")
        else:
            print("✓ All teams matched!")
    
    return all_suggestions

def main():
    print("=" * 80)
    print("CBB RATINGS PIPELINE - TEAM NAME DIAGNOSTIC TOOL")
    print("=" * 80)
    print()
    
    # Check if we should collect data first
    if '--collect' in sys.argv:
        run_data_collection()
    
    # Define paths
    data_raw_dir = "data_raw"
    data_processed_dir = "data_processed"
    games_file = os.path.join(data_processed_dir, 'games_with_ranks.csv')
    
    # Load processed games data
    print("LOADING PROCESSED GAMES DATA")
    print("-" * 80)
    df_games = load_data_safely(games_file, 'games_with_ranks.csv')
    
    if df_games is None:
        print("\n✗ Cannot proceed without games_with_ranks.csv")
        print("Make sure you've run the pipeline at least once with: python update_all.py")
        return
    
    print()
    
    # Get base team list
    base_teams = set()
    if 'Team' in df_games.columns:
        base_teams.update(df_games['Team'].dropna().unique())
    if 'Opponent' in df_games.columns:
        base_teams.update(df_games['Opponent'].dropna().unique())
    
    print(f"Found {len(base_teams)} unique teams in game data")
    print()
    
    # Analyze NR issues in processed data
    nr_analysis = analyze_processed_data(df_games)
    
    # Compare with ranking sources if they exist
    suggestions = compare_with_rankings(base_teams, data_raw_dir)
    
    # Save suggestions
    if suggestions:
        print()
        print("=" * 80)
        print("SAVING SUGGESTIONS")
        print("=" * 80)
        print()
        
        df_suggestions = pd.DataFrame(suggestions)
        df_suggestions = df_suggestions.sort_values('similarity', ascending=False)
        
        output_file = 'suggested_team_aliases.csv'
        df_suggestions.to_csv(output_file, index=False)
        print(f"✓ Saved {len(df_suggestions)} suggestions to: {output_file}")
        print()
        print("Top 10 highest confidence suggestions:")
        print(df_suggestions.head(10).to_string(index=False))
    
    # Summary
    print()
    print("=" * 80)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 80)
    print()
    
    if nr_analysis:
        print("Issues found:")
        for col, info in nr_analysis.items():
            print(f"  • {col}: {info['count']} games with 'NR' ({len(info['teams'])} unique teams)")
    else:
        print("✓ No 'NR' issues found in processed data")
    
    print()
    print("Next steps:")
    print("1. If raw ranking files are missing, run: python update_all.py")
    print("2. Review 'suggested_team_aliases.csv' if generated")
    print("3. Add confirmed mappings to 'team_alias.csv'")
    print("4. Format: espn_name,alternate_name,source")
    print("5. Re-run pipeline: python update_all.py")
    print()

if __name__ == "__main__":
    main()
