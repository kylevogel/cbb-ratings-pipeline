#!/usr/bin/env python3
"""
Utility functions for standardizing team names across different data sources.
"""

import pandas as pd
import os
import re

def load_team_alias():
    """Load the team alias mapping file."""
    alias_path = os.path.join(os.path.dirname(__file__), 'team_alias.csv')
    if os.path.exists(alias_path):
        return pd.read_csv(alias_path)
    return None


def normalize_team_name(name):
    """Basic normalization of team names."""
    if pd.isna(name):
        return None
    
    name = str(name).strip()
    
    # Remove common suffixes/prefixes
    name = re.sub(r'\s*\(\d+-\d+\)\s*$', '', name)  # Remove records like (15-3)
    name = re.sub(r'\s*#\d+\s*', '', name)  # Remove rankings like #5
    name = re.sub(r'^\d+\s+', '', name)  # Remove leading numbers
    
    # Standardize common variations
    replacements = {
        'St.': 'State',
        'Univ.': '',
        'University': '',
        '  ': ' '
    }
    
    for old, new in replacements.items():
        name = name.replace(old, new)
    
    return name.strip()


def create_name_lookup(alias_df, source_column):
    """Create a lookup dictionary from source name to canonical name."""
    if alias_df is None:
        return {}
    
    lookup = {}
    for _, row in alias_df.iterrows():
        canonical = row['canonical']
        source_name = row.get(source_column, '')
        if pd.notna(source_name) and source_name:
            lookup[source_name.lower().strip()] = canonical
            lookup[source_name.strip()] = canonical
    
    return lookup


def standardize_team_names(df, team_column, source='espn'):
    """
    Standardize team names in a dataframe.
    
    Args:
        df: DataFrame containing team names
        team_column: Name of the column containing team names
        source: Source of the data ('espn', 'net', 'kenpom', 'bpi', 'ap', 'sos')
    
    Returns:
        DataFrame with standardized team names added in 'team' column
    """
    alias_df = load_team_alias()
    
    source_col_map = {
        'espn': 'espn',
        'net': 'net',
        'kenpom': 'kenpom',
        'bpi': 'bpi',
        'ap': 'ap',
        'sos': 'sos',
        'warrennolan': 'warrennolan'
    }
    
    source_col = source_col_map.get(source, source)
    lookup = create_name_lookup(alias_df, source_col)
    
    def get_canonical(name):
        if pd.isna(name):
            return None
        
        name_str = str(name).strip()
        
        # Try exact match first
        if name_str in lookup:
            return lookup[name_str]
        
        # Try lowercase match
        if name_str.lower() in lookup:
            return lookup[name_str.lower()]
        
        # Try normalized match
        normalized = normalize_team_name(name_str)
        if normalized in lookup:
            return lookup[normalized]
        if normalized.lower() in lookup:
            return lookup[normalized.lower()]
        
        # Return original if no match found
        return name_str
    
    df = df.copy()
    df['team'] = df[team_column].apply(get_canonical)
    
    return df


def get_unmatched_teams(df, team_column, source='espn'):
    """Find teams that couldn't be matched to canonical names."""
    alias_df = load_team_alias()
    if alias_df is None:
        return df[team_column].unique().tolist()
    
    canonical_names = set(alias_df['canonical'].dropna().unique())
    
    standardized = standardize_team_names(df, team_column, source)
    
    unmatched = []
    for orig, std in zip(df[team_column], standardized['team']):
        if std not in canonical_names:
            unmatched.append({'original': orig, 'attempted_match': std})
    
    return pd.DataFrame(unmatched).drop_duplicates()


if __name__ == "__main__":
    # Test the module
    alias_df = load_team_alias()
    if alias_df is not None:
        print(f"Loaded {len(alias_df)} team aliases")
        print(f"Columns: {alias_df.columns.tolist()}")
    else:
        print("Could not load team alias file")
