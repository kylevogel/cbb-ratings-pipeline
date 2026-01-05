#!/usr/bin/env python3
"""
Utility functions for standardizing team names across different data sources.
"""

import os
import re
import pandas as pd


def load_team_alias():
    """Load the team alias mapping file."""
    alias_path = os.path.join(os.path.dirname(__file__), "team_alias.csv")
    if os.path.exists(alias_path):
        return pd.read_csv(alias_path)
    return None


def normalize_team_name(name):
    """Basic normalization of team names."""
    if pd.isna(name):
        return None

    name = str(name).strip()

    # Remove common suffixes/prefixes
    name = re.sub(r"\s*\(\d+-\d+\)\s*$", "", name)  # Remove records like (15-3)
    name = re.sub(r"\s*#\d+\s*", "", name)          # Remove rankings like #5
    name = re.sub(r"^\d+\s+", "", name)             # Remove leading numbers

    # Standardize common variations
    replacements = {
        "St.": "State",
        "Univ.": "",
        "University": "",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)

    name = re.sub(r"\s+", " ", name).strip()
    return name


def _add_lookup_key(lookup: dict, key, canonical: str):
    """Add multiple normalized variants of a key to the lookup dict."""
    if key is None or pd.isna(key):
        return

    key = str(key).strip()
    if not key:
        return

    lookup[key] = canonical
    lookup[key.lower()] = canonical

    nk = normalize_team_name(key)
    if nk:
        lookup[nk] = canonical
        lookup[nk.lower()] = canonical


def create_name_lookup(alias_df, source_columns):
    """
    Create a lookup dictionary from one or multiple source columns to canonical name.
    Always includes canonical->canonical mapping too.
    """
    if alias_df is None or alias_df.empty:
        return {}

    if isinstance(source_columns, str):
        source_columns = [source_columns]

    lookup = {}

    for _, row in alias_df.iterrows():
        canonical = row.get("canonical")
        if pd.isna(canonical) or not str(canonical).strip():
            continue
        canonical = str(canonical).strip()

        # Always map canonical to itself
        _add_lookup_key(lookup, canonical, canonical)

        # Map each source column value to canonical
        for col in source_columns:
            _add_lookup_key(lookup, row.get(col, ""), canonical)

    return lookup


def standardize_team_names(df, team_column, source="espn"):
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
        "espn": "espn",
        "net": "net",
        "kenpom": "kenpom",
        "bpi": "bpi",
        "ap": "ap",
        "sos": "sos",
        "warrennolan": "warrennolan",
    }

    source_col = source_col_map.get(source, source)

    # Key fix:
    # BPI comes from ESPN, but can appear as either ESPN-style short names or full mascot strings.
    # Use both columns to maximize match rate.
    if source == "bpi":
        lookup_cols = ["bpi", "espn"]
    else:
        lookup_cols = [source_col]

    lookup = create_name_lookup(alias_df, lookup_cols)

    def get_canonical(name):
        if pd.isna(name):
            return None

        name_str = str(name).strip()
        if not name_str:
            return None

        # Exact match
        if name_str in lookup:
            return lookup[name_str]
        if name_str.lower() in lookup:
            return lookup[name_str.lower()]

        # Normalized match
        normalized = normalize_team_name(name_str)
        if normalized in lookup:
            return lookup[normalized]
        if normalized and normalized.lower() in lookup:
            return lookup[normalized.lower()]

        # Return original if no match found
        return name_str

    df = df.copy()
    df["team"] = df[team_column].apply(get_canonical)
    return df


def get_unmatched_teams(df, team_column, source="espn"):
    """Find teams that couldn't be matched to canonical names."""
    alias_df = load_team_alias()
    if alias_df is None:
        return df[team_column].unique().tolist()

    canonical_names = set(alias_df["canonical"].dropna().unique())

    standardized = standardize_team_names(df, team_column, source)

    unmatched = []
    for orig, std in zip(df[team_column], standardized["team"]):
        if std not in canonical_names:
            unmatched.append({"original": orig, "attempted_match": std})

    return pd.DataFrame(unmatched).drop_duplicates()


if __name__ == "__main__":
    alias_df = load_team_alias()
    if alias_df is not None:
        print(f"Loaded {len(alias_df)} team aliases")
        print(f"Columns: {alias_df.columns.tolist()}")
    else:
        print("Could not load team alias file")
