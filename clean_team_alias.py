#!/usr/bin/env python3
"""
Utility functions for standardizing team names across different data sources.
"""

import os
import re
import pandas as pd


def load_team_alias():
    alias_path = os.path.join(os.path.dirname(__file__), "team_alias.csv")
    if os.path.exists(alias_path):
        return pd.read_csv(alias_path)
    return None


def normalize_team_name(name):
    if pd.isna(name):
        return None

    name = str(name).strip()

    name = re.sub(r"\s*\(\d+-\d+\)\s*$", "", name)
    name = re.sub(r"\s*#\d+\s*", "", name)
    name = re.sub(r"^\d+\s+", "", name)

    replacements = {
        "St.": "State",
        "Univ.": "",
        "University": "",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)

    name = re.sub(r"\s+", " ", name).strip()
    return name


def create_name_lookup(alias_df, source_column):
    if alias_df is None or alias_df.empty:
        return {}

    lookup = {}

    def add_key(k, canonical):
        if k is None:
            return
        k = str(k).strip()
        if not k:
            return
        lookup[k] = canonical
        lookup[k.lower()] = canonical
        nk = normalize_team_name(k)
        if nk:
            lookup[nk] = canonical
            lookup[nk.lower()] = canonical

    for _, row in alias_df.iterrows():
        canonical = row.get("canonical")
        if pd.isna(canonical) or not str(canonical).strip():
            continue
        canonical = str(canonical).strip()

        add_key(canonical, canonical)

        source_name = row.get(source_column, "")
        if pd.notna(source_name) and str(source_name).strip():
            add_key(source_name, canonical)

    return lookup


def standardize_team_names(df, team_column, source="espn"):
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
    lookup = create_name_lookup(alias_df, source_col)

    def get_canonical(name):
        if pd.isna(name):
            return None

        name_str = str(name).strip()
        if not name_str:
            return None

        if name_str in lookup:
            return lookup[name_str]
        if name_str.lower() in lookup:
            return lookup[name_str.lower()]

        normalized = normalize_team_name(name_str)
        if normalized in lookup:
            return lookup[normalized]
        if normalized and normalized.lower() in lookup:
            return lookup[normalized.lower()]

        return name_str

    df = df.copy()
    df["team"] = df[team_column].apply(get_canonical)
    return df


def get_unmatched_teams(df, team_column, source="espn"):
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
