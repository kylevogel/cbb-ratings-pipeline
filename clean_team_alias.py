#!/usr/bin/env python3
"""
Utility functions for standardizing team names across different data sources.
"""

import pandas as pd
import os
import re


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
    name = re.sub(r"\s+", " ", name).strip()

    m = re.match(r"^(.+?)([A-Z]{2,6})$", name)
    if m:
        base, abbr = m.group(1), m.group(2)
        if base.upper().endswith(abbr):
            name = base
        elif any(ch.islower() for ch in base) or any(ch in base for ch in " .&'()-"):
            name = base
        elif len(name) > 8:
            name = base

    replacements = {
        "St.": "State",
        "Univ.": "",
        "University": "",
        "  ": " ",
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    return re.sub(r"\s+", " ", name).strip()


def create_name_lookup(alias_df, source_column):
    if alias_df is None:
        return {}

    lookup = {}
    for _, row in alias_df.iterrows():
        canonical = row.get("canonical", "")
        source_name = row.get(source_column, "")

        if pd.notna(canonical) and canonical:
            if pd.notna(source_name) and str(source_name).strip():
                raw = str(source_name).strip()
                lookup[raw] = canonical
                lookup[raw.lower()] = canonical

                norm = normalize_team_name(raw)
                if norm:
                    lookup[norm] = canonical
                    lookup[norm.lower()] = canonical

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

    if source == "bpi":
        lookup_espn = create_name_lookup(alias_df, "espn")
        for k, v in lookup_espn.items():
            if k not in lookup:
                lookup[k] = v

    def get_canonical(name):
        if pd.isna(name):
            return None

        name_str = str(name).strip()

        if name_str in lookup:
            return lookup[name_str]
        if name_str.lower() in lookup:
            return lookup[name_str.lower()]

        normalized = normalize_team_name(name_str)
        if normalized and normalized in lookup:
            return lookup[normalized]
        if normalized and normalized.lower() in lookup:
            return lookup[normalized.lower()]

        return name_str

    out = df.copy()
    out["team"] = out[team_column].apply(get_canonical)
    return out


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
