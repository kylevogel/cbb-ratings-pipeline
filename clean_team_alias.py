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
    name = re.sub(r"\s*\(\d+\-\d+\)\s*$", "", name)
    name = re.sub(r"\s*#\d+\s*", "", name)
    name = re.sub(r"^\d+\s+", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def _clean_trailing_abbrev(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)

    if re.fullmatch(r"[A-Z]{2,5}", s):
        return s

    m = re.match(r"^(.*?)([A-Z]{2,5})$", s)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        if left and left != right:
            return left

    m2 = re.match(r"^(.*)\s+([A-Z]{2,5})$", s)
    if m2:
        left = m2.group(1).strip()
        right = m2.group(2).strip()
        if left and left != right:
            return left

    return s


def create_name_lookup(alias_df, source_column):
    if alias_df is None:
        return {}

    lookup = {}
    for _, row in alias_df.iterrows():
        canonical = row.get("canonical", "")
        source_name = row.get(source_column, "")

        if pd.isna(canonical) or not str(canonical).strip():
            continue
        if pd.isna(source_name) or not str(source_name).strip():
            continue

        raw = str(source_name).strip()
        low = raw.lower()
        norm = normalize_team_name(raw)
        norm_low = norm.lower() if norm else None
        cleaned = _clean_trailing_abbrev(raw)
        cleaned_low = cleaned.lower()

        lookup[raw] = canonical
        lookup[low] = canonical
        if norm:
            lookup[norm] = canonical
            lookup[norm_low] = canonical
        if cleaned:
            lookup[cleaned] = canonical
            lookup[cleaned_low] = canonical

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

        cleaned = _clean_trailing_abbrev(name_str)
        if cleaned in lookup:
            return lookup[cleaned]
        if cleaned.lower() in lookup:
            return lookup[cleaned.lower()]

        normalized = normalize_team_name(cleaned)
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
