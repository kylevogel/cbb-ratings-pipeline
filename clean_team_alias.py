#!/usr/bin/env python3
"""
Utility functions for standardizing team names across different data sources.
"""

import os
import re
import unicodedata
import pandas as pd


def load_team_alias():
    alias_path = os.path.join(os.path.dirname(__file__), "team_alias.csv")
    if os.path.exists(alias_path):
        return pd.read_csv(alias_path)
    return None


def _fold_accents(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def _strip_trailing_abbrev(name: str) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    s = re.sub(r"\s+", " ", s)

    s = re.sub(r"([a-z0-9\)\]\}'\.])([A-Z]{2,6})$", r"\1 \2", s)

    tokens = s.split(" ")
    if len(tokens) >= 2:
        last = tokens[-1]
        prev = tokens[-2]

        if last == prev:
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]{2,6}", last):
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]{1,3}-[A-Z]{1,3}", last):
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]&[A-Z]", last):
            tokens = tokens[:-1]

    return " ".join(tokens).strip()


def normalize_team_name(name):
    if pd.isna(name):
        return None

    name = str(name).strip()
    name = name.replace("’", "'")
    name = _strip_trailing_abbrev(name)

    name = re.sub(r"\s*\(\d+-\d+\)\s*$", "", name)
    name = re.sub(r"\s*#\d+\s*", " ", name)
    name = re.sub(r"^\d+\s+", "", name)

    name = re.sub(r"^St\.\s+", "Saint ", name)

    name = re.sub(r"\sSt\.\s*$", " State", name)

    name = name.replace("University", "")
    name = name.replace("Univ.", "")
    name = re.sub(r"\s+", " ", name).strip()

    return name


def create_name_lookup(alias_df, source_column):
    if alias_df is None:
        return {}

    lookup = {}

    def add_key(k: str, canonical: str):
        if not k:
            return
        k = str(k).strip()
        if not k:
            return
        lookup[k] = canonical
        lookup[k.lower()] = canonical

        n = normalize_team_name(k)
        if n:
            lookup[n] = canonical
            lookup[n.lower()] = canonical

        f = _fold_accents(k)
        if f:
            lookup[f] = canonical
            lookup[f.lower()] = canonical

        if n:
            fn = _fold_accents(n)
            if fn:
                lookup[fn] = canonical
                lookup[fn.lower()] = canonical

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

        raw = str(name).strip()
        raw = raw.replace("’", "'")
        raw = _strip_trailing_abbrev(raw)

        if raw in lookup:
            return lookup[raw]
        if raw.lower() in lookup:
            return lookup[raw.lower()]

        n = normalize_team_name(raw)
        if n and n in lookup:
            return lookup[n]
        if n and n.lower() in lookup:
            return lookup[n.lower()]

        f = _fold_accents(raw)
        if f in lookup:
            return lookup[f]
        if f.lower() in lookup:
            return lookup[f.lower()]

        if n:
            fn = _fold_accents(n)
            if fn in lookup:
                return lookup[fn]
            if fn.lower() in lookup:
                return lookup[fn.lower()]

        return raw

    out = df.copy()
    out["team"] = out[team_column].apply(get_canonical)
    return out


def get_unmatched_teams(df, team_column, source="espn"):
    alias_df = load_team_alias()
    if alias_df is None:
        return df[team_column].unique().tolist()

    canonical_names = set(alias_df["canonical"].dropna().astype(str).str.strip().unique())
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
