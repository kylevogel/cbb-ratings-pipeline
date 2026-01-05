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


def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def normalize_team_name(name: str | None) -> str | None:
    """
    Normalization used for both incoming names and alias keys.

    Key goals:
    - remove rankings/records noise
    - standardize punctuation/spaces
    - handle St. carefully (Saint vs State)
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None

    s = str(name).strip()

    # Remove records like (15-3)
    s = re.sub(r"\s*\(\d+\s*-\s*\d+\)\s*$", "", s)

    # Remove rankings like "#5" anywhere
    s = re.sub(r"#\s*\d+\s*", "", s)

    # Remove leading numeric rank like "12 Kansas"
    s = re.sub(r"^\s*\d+\s+", "", s)

    # Standardize apostrophes and periods spacing
    s = s.replace("’", "'")
    s = _collapse_spaces(s)

    # Remove "University" / "Univ." tokens (common in some feeds)
    s = re.sub(r"\bUniv\.\b", "", s)
    s = re.sub(r"\bUniversity\b", "", s)
    s = _collapse_spaces(s)

    # Handle "St." intelligently:
    # - If at the START: "St. John's" => "Saint John's"
    # - If at the END or followed by nothing: "Florida St." => "Florida State"
    # - If mid-string, we generally should not touch it.
    s = re.sub(r"^St\.\s+", "Saint ", s)
    s = re.sub(r"\bSt\.\s*$", "State", s)

    # Also handle "St " (no period) similarly (rare)
    s = re.sub(r"^St\s+", "Saint ", s)
    s = re.sub(r"\bSt\s*$", "State", s)

    return _collapse_spaces(s)


def create_name_lookup(alias_df: pd.DataFrame | None, source_column: str) -> dict:
    """
    Build lookup keys for:
    - exact source strings (raw + lower)
    - normalized source strings (normalized + lower)
    """
    if alias_df is None:
        return {}

    lookup: dict[str, str] = {}

    for _, row in alias_df.iterrows():
        canonical = row.get("canonical", None)
        if pd.isna(canonical) or not str(canonical).strip():
            continue
        canonical = str(canonical).strip()

        source_name = row.get(source_column, "")
        if pd.isna(source_name) or not str(source_name).strip():
            continue

        raw = str(source_name).strip()
        norm = normalize_team_name(raw)

        # raw keys
        lookup[raw] = canonical
        lookup[raw.lower()] = canonical

        # normalized keys
        if norm:
            lookup[norm] = canonical
            lookup[norm.lower()] = canonical

    return lookup


def _canonical_fallback_lookup(alias_df: pd.DataFrame | None) -> dict:
    """
    For sources that sometimes emit shorter names (espn/bpi), create a fallback mapping
    from normalized canonical -> canonical.
    """
    if alias_df is None or "canonical" not in alias_df.columns:
        return {}

    out: dict[str, str] = {}
    for c in alias_df["canonical"].dropna().unique():
        c = str(c).strip()
        if not c:
            continue
        norm = normalize_team_name(c)
        if norm:
            out[norm] = c
            out[norm.lower()] = c
    return out


def standardize_team_names(df: pd.DataFrame, team_column: str, source: str = "espn") -> pd.DataFrame:
    """
    Standardize team names in a dataframe.

    Adds standardized team names in a 'team' column.

    Args:
        df: DataFrame containing team names
        team_column: column containing team names in the df
        source: 'espn', 'net', 'kenpom', 'bpi', 'ap', 'sos' (and other custom keys)
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
    lookup = create_name_lookup(alias_df, source_col)

    # Helpful fallback for cases like BPI showing shorter names than your alias strings
    canonical_norm_lookup = _canonical_fallback_lookup(alias_df)

    def get_canonical(name):
        if pd.isna(name):
            return None

        raw = str(name).strip()
        if not raw:
            return None

        # exact/raw
        if raw in lookup:
            return lookup[raw]
        low = raw.lower()
        if low in lookup:
            return lookup[low]

        # normalized
        norm = normalize_team_name(raw)
        if norm:
            if norm in lookup:
                return lookup[norm]
            nlow = norm.lower()
            if nlow in lookup:
                return lookup[nlow]

            # Source-specific fallback:
            # For espn/bpi-like feeds, try matching normalized canonical
            if source in {"espn", "bpi", "ap"}:
                if norm in canonical_norm_lookup:
                    return canonical_norm_lookup[norm]
                if nlow in canonical_norm_lookup:
                    return canonical_norm_lookup[nlow]

        # no match
        return raw

    out = df.copy()
    out["team"] = out[team_column].apply(get_canonical)
    return out


def get_unmatched_teams(df: pd.DataFrame, team_column: str, source: str = "espn") -> pd.DataFrame:
    """Find teams that couldn't be matched to canonical names."""
    alias_df = load_team_alias()
    if alias_df is None or "canonical" not in alias_df.columns:
        return pd.DataFrame({"original": df[team_column].dropna().unique(), "attempted_match": None})

    canonical_names = set(alias_df["canonical"].dropna().astype(str).str.strip().unique())
    standardized = standardize_team_names(df, team_column, source)

    unmatched = []
    for orig, std in zip(df[team_column], standardized["team"]):
        if pd.isna(orig):
            continue
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
