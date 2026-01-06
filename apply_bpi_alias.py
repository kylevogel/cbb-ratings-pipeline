#!/usr/bin/env python3
import pandas as pd

# Put bpi_name_map.csv in repo root (same folder as team_alias.csv)
MAP_PATH = "bpi_name_map.csv"
ALIAS_PATH = "team_alias.csv"

m = pd.read_csv(MAP_PATH)
if not {"canonical_team", "bpi_name"}.issubset(m.columns):
    raise SystemExit("bpi_name_map.csv must have columns: canonical_team,bpi_name")

a = pd.read_csv(ALIAS_PATH)

# Identify canonical column in team_alias.csv (prefer 'team', else first column)
canonical_col = "team" if "team" in a.columns else a.columns[0]

# Ensure bpi column exists (this is what standardize_team_names(..., source='bpi') should use)
if "bpi" not in a.columns:
    a["bpi"] = ""

merged = a.merge(m, left_on=canonical_col, right_on="canonical_team", how="left")

# Fill bpi from mapping where available
a["bpi"] = merged["bpi_name"].fillna(a["bpi"])

a.to_csv(ALIAS_PATH, index=False)
print(f"Updated {ALIAS_PATH}: filled bpi names for {merged['bpi_name'].notna().sum()} teams")
