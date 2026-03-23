"""
update_kenpom_rank.py
Pulls KenPom rankings and writes a clean CSV with canonical team names.

Fixes applied:
  1. Strips trailing conference-seed suffixes from KenPom names (e.g. "Michigan 1" -> "Michigan")
  2. Normalises KenPom abbreviations to the canonical names used by every other source
     (e.g. "Iowa St." -> "Iowa State", "Miami FL" -> "Miami", "SIUE" -> "SIU Edwardsville")
  3. AP source uses "UConn" - this file outputs "Connecticut" to match canonical; the AP
     normaliser handles its own alias instead (see update_ap_rank.py).
"""

import re
import pandas as pd

# ---------------------------------------------------------------------------
# KenPom-specific name normalisation
# ---------------------------------------------------------------------------

# Step 1 – strip trailing " <digit(s)>" conference-rank suffixes that KenPom
# appends to disambiguate teams sharing a city/nickname within a conference.
# e.g. "Michigan 1", "Iowa St. 2", "Prairie View A&M 16"
_SUFFIX_RE = re.compile(r'\s+\d+$')


def _strip_suffix(name: str) -> str:
    return _SUFFIX_RE.sub('', name.strip())


# Step 2 – map every KenPom abbreviation/shorthand to the canonical team name
# used across NET, BPI, AP, and SOS in this pipeline.
_KENPOM_TO_CANONICAL: dict[str, str] = {
    # "St." -> "State" schools
    "Iowa St.":             "Iowa State",
    "Michigan St.":         "Michigan State",
    "Ohio St.":             "Ohio State",
    "Utah St.":             "Utah State",
    "Arizona St.":          "Arizona State",
    "Florida St.":          "Florida State",
    "Boise St.":            "Boise State",
    "San Diego St.":        "San Diego State",
    "Illinois St.":         "Illinois State",
    "Kansas St.":           "Kansas State",
    "Colorado St.":         "Colorado State",
    "Montana St.":          "Montana State",
    "Morehead St.":         "Morehead State",
    "South Dakota St.":     "South Dakota State",
    "North Dakota St.":     "North Dakota State",
    "Weber St.":            "Weber State",
    "Arkansas St.":         "Arkansas State",
    "Kennesaw St.":         "Kennesaw State",
    "Jacksonville St.":     "Jacksonville State",
    "Murray St.":           "Murray State",
    "Fresno St.":           "Fresno State",
    "Penn St.":             "Penn State",
    "Washington St.":       "Washington State",
    "Portland St.":         "Portland State",
    "Oklahoma St.":         "Oklahoma State",
    "Appalachian St.":      "Appalachian State",
    "East Tennessee St.":   "East Tennessee State",
    "New Mexico St.":       "New Mexico State",
    "Wright St.":           "Wright State",
    "Youngstown St.":       "Youngstown State",
    "Idaho St.":            "Idaho State",
    "Ball St.":             "Ball State",
    "Kent St.":             "Kent State",
    "Morgan St.":           "Morgan State",
    "Alcorn St.":           "Alcorn State",
    "Mississippi Valley St.": "Mississippi Valley State",
    "Alabama St.":          "Alabama State",
    "Long Beach St.":       "Long Beach State",
    "Norfolk St.":          "Norfolk State",
    "Georgia St.":          "Georgia State",
    "Texas St.":            "Texas State",
    "Jackson St.":          "Jackson State",
    "Tennessee St.":        "Tennessee State",
    "Sacramento St.":       "Sacramento State",
    "Northwestern St.":     "Northwestern State",
    "Cal St. Fullerton":    "Cal State Fullerton",
    "Cal St. Bakersfield":  "Cal State Bakersfield",
    "Wichita St.":          "Wichita State",

    # Conference/style differences
    "N.C. State":           "NC State",
    "Miami FL":             "Miami",
    "Miami OH":             "Miami (OH)",
    "Mississippi":          "Ole Miss",
    "Sam Houston St.":      "Sam Houston",
    "UT Rio Grande Valley": "UTRGV",
    "Illinois Chicago":     "UIC",
    "Texas A&M Corpus Chris": "Texas A&M-CC",
    "CSUN":                 "Cal State Northridge",
    "SIUE":                 "SIU Edwardsville",
    "Nebraska Omaha":       "Omaha",
    "Bethune Cookman":      "Bethune-Cookman",
    "Tennessee Martin":     "UT Martin",
    "Southeast Missouri":   "Southeast Missouri State",
    "Grambling St.":        "Grambling",
    "Loyola MD":            "Loyola Maryland",
    "Louisiana Monroe":     "ULM",
    "Saint Francis":        "Saint Francis (PA)",
    "Gardner Webb":         "Gardner-Webb",
    "IU Indy":              "IUPUI",
    "St. Bonaventure":      "Saint Bonaventure",
    "Furman":               "Furman",       # already fine, listed for completeness
    "Prairie View A&M":     "Prairie View A&M",  # fine after suffix strip
}


def normalise_kenpom_name(raw: str) -> str:
    """Strip conference suffix then apply abbreviation map."""
    name = _strip_suffix(raw)
    return _KENPOM_TO_CANONICAL.get(name, name)


# ---------------------------------------------------------------------------
# Main scraping / loading logic  (replace the body below with your actual
# scraper; the normalisation call at the end is what matters)
# ---------------------------------------------------------------------------

def fetch_kenpom_rankings() -> pd.DataFrame:
    """
    Load kenpom_rankings.csv (written by your scraper) and return a clean
    DataFrame with columns [kenpom_rank, team_kenpom, record] where
    team_kenpom has been normalised to canonical names.
    """
    import os

    raw_path = "data_raw/kenpom_rankings.csv"
    if not os.path.exists(raw_path):
        print(f"WARNING: {raw_path} not found – skipping KenPom")
        return pd.DataFrame(columns=["kenpom_rank", "team_kenpom", "record"])

    df = pd.read_csv(raw_path)

    # Normalise team names in-place
    df["team_kenpom"] = df["team_kenpom"].astype(str).apply(normalise_kenpom_name)

    # Ensure rank is numeric
    df["kenpom_rank"] = pd.to_numeric(df["kenpom_rank"], errors="coerce")

    return df


def main():
    print("Updating KenPom rankings...")
    df = fetch_kenpom_rankings()
    if df.empty:
        print("No KenPom data – nothing written.")
        return

    import os
    os.makedirs("data_raw", exist_ok=True)
    out_path = "data_raw/kenpom_rankings.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} KenPom rankings to {out_path}")

    # Spot-check a few known tricky names
    checks = ["Iowa State", "Michigan State", "NC State", "Miami", "Miami (OH)",
              "Ole Miss", "UTRGV", "UIC", "SIU Edwardsville", "Cal State Northridge",
              "Omaha", "IUPUI", "ULM", "Gardner-Webb", "Saint Bonaventure"]
    found = set(df["team_kenpom"].tolist())
    print("\nName normalisation spot-check:")
    for name in checks:
        status = "✓" if name in found else "✗ MISSING"
        print(f"  {status}  {name}")


if __name__ == "__main__":
    main()
