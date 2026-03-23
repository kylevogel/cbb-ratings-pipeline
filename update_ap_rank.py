"""
update_ap_rank.py
Pulls AP Poll rankings and writes a clean CSV with canonical team names.

Fixes applied:
  1. Maps AP-specific team name variants to canonical names used by rest of pipeline
     (most importantly: "UConn" -> "Connecticut", "Miami (FL)" -> "Miami")
  2. Logs a warning if any expected ranks 1-25 are missing from the source data
     (e.g. the current data has no rank 24 — that is an upstream data gap)
"""

import os
import pandas as pd


# ---------------------------------------------------------------------------
# AP-specific name normalisation
# ---------------------------------------------------------------------------

_AP_TO_CANONICAL: dict[str, str] = {
    "UConn":        "Connecticut",
    "Miami (FL)":   "Miami",
    "Miami (Oh)":   "Miami (OH)",
    "Saint Mary's (CA)": "Saint Mary's",
    "N.C. State":   "NC State",
    "Ole Miss":     "Ole Miss",       # already canonical
    "Iowa St.":     "Iowa State",
    "Michigan St.": "Michigan State",
    "Ohio St.":     "Ohio State",
}


def normalise_ap_name(raw: str) -> str:
    return _AP_TO_CANONICAL.get(raw.strip(), raw.strip())


# ---------------------------------------------------------------------------
# Main loading logic
# ---------------------------------------------------------------------------

def fetch_ap_rankings() -> pd.DataFrame:
    raw_path = "data_raw/ap_rankings.csv"
    if not os.path.exists(raw_path):
        print(f"WARNING: {raw_path} not found – skipping AP")
        return pd.DataFrame(columns=["ap_rank", "team_ap"])

    df = pd.read_csv(raw_path)
    df["team_ap"] = df["team_ap"].astype(str).apply(normalise_ap_name)
    df["ap_rank"] = pd.to_numeric(df["ap_rank"], errors="coerce")
    return df


def main():
    print("Updating AP rankings...")
    df = fetch_ap_rankings()
    if df.empty:
        print("No AP data – nothing written.")
        return

    # Warn about any missing ranks (e.g. rank 24 is missing from source today)
    present = set(df["ap_rank"].dropna().astype(int).tolist())
    expected = set(range(1, 26))
    missing = sorted(expected - present)
    if missing:
        print(f"WARNING: AP source data is missing rank(s): {missing}")
        print("  This is an upstream data gap — not a pipeline bug.")
        print("  The dashboard will show no team at those positions.")

    os.makedirs("data_raw", exist_ok=True)
    out_path = "data_raw/ap_rankings.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} AP rankings to {out_path}")


if __name__ == "__main__":
    main()
