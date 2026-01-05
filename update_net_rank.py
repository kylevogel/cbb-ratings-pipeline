import pandas as pd
import requests
from pathlib import Path
from datetime import date
from difflib import SequenceMatcher

URL = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"
DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")
OUT_PATH = DATA_RAW / "NET_Rank.csv"
UNMATCHED_PATH = DATA_RAW / "unmatched_net_teams.csv"

def norm(s):
    return str(s or "").strip().casefold().replace("'", "'")

def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    
    r = requests.get(
        URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    r.raise_for_status()
    
    tables = pd.read_html(r.text)
    df = None
    for t in tables:
        cols = {c.lower().strip(): c for c in t.columns}
        if "rank" in cols and "school" in cols and "record" in cols:
            df = t.rename(columns={cols["rank"]: "NET_Rank", cols["school"]: "Team", cols["record"]: "Record"}).copy()
            break
    
    if df is None or df.empty:
        raise RuntimeError("Could not find NET rankings table with Rank/School/Record columns.")
    
    df["Team"] = df["Team"].astype(str).str.strip()
    df["Record"] = df["Record"].astype(str).str.strip()
    df["NET_Rank"] = pd.to_numeric(df["NET_Rank"], errors="coerce")
    df = df.dropna(subset=["NET_Rank"])
    df["NET_Rank"] = df["NET_Rank"].astype(int)
    df = df[["Team", "NET_Rank", "Record"]].drop_duplicates(subset=["Team"], keep="first")
    
    # Load alias file
    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    
    # Build NET mapping: NET alternate_name -> espn_name (standard name)
    net_map = {}
    for _, row in alias.iterrows():
        if str(row.get("source", "")).strip() == "NET":
            espn = str(row.get("espn_name", "")).strip()
            alt = str(row.get("alternate_name", "")).strip()
            if espn and alt:
                net_map[alt] = espn
    
    # Create source team to rank/record mapping
    src_team_to_rank = {}
    src_team_to_record = {}
    for _, row in df.iterrows():
        team = str(row["Team"]).strip()
        rank = int(row["NET_Rank"])
        record = str(row["Record"]).strip()
        src_team_to_rank[team] = rank
        src_team_to_record[team] = record
    
    snapshot_date = date.today().isoformat()
    
    used = set()
    matched_rows = []
    unmatched_rows = []
    
    src_names = list(src_team_to_rank.keys())
    
    # Get unique ESPN names from alias file
    espn_names = alias[alias["espn_name"].str.strip() != ""]["espn_name"].str.strip().unique()
    
    for espn_name in espn_names:
        # Get all NET alternate names for this ESPN name
        net_alternates = [alt for alt, esp in net_map.items() if esp == espn_name]
        
        # Try exact match first
        chosen = None
        for desired in net_alternates:
            exact = None
            for nm in src_names:
                if norm(nm) == norm(desired):
                    exact = nm
                    break
            if exact is not None and exact not in used:
                chosen = exact
                break
        
        # If no exact match, try fuzzy matching
        if chosen is None and net_alternates:
            desired = net_alternates[0]  # Use first alternate for fuzzy matching
            best_nm = None
            best_sc = -1.0
            for nm in src_names:
                if nm in used:
                    continue
                sc = ratio(desired, nm)
                if sc > best_sc:
                    best_sc = sc
                    best_nm = nm
            if best_nm is not None and best_sc >= 0.86:
                chosen = best_nm
        
        if chosen is None:
            # Record as unmatched
            if net_alternates:
                desired = net_alternates[0]
                best_nm = None
                best_sc = -1.0
                for nm in src_names:
                    sc = ratio(desired, nm)
                    if sc > best_sc:
                        best_sc = sc
                        best_nm = nm
                unmatched_rows.append(
                    {
                        "source_team": desired,
                        "suggested_standard": espn_name,
                        "best_match_found": best_nm if best_nm else "",
                        "match_score": round(best_sc if best_sc >= 0 else 0.0, 6),
                    }
                )
            continue
        
        used.add(chosen)
        matched_rows.append(
            {
                "snapshot_date": snapshot_date,
                "Team": espn_name,
                "NET_Rank": int(src_team_to_rank[chosen]),
                "Record": src_team_to_record[chosen]
            }
        )
    
    # Create output DataFrame
    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "NET_Rank", "Record"])
    out_df.to_csv(OUT_PATH, index=False)
    
    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "best_match_found", "match_score"])
    um_df.to_csv(UNMATCHED_PATH, index=False)
    
    print(f"NET_Rank.csv written to {OUT_PATH}")
    print(f"unmatched_net_teams.csv written to {UNMATCHED_PATH}")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")

if __name__ == "__main__":
    main()
