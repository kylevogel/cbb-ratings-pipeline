import re
from pathlib import Path
from datetime import date
from difflib import SequenceMatcher
import pandas as pd
import requests
from bs4 import BeautifulSoup

AP_URL = "https://www.ncaa.com/rankings/basketball-men/d1/associated-press"
DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")
OUT_PATH = DATA_RAW / "AP_Rank.csv"
UNMATCHED_PATH = DATA_RAW / "unmatched_ap_teams.csv"

def norm(s):
    return str(s or "").strip().casefold().replace("'", "'")

def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def clean_team(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    
    r = requests.get(AP_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    table = None
    for t in soup.find_all("table"):
        head = t.find("thead")
        if not head:
            continue
        hdr = head.get_text(" ", strip=True).lower()
        if "rank" in hdr and ("school" in hdr or "team" in hdr):
            table = t
            break
    
    if table is None:
        raise RuntimeError("Could not find AP rankings table")
    
    body = table.find("tbody")
    if body is None:
        raise RuntimeError("AP rankings table missing tbody")
    
    rows = []
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue
        
        rank_text = tds[0].get_text(strip=True)
        team_text = clean_team(tds[1].get_text(strip=True))
        
        if not team_text:
            continue
        
        try:
            rank_val = int(rank_text)
        except:
            continue
        
        rows.append({"Team": team_text, "AP_Rank": rank_val})
    
    if not rows:
        raise RuntimeError("No AP rankings data extracted")
    
    ap_df = pd.DataFrame(rows)
    
    # Load alias file
    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    
    # Build AP mapping: AP alternate_name -> espn_name (standard name)
    ap_map = {}
    for _, row in alias.iterrows():
        if str(row.get("source", "")).strip() == "AP":
            espn = str(row.get("espn_name", "")).strip()
            alt = str(row.get("alternate_name", "")).strip()
            if espn and alt:
                ap_map[alt] = espn
    
    # Create source team to rank mapping
    src_team_to_rank = {}
    for _, row in ap_df.iterrows():
        team = str(row["Team"]).strip()
        rank = int(row["AP_Rank"])
        src_team_to_rank[team] = rank
    
    snapshot_date = date.today().isoformat()
    
    used = set()
    matched_rows = []
    unmatched_rows = []
    
    src_names = list(src_team_to_rank.keys())
    
    # Get unique ESPN names from alias file
    espn_names = alias[alias["espn_name"].str.strip() != ""]["espn_name"].str.strip().unique()
    
    for espn_name in espn_names:
        # Get all AP alternate names for this ESPN name
        ap_alternates = [alt for alt, esp in ap_map.items() if esp == espn_name]
        
        # Try exact match first
        chosen = None
        for desired in ap_alternates:
            exact = None
            for nm in src_names:
                if norm(nm) == norm(desired):
                    exact = nm
                    break
            if exact is not None and exact not in used:
                chosen = exact
                break
        
        # If no exact match, try fuzzy matching
        if chosen is None and ap_alternates:
            desired = ap_alternates[0]  # Use first alternate for fuzzy matching
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
            # Only record as unmatched if we have AP alternates for this team
            # (AP Poll only ranks top 25, so most teams won't have AP rankings)
            if ap_alternates:
                desired = ap_alternates[0]
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
                "AP_Rank": int(src_team_to_rank[chosen])
            }
        )
    
    # Create output DataFrame
    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "AP_Rank"])
    out_df.to_csv(OUT_PATH, index=False)
    
    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "best_match_found", "match_score"])
    um_df.to_csv(UNMATCHED_PATH, index=False)
    
    print(f"AP_Rank.csv written to {OUT_PATH}")
    print(f"unmatched_ap_teams.csv written to {UNMATCHED_PATH}")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")

if __name__ == "__main__":
    main()
