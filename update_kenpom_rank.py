from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
from difflib import SequenceMatcher
import os
import pandas as pd
import requests

URL = "https://kenpom.com/index.php?y=2026"
DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")
OUT_PATH = DATA_RAW / "KenPom_Rank.csv"
UNMATCHED_PATH = DATA_RAW / "unmatched_kenpom_teams.csv"

def norm(s):
    return str(s or "").strip().casefold().replace("'", "'")

def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def main() -> None:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    cookie = os.getenv("KENPOM_COOKIE", "").strip()
    if cookie:
        headers["Cookie"] = cookie
    
    try:
        r = requests.get(URL, headers=headers, timeout=45)
    except Exception as e:
        print(f"KenPom request failed: {e}")
        return
    
    if r.status_code in (401, 403):
        print(f"KenPom blocked the request (HTTP {r.status_code}). Skipping update.")
        return
    
    r.raise_for_status()
    
    tables = pd.read_html(StringIO(r.text))
    t = None
    for df in tables:
        cols = [str(c).strip().lower() for c in df.columns]
        if "rk" in cols or "rank" in cols:
            t = df
            break
    
    if t is None:
        print("Could not find KenPom table.")
        return
    
    cols = {str(c).strip().lower(): c for c in t.columns}
    team_col = None
    for k in ["team", "school"]:
        if k in cols:
            team_col = cols[k]
            break
    if team_col is None:
        team_col = t.columns[0]
    
    rank_col = None
    for k in ["rk", "rank"]:
        if k in cols:
            rank_col = cols[k]
            break
    
    wl_col = None
    for k in ["w-l", "wl"]:
        if k in cols:
            wl_col = cols[k]
            break
    
    if rank_col is None:
        print("Could not locate KenPom rank column.")
        return
    
    keep = [team_col, rank_col] + ([wl_col] if wl_col is not None else [])
    kenpom_df = t[keep].copy()
    kenpom_df.columns = ["Team", "KenPom_Rank"] + (["W-L"] if wl_col is not None else [])
    kenpom_df["Team"] = kenpom_df["Team"].astype(str).str.strip()
    kenpom_df["KenPom_Rank"] = pd.to_numeric(kenpom_df["KenPom_Rank"], errors="coerce")
    kenpom_df = kenpom_df.dropna(subset=["KenPom_Rank"])
    kenpom_df["KenPom_Rank"] = kenpom_df["KenPom_Rank"].astype(int)
    
    # Load alias file
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    
    # Build KenPom mapping: KenPom alternate_name -> espn_name (standard name)
    kenpom_map = {}
    for _, row in alias.iterrows():
        if str(row.get("source", "")).strip() == "KenPom":
            espn = str(row.get("espn_name", "")).strip()
            alt = str(row.get("alternate_name", "")).strip()
            if espn and alt:
                kenpom_map[alt] = espn
    
    # Create source team to rank mapping
    src_team_to_rank = {}
    src_team_to_wl = {}
    for _, row in kenpom_df.iterrows():
        team = str(row["Team"]).strip()
        rank = int(row["KenPom_Rank"])
        src_team_to_rank[team] = rank
        if "W-L" in row:
            src_team_to_wl[team] = str(row["W-L"]).strip()
    
    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    snapshot_date = now_et.strftime("%Y-%m-%d")
    
    used = set()
    matched_rows = []
    unmatched_rows = []
    
    src_names = list(src_team_to_rank.keys())
    
    # Get unique ESPN names from alias file
    espn_names = alias[alias["espn_name"].str.strip() != ""]["espn_name"].str.strip().unique()
    
    for espn_name in espn_names:
        # Get all KenPom alternate names for this ESPN name
        kenpom_alternates = [alt for alt, esp in kenpom_map.items() if esp == espn_name]
        
        # Try exact match first
        chosen = None
        for desired in kenpom_alternates:
            exact = None
            for nm in src_names:
                if norm(nm) == norm(desired):
                    exact = nm
                    break
            if exact is not None and exact not in used:
                chosen = exact
                break
        
        # If no exact match, try fuzzy matching
        if chosen is None and kenpom_alternates:
            desired = kenpom_alternates[0]  # Use first alternate for fuzzy matching
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
            if kenpom_alternates:
                desired = kenpom_alternates[0]
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
        row_data = {
            "snapshot_date": snapshot_date,
            "Team": espn_name,
            "KenPom_Rank": int(src_team_to_rank[chosen])
        }
        if chosen in src_team_to_wl:
            row_data["W-L"] = src_team_to_wl[chosen]
        
        matched_rows.append(row_data)
    
    # Create output DataFrame
    cols_out = ["snapshot_date", "Team", "KenPom_Rank"]
    if src_team_to_wl:
        cols_out.append("W-L")
    
    out_df = pd.DataFrame(matched_rows, columns=cols_out)
    out_df.to_csv(OUT_PATH, index=False)
    
    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "best_match_found", "match_score"])
    um_df.to_csv(UNMATCHED_PATH, index=False)
    
    print(f"KenPom_Rank.csv written to {OUT_PATH}")
    print(f"unmatched_kenpom_teams.csv written to {UNMATCHED_PATH}")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")
    print(",".join(out_df.columns.tolist()))
    print(out_df.head(5).to_csv(index=False).strip())

if __name__ == "__main__":
    main()
