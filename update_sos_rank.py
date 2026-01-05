import re
from io import StringIO
from pathlib import Path
from difflib import SequenceMatcher
from datetime import date

import pandas as pd
import requests

DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")
OUT_PATH = DATA_RAW / "SOS_Rank.csv"
UNMATCHED_PATH = DATA_RAW / "unmatched_sos_teams.csv"

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ").strip().lower()
    s = s.replace("'", "'")
    s = s.replace("&", "and")
    s = s.replace(".", " ")
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(c[-1]).strip() for c in df.columns]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def fetch_source() -> pd.DataFrame:
    html = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    tables = pd.read_html(StringIO(html))

    best = None
    best_score = -1

    for t in tables:
        t = flatten_columns(t)
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        if "rank" in cols or "rk" in cols:
            score += 3
        if "team" in cols or "school" in cols:
            score += 3
        if "sos" in cols:
            score += 1
        if score > best_score:
            best_score = score
            best = t

    if best is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    best = flatten_columns(best)
    colmap = {str(c).strip().lower(): c for c in best.columns}

    rank_col = colmap.get("rank", colmap.get("rk"))
    team_col = colmap.get("team", colmap.get("school"))

    if rank_col is None or team_col is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    out = best[[rank_col, team_col]].copy()
    out.columns = ["Rank", "Team"]

    out["Team"] = out["Team"].astype(str).map(lambda x: x.replace("\xa0", " ").strip())
    out["Rank"] = out["Rank"].astype(str).str.replace("#", "", regex=False).str.strip()
    out = out[out["Rank"].str.match(r"^\d+$", na=False)].copy()
    out["Rank"] = out["Rank"].astype(int)

    out = out.drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)
    return out


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    # Load alias file
    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    
    # Build SoS mapping: SoS alternate_name -> espn_name (standard name)
    sos_map = {}
    for _, row in alias.iterrows():
        if str(row.get("source", "")).strip() == "SoS":
            espn = str(row.get("espn_name", "")).strip()
            alt = str(row.get("alternate_name", "")).strip()
            if espn and alt:
                sos_map[alt] = espn

    src = fetch_source()

    if src.empty:
        pd.DataFrame(columns=["snapshot_date", "Team", "SoS"]).to_csv(OUT_PATH, index=False)
        pd.DataFrame(columns=["source_team", "suggested_standard", "best_match_found", "match_score"]).to_csv(
            UNMATCHED_PATH, index=False
        )
        pd.DataFrame(columns=["Rank", "Team"]).to_csv(DATA_RAW / "sos_source_snapshot.csv", index=False)
        print("Pulled source teams: 0")
        print("Matched: 0")
        print("Unmatched: 0")
        return

    src.to_csv(DATA_RAW / "sos_source_snapshot.csv", index=False)

    # Create source team to rank mapping
    src_team_to_rank = {}
    for _, row in src.iterrows():
        team = str(row["Team"]).strip()
        rank = int(row["Rank"])
        src_team_to_rank[team] = rank

    snapshot_date = date.today().isoformat()

    used = set()
    matched_rows = []
    unmatched_rows = []

    src_names = list(src_team_to_rank.keys())

    # Get unique ESPN names from alias file
    espn_names = alias[alias["espn_name"].str.strip() != ""]["espn_name"].str.strip().unique()

    for espn_name in espn_names:
        # Get all SoS alternate names for this ESPN name
        sos_alternates = [alt for alt, esp in sos_map.items() if esp == espn_name]
        
        # Try exact match first
        chosen = None
        for desired in sos_alternates:
            exact = None
            for nm in src_names:
                if norm(nm) == norm(desired):
                    exact = nm
                    break
            if exact is not None and exact not in used:
                chosen = exact
                break
        
        # If no exact match, try fuzzy matching
        if chosen is None and sos_alternates:
            desired = sos_alternates[0]  # Use first alternate for fuzzy matching
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
            if sos_alternates:
                desired = sos_alternates[0]
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
                "SoS": int(src_team_to_rank[chosen])
            }
        )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(OUT_PATH, index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "best_match_found", "match_score"])
    um_df.to_csv(UNMATCHED_PATH, index=False)

    print(f"SOS_Rank.csv written to {OUT_PATH}")
    print(f"unmatched_sos_teams.csv written to {UNMATCHED_PATH}")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")
    print("Wrote:")
    print(f" - {OUT_PATH}")
    print(" - data_raw/sos_source_snapshot.csv")
    print(f" - {UNMATCHED_PATH}")


if __name__ == "__main__":
    main()
