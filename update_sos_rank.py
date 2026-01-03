import io
import json
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"

ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data_raw"
TEAM_ALIAS = ROOT / "team_alias.csv"

def clean(s):
    return str(s).replace("\xa0", " ").strip()

def norm(s):
    return clean(s).lower()

def best_ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def load_source():
    html = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    tables = pd.read_html(io.StringIO(html))

    tbl = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "team" in cols and "rank" in cols:
            tbl = t.copy()
            break

    if tbl is None:
        raise SystemExit("Could not find a table with columns including Team and Rank")

    team_col = [c for c in tbl.columns if str(c).strip().lower() == "team"][0]
    rank_col = [c for c in tbl.columns if str(c).strip().lower() == "rank"][0]

    tbl = tbl[[rank_col, team_col]].copy()
    tbl[team_col] = tbl[team_col].astype(str).map(clean)

    tbl[rank_col] = pd.to_numeric(tbl[rank_col], errors="coerce")
    tbl = tbl.dropna(subset=[rank_col])
    tbl[rank_col] = tbl[rank_col].astype(int)

    src_team_to_rank = dict(zip(tbl[team_col].tolist(), tbl[rank_col].tolist()))
    return src_team_to_rank

def load_alias():
    df = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "sos_name" not in df.columns:
        df["sos_name"] = ""
        df.to_csv(TEAM_ALIAS, index=False)
    return df

def desired_for_row(r):
    for k in ["sos_name", "net_name", "standard_name"]:
        v = clean(r.get(k, ""))
        if v:
            return v
    return ""

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    src_team_to_rank = load_source()
    src_teams = list(src_team_to_rank.keys())
    src_norm_to_src = {}
    for s in src_teams:
        src_norm_to_src.setdefault(norm(s), s)

    alias = load_alias()
    alias = alias[alias["standard_name"].astype(str).map(clean).ne("")]

// ensure unique
    alias["standard_name"] = alias["standard_name"].map(clean)
    alias["sos_name"] = alias["sos_name"].map(clean)
    alias["net_name"] = alias["net_name"].map(clean)

    standards = alias["standard_name"].tolist()
    desired_map = {std: desired_for_row(alias.loc[i]) for i, std in enumerate(standards)}

    candidates = []
    for std in standards:
        desired = desired_map.get(std, "")
        if not desired:
            continue

        if desired in src_team_to_rank:
            candidates.append((2.0, std, desired, desired, "exact"))
            continue

        dn = norm(desired)
        if dn in src_norm_to_src:
            s = src_norm_to_src[dn]
            candidates.append((1.9, std, desired, s, "norm_exact"))
            continue

        best_s = None
        best_sc = -1.0
        for s in src_teams:
            sc = best_ratio(desired, s)
            if sc > best_sc:
                best_sc = sc
                best_s = s
        candidates.append((best_sc, std, desired, best_s, "fuzzy"))

    candidates.sort(reverse=True, key=lambda x: x[0])

    assigned_std = set()
    assigned_src = {}
    chosen = {}
    collisions = []

    for sc, std, desired, s, note in candidates:
        if std in assigned_std:
            continue
        if s is None or not s:
            continue
        if s in assigned_src:
            collisions.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "matched_source": s,
                    "matched_rank": src_team_to_rank.get(s, ""),
                    "match_score": sc,
                    "note": f"already used by {assigned_src[s]}",
                }
            )
            continue
        assigned_std.add(std)
        assigned_src[s] = std
        chosen[std] = (s, sc, note)

    unmatched_rows = []
    matched_rows = []
    snapshot_date = date.today().isoformat()

    for std in standards:
        if std in chosen:
            s, sc, note = chosen[std]
            matched_rows.append(
                {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[s])}
            )
        else:
            desired = desired_map.get(std, "")
            best_s = None
            best_sc = -1.0
            for s in src_teams:
                sc = best_ratio(desired or std, s)
                if sc > best_sc:
                    best_sc = sc
                    best_s = s
            unmatched_rows.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "suggested_source": best_s,
                    "match_score": best_sc,
                }
            )
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["standard_name", "desired_source", "suggested_source", "match_score"])
    um_df.to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(collisions, columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"])
    col_df.to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {(out_df['SoS'].astype(str).str.strip().ne('') ).sum()}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")

if __name__ == "__main__":
    main()
