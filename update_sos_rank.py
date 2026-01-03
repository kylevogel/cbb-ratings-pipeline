from pathlib import Path
from datetime import date
from io import StringIO
from difflib import SequenceMatcher
import re
import pandas as pd
import requests

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"

DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")

def norm(s):
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ").strip().casefold()
    s = s.replace("’", "'")
    s = re.sub(r"[\.']", "", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def pick_table(tables):
    for t in tables:
        cols = [norm(c) for c in list(t.columns)]
        if "rank" in cols and ("team" in cols or "school" in cols):
            return t
    return None

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise SystemExit("team_alias.csv missing standard_name column")
    if "sos_name" not in alias.columns:
        alias["sos_name"] = ""
        alias.to_csv(ALIAS_PATH, index=False)

    r = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = pick_table(tables)
    if t is None:
        raise SystemExit("Could not find SoS table on WarrenNolan page")

    cols_norm = {c: norm(c) for c in t.columns}
    rank_col = None
    team_col = None
    for c, cn in cols_norm.items():
        if cn == "rank":
            rank_col = c
        if cn in ("team", "school"):
            team_col = c

    if rank_col is None or team_col is None:
        raise SystemExit("SoS table missing Rank/Team columns")

    t = t[[rank_col, team_col]].copy()
    t[rank_col] = pd.to_numeric(t[rank_col], errors="coerce")
    t[team_col] = t[team_col].astype(str).str.replace("\xa0", " ").str.strip()
    t = t.dropna(subset=[rank_col, team_col])
    t[rank_col] = t[rank_col].astype(int)

    src_norm_to_rank = {}
    src_norm_to_orig = {}
    for _, row in t.iterrows():
        team = str(row[team_col]).replace("\xa0", " ").strip()
        rk = int(row[rank_col])
        k = norm(team)
        if not k:
            continue
        if k not in src_norm_to_rank:
            src_norm_to_rank[k] = rk
            src_norm_to_orig[k] = team

    src_keys = list(src_norm_to_rank.keys())

    used = {}
    matched_rows = []
    unmatched_rows = []
    collisions = []

    snapshot_date = date.today().isoformat()

    for _, ar in alias.iterrows():
        std = str(ar.get("standard_name", "")).strip()
        if not std:
            continue

        desired = str(ar.get("sos_name", "")).strip() or std

        k = norm(desired)
        chosen = None
        best_score = -1.0

        if k in src_norm_to_rank and k not in used:
            chosen = k
            best_score = 1.0
        else:
            for sk in src_keys:
                if sk in used:
                    continue
                sc = SequenceMatcher(None, norm(desired), sk).ratio()
                if sc > best_score:
                    best_score = sc
                    chosen = sk

            if best_score < 0.86:
                chosen = None

        if chosen is None:
            best_any = None
            best_any_score = -1.0
            for sk in src_keys:
                sc = SequenceMatcher(None, norm(desired), sk).ratio()
                if sc > best_any_score:
                    best_any_score = sc
                    best_any = sk

            suggested = src_norm_to_orig.get(best_any, "") if best_any else ""
            unmatched_rows.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "suggested_source": suggested,
                    "match_score": round(best_any_score if best_any_score >= 0 else 0.0, 6),
                }
            )
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            continue

        if chosen in used:
            collisions.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "matched_source": src_norm_to_orig.get(chosen, ""),
                    "matched_rank": src_norm_to_rank.get(chosen, ""),
                    "match_score": round(best_score, 6),
                    "note": f"already used by {used[chosen]}",
                }
            )
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            continue

        used[chosen] = std
        matched_rows.append(
            {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_norm_to_rank[chosen])}
        )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(
        unmatched_rows,
        columns=["standard_name", "desired_source", "suggested_source", "match_score"],
    )
    um_df.to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(
        collisions,
        columns=[
            "standard_name",
            "desired_source",
            "matched_source",
            "matched_rank",
            "match_score",
            "note",
        ],
    )
    col_df.to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    matched_count = int(out_df["SoS"].astype(str).str.strip().replace("nan", "").ne("").sum())

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_norm_to_rank)}")
    print(f"Matched: {matched_count}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")

if __name__ == "__main__":
    main()
