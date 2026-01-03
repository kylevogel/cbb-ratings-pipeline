import csv
import datetime as dt
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


DATA_RAW = Path("data_raw")
TEAM_ALIAS = Path("team_alias.csv")
RANKINGS_JSON = Path("docs/data/rankings_current.json")

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def clean(s):
    if s is None:
        return ""
    return str(s).replace("\xa0", " ").strip()


def norm(s):
    s = clean(s).lower()
    for ch in [".", ",", "'", "’", "(", ")", "[", "]"]:
        s = s.replace(ch, "")
    s = s.replace("&", "and")
    s = " ".join(s.split())
    return s


def load_alias_sos_map():
    if not TEAM_ALIAS.exists():
        return {}
    with TEAM_ALIAS.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        out = {}
        for row in r:
            std = clean(row.get("standard_name", ""))
            if not std:
                continue
            sos = clean(row.get("sos_name", ""))
            out[std] = sos
        return out


def load_standard_teams():
    d = __import__("json").load(RANKINGS_JSON.open("r", encoding="utf-8"))
    teams = []
    for r in d.get("rows", []):
        t = clean(r.get("Team", ""))
        if t:
            teams.append(t)
    return teams


def fetch_source():
    html = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).text
    tables = pd.read_html(StringIO(html))
    best = None
    best_score = -1
    for t in tables:
        cols = [clean(c) for c in t.columns]
        lcols = [c.lower() for c in cols]
        score = 0
        if "rank" in lcols:
            score += 3
        if "team" in lcols:
            score += 3
        if "sos" in lcols:
            score += 1
        if score > best_score:
            best_score = score
            best = t
    if best is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    cols_map = {clean(c).lower(): c for c in best.columns}
    if "rank" not in cols_map or "team" not in cols_map:
        return pd.DataFrame(columns=["Rank", "Team"])

    out = best[[cols_map["rank"], cols_map["team"]]].copy()
    out.columns = ["Rank", "Team"]
    out["Team"] = out["Team"].map(clean)
    out["Rank"] = out["Rank"].astype(str).str.replace("#", "", regex=False).str.strip()
    out = out[out["Rank"].str.match(r"^\d+$", na=False)].copy()
    out["Rank"] = out["Rank"].astype(int)
    out = out[out["Team"].astype(str).str.len() > 0].copy()
    out = out.sort_values("Rank").drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)
    return out


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    src = fetch_source()
    src_team_to_rank = {clean(r.Team): int(r.Rank) for r in src.itertuples(index=False)}

    src_norm_to_team = {}
    for t in src_team_to_rank.keys():
        n = norm(t)
        if n and n not in src_norm_to_team:
            src_norm_to_team[n] = t

    alias_sos = load_alias_sos_map()
    standard_teams = load_standard_teams()

    snapshot_date = dt.date.today().isoformat()

    matched_rows = []
    unmatched_rows = []
    collisions = []

    desired_to_source = {}

    for std in standard_teams:
        desired = alias_sos.get(std, "")
        desired = desired if desired else std
        desired = clean(desired)

        source_team = None
        rank = None

        if desired in src_team_to_rank:
            source_team = desired
            rank = src_team_to_rank[desired]
        else:
            n = norm(desired)
            if n in src_norm_to_team:
                source_team = src_norm_to_team[n]
                rank = src_team_to_rank[source_team]

        if rank is None:
            unmatched_rows.append({"source_team": desired, "suggested_standard": "", "match_score": 0.0})
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            continue

        if source_team in desired_to_source and desired_to_source[source_team] != std:
            collisions.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "matched_source": source_team,
                    "matched_rank": rank,
                    "match_score": 1.0,
                    "note": f"also matched by {desired_to_source[source_team]}",
                }
            )
        else:
            desired_to_source[source_team] = std

        matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": int(rank)})

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_df.to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(
        collisions,
        columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"],
    )
    col_df.to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {(out_df['SoS'].astype(str).str.strip().replace('nan','').ne('').sum())}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")


if __name__ == "__main__":
    main()
