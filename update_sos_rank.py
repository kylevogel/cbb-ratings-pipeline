import re
from io import StringIO
from pathlib import Path
import pandas as pd
import requests


DATA_RAW = Path("data_raw")
TEAM_ALIAS = Path("team_alias.csv")

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ").strip().lower()
    s = s.replace("’", "'")
    s = s.replace("&", "and")
    s = s.replace(".", " ")
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fetch_source() -> pd.DataFrame:
    html = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    tables = pd.read_html(StringIO(html))

    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        if "rank" in cols or "rk" in cols:
            score += 3
        if "team" in cols:
            score += 3
        if "sos" in cols:
            score += 1
        if score > best_score:
            best_score = score
            best = t

    if best is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    colmap = {str(c).strip().lower(): c for c in best.columns}
    if "rank" not in colmap and "rk" not in colmap:
        return pd.DataFrame(columns=["Rank", "Team"])
    rank_col = colmap.get("rank", colmap.get("rk"))
    team_col = colmap.get("team")
    if team_col is None:
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

    src = fetch_source()
    if src.empty:
        out_df = pd.DataFrame(columns=["snapshot_date", "Team", "SoS"])
        out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)
        pd.DataFrame(columns=["source_team", "suggested_standard", "match_score"]).to_csv(
            DATA_RAW / "unmatched_sos_teams.csv", index=False
        )
        pd.DataFrame(
            columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"]
        ).to_csv(DATA_RAW / "sos_collisions.csv", index=False)
        print("SOS_Rank.csv")
        print("unmatched_sos_teams.csv")
        print("sos_collisions.csv")
        print("Pulled source teams: 0")
        print("Matched: 0")
        print("Unmatched: 0")
        print("Collisions: 0")
        return

    src_team_to_rank = dict(zip(src["Team"].tolist(), src["Rank"].tolist()))
    src_norm_to_team = {}
    collisions = []
    for t in src["Team"].tolist():
        nt = norm(t)
        if nt in src_norm_to_team and src_norm_to_team[nt] != t:
            collisions.append((nt, src_norm_to_team[nt], t))
            continue
        src_norm_to_team[nt] = t

    alias = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise ValueError("team_alias.csv must have a 'standard_name' column")
    if "sos_name" not in alias.columns:
        alias["sos_name"] = ""
        alias.to_csv(TEAM_ALIAS, index=False)

    snapshot_date = pd.Timestamp.now().date().isoformat()

    matched_rows = []
    unmatched_rows = []

    for _, r in alias.iterrows():
        std = str(r.get("standard_name", "")).strip()
        desired = str(r.get("sos_name", "")).strip() or std
        key = norm(desired)

        if key in src_norm_to_team:
            chosen = src_norm_to_team[key]
            matched_rows.append(
                {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[chosen])}
            )
        else:
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            unmatched_rows.append({"source_team": desired, "suggested_standard": "", "match_score": 0.0})

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_df.to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(
        [{"standard_name": "", "desired_source": a, "matched_source": b, "matched_rank": "", "match_score": 0.0, "note": "normalized collision"}
         for _, a, b in collisions],
        columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"],
    )
    col_df.to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src)}")
    print(f"Matched: {(out_df['SoS'].astype(str).str.strip().ne('') & out_df['SoS'].astype(str).str.lower().ne('nan')).sum()}")
    print(f"Unmatched: {(out_df['SoS'].astype(str).str.strip().eq('') | out_df['SoS'].astype(str).str.lower().eq('nan')).sum()}")
    print(f"Collisions: {len(col_df)}")


if __name__ == "__main__":
    main()
