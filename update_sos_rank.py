import re
from io import StringIO
from pathlib import Path
from difflib import SequenceMatcher

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


def best_match(target_key: str, keys: list[str]) -> tuple[str, float]:
    best_k = ""
    best_s = 0.0
    for k in keys:
        s = SequenceMatcher(None, target_key, k).ratio()
        if s > best_s:
            best_s = s
            best_k = k
    return best_k, float(best_s)


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise ValueError("team_alias.csv must have a 'standard_name' column")
    if "sos_name" not in alias.columns:
        raise ValueError("team_alias.csv must have a 'sos_name' column")

    src = fetch_source()

    if src.empty:
        pd.DataFrame(columns=["snapshot_date", "Team", "SoS"]).to_csv(DATA_RAW / "SOS_Rank.csv", index=False)
        pd.DataFrame(columns=["standard_name", "desired_source", "suggested_team", "match_score"]).to_csv(
            DATA_RAW / "unmatched_sos_teams.csv", index=False
        )
        pd.DataFrame(columns=["Rank", "Team"]).to_csv(DATA_RAW / "sos_source_snapshot.csv", index=False)
        print("Pulled source teams: 0")
        print("Matched: 0")
        print("Unmatched: 0")
        return

    src.to_csv(DATA_RAW / "sos_source_snapshot.csv", index=False)

    src_team_to_rank = dict(zip(src["Team"].tolist(), src["Rank"].tolist()))

    src_norm_to_team = {}
    collisions = 0
    for t in src["Team"].tolist():
        k = norm(t)
        if k in src_norm_to_team and src_norm_to_team[k] != t:
            collisions += 1
        else:
            src_norm_to_team[k] = t

    src_keys = list(src_norm_to_team.keys())

    snapshot_date = pd.Timestamp.now().date().isoformat()

    matched_rows = []
    unmatched_rows = []

    for _, r in alias.iterrows():
        std = str(r.get("standard_name", "")).strip()
        desired = str(r.get("sos_name", "")).strip() or std
        key = norm(desired)

        if key in src_norm_to_team:
            chosen = src_norm_to_team[key]
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[chosen])})
        else:
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            sug_key, sug_score = best_match(key, src_keys)
            sug_team = src_norm_to_team.get(sug_key, "") if sug_key else ""
            unmatched_rows.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "suggested_team": sug_team,
                    "match_score": round(sug_score, 4),
                }
            )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    pd.DataFrame(
        unmatched_rows, columns=["standard_name", "desired_source", "suggested_team", "match_score"]
    ).to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    matched_ct = (out_df["SoS"].astype(str).str.strip().ne("") & out_df["SoS"].astype(str).str.lower().ne("nan")).sum()
    print(f"Pulled source teams: {len(src)}")
    print(f"Matched: {int(matched_ct)}")
    print(f"Unmatched: {len(out_df) - int(matched_ct)}")
    print(f"Collisions (normalized): {collisions}")
    print("Wrote:")
    print(" - data_raw/SOS_Rank.csv")
    print(" - data_raw/sos_source_snapshot.csv")
    print(" - data_raw/unmatched_sos_teams.csv")


if __name__ == "__main__":
    main()
