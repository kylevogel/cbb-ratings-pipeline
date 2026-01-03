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


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [str(c[-1]).strip() for c in df.columns]
    else:
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
    return df


def fetch_source() -> pd.DataFrame:
    html = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    tables = pd.read_html(StringIO(html))

    best = None
    best_score = -1

    for t in tables:
        t = _flatten_columns(t)
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

    best = _flatten_columns(best)
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


def best_match(target_key: str, choices: list[str]) -> tuple[str, float]:
    if not choices:
        return ("", 0.0)

    best_choice = ""
    best_score = 0.0
    for c in choices:
        sc = SequenceMatcher(None, target_key, c).ratio()
        if sc > best_score:
            best_score = sc
            best_choice = c

    return (best_choice, float(best_score))


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    src = fetch_source()
    if src.empty:
        pd.DataFrame(columns=["snapshot_date", "Team", "SoS"]).to_csv(DATA_RAW / "SOS_Rank.csv", index=False)
        pd.DataFrame(
            columns=["standard_name", "desired_source", "suggested_source", "suggested_team", "match_score"]
        ).to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)
        pd.DataFrame(
            columns=["normalized_key", "team_a", "team_b"]
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

    src_norm_to_team: dict[str, str] = {}
    collisions = []
    for t in src["Team"].tolist():
        k = norm(t)
        if k in src_norm_to_team and src_norm_to_team[k] != t:
            collisions.append((k, src_norm_to_team[k], t))
        else:
            src_norm_to_team[k] = t

    src_keys = list(src_norm_to_team.keys())

    alias = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise ValueError("team_alias.csv must have a 'standard_name' column")
    if "sos_name" not in alias.columns:
        raise ValueError("team_alias.csv must have a 'sos_name' column")

    snapshot_date = pd.Timestamp.now().date().isoformat()

    matched_rows = []
    unmatched_rows = []

    for _, r in alias.iterrows():
        std = str(r.get("standard_name", "")).strip()
        desired = str(r.get("sos_name", "")).strip() or std
        key = norm(desired)

        if key in src_norm_to_team:
            chosen_team = src_norm_to_team[key]
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[chosen_team])})
        else:
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
            sug_key, sug_score = best_match(key, src_keys)
            sug_team = src_norm_to_team.get(sug_key, "") if sug_key else ""
            unmatched_rows.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "suggested_source": sug_key,
                    "suggested_team": sug_team,
                    "match_score": round(sug_score, 4),
                }
            )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    pd.DataFrame(
        unmatched_rows,
        columns=["standard_name", "desired_source", "suggested_source", "suggested_team", "match_score"],
    ).to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    pd.DataFrame(
        [{"normalized_key": k, "team_a": a, "team_b": b} for (k, a, b) in collisions],
        columns=["normalized_key", "team_a", "team_b"],
    ).to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    matched_ct = (out_df["SoS"].astype(str).str.strip().ne("") & out_df["SoS"].astype(str).str.lower().ne("nan")).sum()
    unmatched_ct = len(out_df) - int(matched_ct)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src)}")
    print(f"Matched: {matched_ct}")
    print(f"Unmatched: {unmatched_ct}")
    print(f"Collisions: {len(collisions)}")


if __name__ == "__main__":
    main()
