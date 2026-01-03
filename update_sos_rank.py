import pandas as pd
import requests
from pathlib import Path
from datetime import date
from difflib import SequenceMatcher
from io import StringIO

SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"

ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data_raw"

TEAM_ALIAS = ROOT / "team_alias.csv"
OUT_SOS = DATA_RAW / "SOS_Rank.csv"
OUT_UNMATCHED = DATA_RAW / "unmatched_sos_teams.csv"
OUT_COLLISIONS = DATA_RAW / "sos_collisions.csv"


def clean(s: str) -> str:
    return str(s).replace("\xa0", " ").strip()


def norm_key(s: str) -> str:
    s = clean(s).lower()
    s = " ".join(s.split())
    return s


def ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, norm_key(a), norm_key(b)).ratio()


def load_sos_table() -> pd.DataFrame:
    html = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    tables = pd.read_html(StringIO(html))
    tbl = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if "rank" in cols and "team" in cols:
            tbl = t.copy()
            break
    if tbl is None:
        raise RuntimeError("Could not find a table with Rank and Team columns on WarrenNolan page.")

    team_col = [c for c in tbl.columns if str(c).strip().lower() == "team"][0]
    rank_col = [c for c in tbl.columns if str(c).strip().lower() == "rank"][0]

    tbl = tbl[[rank_col, team_col]].rename(columns={rank_col: "Rank", team_col: "Team"})
    tbl["Team"] = tbl["Team"].map(clean)

    def to_int(x):
        s = clean(x)
        s = "".join(ch for ch in s if ch.isdigit())
        return int(s) if s else None

    tbl["Rank"] = tbl["Rank"].map(to_int)
    tbl = tbl.dropna(subset=["Rank", "Team"])
    tbl["Rank"] = tbl["Rank"].astype(int)

    return tbl


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise RuntimeError("team_alias.csv missing standard_name column")

    if "sos_name" not in alias.columns:
        alias["sos_name"] = ""

    if "net_name" not in alias.columns:
        alias["net_name"] = ""

    stds = (
        alias["standard_name"]
        .astype(str)
        .map(clean)
        .loc[lambda s: s.ne("")]
        .drop_duplicates()
        .tolist()
    )

    sos_tbl = load_sos_table()

    src_teams = sos_tbl["Team"].tolist()
    src_key_to_team = {norm_key(t): t for t in src_teams}
    src_team_to_rank = dict(zip(sos_tbl["Team"], sos_tbl["Rank"]))

    def desired_for(std: str) -> str:
        row = alias.loc[alias["standard_name"].astype(str).map(clean).eq(std)]
        if row.empty:
            return std
        sos_name = clean(row.iloc[0].get("sos_name", ""))
        net_name = clean(row.iloc[0].get("net_name", ""))
        return sos_name or net_name or std

    used_source = {}
    matched_rows = []
    unmatched_rows = []
    collisions = []

    snapshot_date = date.today().isoformat()

    for std in stds:
        desired = desired_for(std)
        desired_k = norm_key(desired)

        chosen = None
        score = 0.0

        if desired_k in src_key_to_team:
            chosen = src_key_to_team[desired_k]
            score = 1.0

        if chosen is not None:
            if chosen in used_source:
                collisions.append(
                    {
                        "standard_name": std,
                        "desired_source": desired,
                        "matched_source": chosen,
                        "matched_rank": src_team_to_rank.get(chosen, ""),
                        "match_score": score,
                        "note": f"already used by {used_source[chosen]}",
                    }
                )
                unmatched_rows.append(
                    {"source_team": std, "suggested_standard": "", "match_score": 0.0}
                )
            else:
                used_source[chosen] = std
                matched_rows.append(
                    {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[chosen])}
                )

    for std in stds:
        if any(r["Team"] == std for r in matched_rows):
            continue

        desired = desired_for(std)

        best_team = None
        best_score = 0.0

        for t in src_teams:
            if t in used_source:
                continue
            sc = ratio(desired, t)
            if sc > best_score:
                best_score = sc
                best_team = t

        if best_team is None or best_score < 0.80:
            unmatched_rows.append(
                {"source_team": std, "suggested_standard": "", "match_score": float(best_score)}
            )
            continue

        used_source[best_team] = std
        matched_rows.append(
            {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[best_team])}
        )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(OUT_SOS, index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_df.to_csv(OUT_UNMATCHED, index=False)

    col_df = pd.DataFrame(
        collisions,
        columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"],
    )
    col_df.to_csv(OUT_COLLISIONS, index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")


if __name__ == "__main__":
    main()
