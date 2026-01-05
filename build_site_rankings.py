import os
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

import pandas as pd


ALIAS_PATHS = ["team_alias.csv", "data_raw/team_alias.csv"]
GAMES_PATHS = [
    "data_processed/games_with_ranks.csv",
    "data_raw/games_2024.csv",
    "data_raw/games.csv",
]

OUT_CSV = "docs/rankings.csv"
OUT_UPDATED = "docs/last_updated.csv"
OUT_JSON = "docs/rankings.json"


def first_existing(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def normalize_columns(cols):
    out = []
    for c in cols:
        s = str(c).replace("\ufeff", "").strip()
        out.append(s)
    return out


def read_alias() -> pd.DataFrame:
    p = first_existing(ALIAS_PATHS)
    if not p:
        raise RuntimeError("Could not find team_alias.csv")

    df = pd.read_csv(p, dtype=str, encoding="utf-8-sig").fillna("")
    df.columns = normalize_columns(df.columns)

    lower_map = {c.lower().strip(): c for c in df.columns}

    std_col = None
    for key in ["standard_name", "standard name", "standard", "team", "school", "name"]:
        if key in lower_map:
            std_col = lower_map[key]
            break
    if std_col is None:
        std_col = df.columns[0]

    df = df.rename(columns={std_col: "standard_name"})
    df["standard_name"] = df["standard_name"].astype(str).str.strip()
    df = df[df["standard_name"].str.len() > 0].copy()
    df = df.drop_duplicates(subset=["standard_name"], keep="first").reset_index(drop=True)
    return df


def build_lookup(alias: pd.DataFrame) -> Dict[str, str]:
    lookup = {}
    for _, row in alias.iterrows():
        std = str(row["standard_name"]).strip()
        if not std:
            continue
        lookup[std] = std
        for c in alias.columns:
            v = str(row.get(c, "")).strip()
            if v:
                lookup[v] = std
    return lookup


def load_rank_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["name", "rank"])

    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    df.columns = normalize_columns(df.columns)
    cols = list(df.columns)

    name_col = None
    rank_col = None

    for c in cols:
        lc = c.lower().strip()
        if lc.endswith("_name") or lc in ["team", "school", "name"]:
            name_col = c
            break
    if name_col is None and cols:
        name_col = cols[0]

    for c in cols:
        lc = c.lower().strip()
        if lc.endswith("_rank") or lc in ["rank", "rk"]:
            rank_col = c
            break
    if rank_col is None and len(cols) >= 2:
        rank_col = cols[1]

    out = pd.DataFrame()
    out["name"] = df[name_col].astype(str).str.strip()
    out["rank"] = pd.to_numeric(df[rank_col], errors="coerce")
    out = out.dropna(subset=["name", "rank"])
    out = out[out["name"].str.len() > 0]
    out["rank"] = out["rank"].astype(int)
    out = out.drop_duplicates(subset=["name"], keep="first").reset_index(drop=True)
    return out


def compute_records(games: pd.DataFrame, lookup: Dict[str, str]) -> Dict[str, str]:
    if games.empty:
        return {}

    games.columns = normalize_columns(games.columns)

    if "Team" in games.columns and "Win?" in games.columns:
        t = games.copy()
        t["Team"] = t["Team"].astype(str).str.strip().map(lambda x: lookup.get(x, x))
        w = t["Win?"].astype(str).str.strip().str.upper()

        wins = t[w == "W"].groupby("Team").size()
        losses = t[w == "L"].groupby("Team").size()

        out = {}
        for team in set(t["Team"].dropna().tolist()):
            ww = int(wins.get(team, 0))
            ll = int(losses.get(team, 0))
            out[team] = f"{ww}-{ll}"
        return out

    return {}


def main() -> int:
    alias = read_alias()
    lookup = build_lookup(alias)

    games_path = first_existing(GAMES_PATHS)
    games = pd.read_csv(games_path, dtype=str).fillna("") if games_path else pd.DataFrame()
    record_map = compute_records(games, lookup)

    net_df = load_rank_csv("data_raw/net.csv")
    kp_df = load_rank_csv("data_raw/kenpom.csv")
    bpi_df = load_rank_csv("data_raw/bpi.csv")
    ap_df = load_rank_csv("data_raw/ap.csv")
    sos_df = load_rank_csv("data_raw/sos.csv")

    def to_map(df: pd.DataFrame) -> Dict[str, int]:
        return {str(r["name"]).strip(): int(r["rank"]) for _, r in df.iterrows()}

    net_map_src = to_map(net_df)
    kp_map_src = to_map(kp_df)
    bpi_map_src = to_map(bpi_df)
    ap_map_src = to_map(ap_df)
    sos_map_src = to_map(sos_df)

    def rank_for(team_std: str, col: str, src_map: Dict[str, int]) -> Optional[int]:
        if col in alias.columns:
            v = alias.loc[alias["standard_name"] == team_std, col]
            if not v.empty:
                key = str(v.iloc[0]).strip()
                if key and key in src_map:
                    return int(src_map[key])
        if team_std in src_map:
            return int(src_map[team_std])
        return None

    rows = []
    for team_std in alias["standard_name"].tolist():
        net = rank_for(team_std, "net_name", net_map_src)
        kp = rank_for(team_std, "kenpom_name", kp_map_src)
        bpi = rank_for(team_std, "bpi_name", bpi_map_src)
        ap = rank_for(team_std, "ap_name", ap_map_src) if "ap_name" in alias.columns else rank_for(team_std, "standard_name", ap_map_src)
        sos = rank_for(team_std, "sos_name", sos_map_src) if "sos_name" in alias.columns else rank_for(team_std, "standard_name", sos_map_src)

        rec = record_map.get(team_std, "0-0")

        rows.append(
            {
                "team": team_std,
                "record": rec,
                "ap": ap if ap is not None else "",
                "net": net if net is not None else "",
                "kenpom": kp if kp is not None else "",
                "bpi": bpi if bpi is not None else "",
                "sos": sos if sos is not None else "",
            }
        )

    out = pd.DataFrame(rows)

    net_num = pd.to_numeric(out["net"], errors="coerce")
    kp_num = pd.to_numeric(out["kenpom"], errors="coerce")
    bpi_num = pd.to_numeric(out["bpi"], errors="coerce")

    avg_val = pd.concat([net_num, kp_num, bpi_num], axis=1)
    avg_val = avg_val.mean(axis=1, skipna=False)

    out["avg"] = avg_val.rank(method="min", ascending=True)
    out["avg"] = out["avg"].astype("Int64")

    out = out[["team", "record", "ap", "avg", "net", "kenpom", "bpi", "sos"]]

    os.makedirs("docs", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    out.to_json(OUT_JSON, orient="records")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    pd.DataFrame([{"last_updated": now}]).to_csv(OUT_UPDATED, index=False)

    print(f"Wrote {len(out)} rows -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
