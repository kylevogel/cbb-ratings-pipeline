import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import pandas as pd

ALIAS_CANDIDATES = ["team_alias.csv", "data_raw/team_alias.csv"]
GAMES_PATH = "data_processed/games_with_ranks.csv"

OUT_CSV = "docs/rankings.csv"
OUT_JSON = "docs/rankings.json"
OUT_UPDATED = "docs/last_updated.csv"


def _first_existing(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def _read_alias() -> pd.DataFrame:
    p = _first_existing(ALIAS_CANDIDATES)
    if not p:
        raise RuntimeError("Could not find team_alias.csv (expected at repo root or data_raw/)")
    df = pd.read_csv(p, dtype=str).fillna("")
    if "standard_name" not in df.columns:
        raise RuntimeError("team_alias.csv must include a standard_name column")
    df["standard_name"] = df["standard_name"].astype(str).str.strip()
    df = df[df["standard_name"].str.len() > 0].copy()
    df = df.drop_duplicates(subset=["standard_name"], keep="first").reset_index(drop=True)
    return df


def _guess_team_rank_cols(df: pd.DataFrame) -> Tuple[str, str]:
    cols = list(df.columns)
    team_col = None
    rank_col = None
    for c in cols:
        if "team" in str(c).lower() or str(c).lower().endswith("_name"):
            team_col = c
            break
    for c in cols:
        lc = str(c).lower()
        if "rank" in lc or lc in {"rk"}:
            rank_col = c
            break
    if team_col is None or rank_col is None:
        raise RuntimeError(f"Could not infer team/rank cols from {cols}")
    return team_col, rank_col


def _load_rank(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, dtype=str).fillna("")
    if df.empty:
        return df
    team_col, rank_col = _guess_team_rank_cols(df)
    out = pd.DataFrame()
    out["name"] = df[team_col].astype(str).str.strip()
    out["rank"] = pd.to_numeric(df[rank_col], errors="coerce")
    out = out.dropna(subset=["name"]).drop_duplicates(subset=["name"])
    out = out[out["name"].str.len() > 0]
    out = out.dropna(subset=["rank"])
    out["rank"] = out["rank"].astype(int)
    return out.reset_index(drop=True)


def _record_from_games(games: pd.DataFrame) -> pd.DataFrame:
    g = games.copy()
    if "Team" not in g.columns or "Win?" not in g.columns:
        return pd.DataFrame(columns=["team", "record"])
    g["Team"] = g["Team"].astype(str).str.strip()
    g["Win?"] = g["Win?"].astype(str).str.strip().str.upper()
    wins = g[g["Win?"] == "W"].groupby("Team").size()
    losses = g[g["Win?"] == "L"].groupby("Team").size()
    teams = sorted(set(g["Team"].dropna().tolist()))
    rows = []
    for t in teams:
        w = int(wins.get(t, 0))
        l = int(losses.get(t, 0))
        rows.append((t, f"{w}-{l}"))
    return pd.DataFrame(rows, columns=["team", "record"])


def main() -> int:
    alias = _read_alias()
    standard = alias["standard_name"].tolist()

    if os.path.exists(GAMES_PATH):
        games = pd.read_csv(GAMES_PATH, dtype=str).fillna("")
    else:
        games = pd.DataFrame()

    rec = _record_from_games(games)

    net = _load_rank("data_raw/net.csv")
    kenpom = _load_rank("data_raw/kenpom.csv")
    bpi = _load_rank("data_raw/bpi.csv")
    sos = _load_rank("data_raw/sos.csv")
    ap = _load_rank("data_raw/ap.csv")

    def map_rank(rank_df: Optional[pd.DataFrame], name_col: str) -> Dict[str, Optional[int]]:
        if rank_df is None or rank_df.empty:
            return {}
        src_to_rank = dict(zip(rank_df["name"], rank_df["rank"]))
        m = {}
        if name_col in alias.columns:
            for _, row in alias.iterrows():
                s = row["standard_name"]
                src = str(row.get(name_col, "")).strip()
                if src and src in src_to_rank:
                    m[s] = int(src_to_rank[src])
        return m

    net_map = map_rank(net, "net_name")
    kenpom_map = map_rank(kenpom, "kenpom_name")
    bpi_map = map_rank(bpi, "bpi_name")
    sos_map = map_rank(sos, "sos_name") if "sos_name" in alias.columns else map_rank(sos, "standard_name")
    ap_map = map_rank(ap, "ap_name") if "ap_name" in alias.columns else map_rank(ap, "standard_name")

    rec_map = dict(zip(rec["team"], rec["record"]))

    rows = []
    for t in standard:
        net_r = net_map.get(t)
        kp_r = kenpom_map.get(t)
        bpi_r = bpi_map.get(t)
        sos_r = sos_map.get(t)
        ap_r = ap_map.get(t)
        record = rec_map.get(t, "")
        rows.append(
            {
                "team": t,
                "record": record,
                "ap": ap_r if ap_r is not None else "",
                "net": net_r if net_r is not None else "",
                "kenpom": kp_r if kp_r is not None else "",
                "bpi": bpi_r if bpi_r is not None else "",
                "sos": sos_r if sos_r is not None else "",
            }
        )

    out = pd.DataFrame(rows)

    def _to_num(series):
        return pd.to_numeric(series, errors="coerce")

    out["_net"] = _to_num(out["net"])
    out["_kenpom"] = _to_num(out["kenpom"])
    out["_bpi"] = _to_num(out["bpi"])

    out["_avg_metric"] = out[["_net", "_kenpom", "_bpi"]].mean(axis=1, skipna=True)
    out["avg"] = out["_avg_metric"].rank(method="min", ascending=True).astype("Int64")

    out = out.drop(columns=["_net", "_kenpom", "_bpi", "_avg_metric"])
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
