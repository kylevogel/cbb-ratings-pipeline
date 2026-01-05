import glob
import os
from datetime import datetime, timezone

import pandas as pd


def read_csv_if_exists(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def build_alias_lookup(team_alias: pd.DataFrame) -> dict[str, str]:
    lookup = {}
    for _, r in team_alias.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        lookup[std] = std
        for c in team_alias.columns:
            if c == "standard_name":
                continue
            v = r.get(c)
            if pd.isna(v):
                continue
            v = str(v).strip()
            if v:
                lookup[v] = std
    return lookup


def newest_games_file() -> str | None:
    cands = sorted(glob.glob("data_raw/games*.csv")) + sorted(glob.glob("data_processed/games*.csv"))
    if not cands:
        return None
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cands[0]


def compute_records(games: pd.DataFrame, name_to_standard: dict[str, str]) -> pd.DataFrame:
    if games.empty:
        return pd.DataFrame(columns=["standard_name", "wins", "losses", "record"])

    cols = set(games.columns)

    def norm_team(s: str) -> str:
        s = str(s).strip()
        return name_to_standard.get(s, s)

    if {"Team", "Team_Score", "Opponent_Score"}.issubset(cols):
        df = games.copy()
        df["standard_name"] = df["Team"].map(norm_team)
        ts = pd.to_numeric(df["Team_Score"], errors="coerce")
        os_ = pd.to_numeric(df["Opponent_Score"], errors="coerce")
        played = ts.notna() & os_.notna()
        df = df.loc[played].copy()
        win = ts.loc[played] > os_.loc[played]
        rec = (
            df.assign(win=win.values)
            .groupby("standard_name", as_index=False)
            .agg(wins=("win", "sum"), games=("win", "size"))
        )
        rec["losses"] = rec["games"] - rec["wins"]
        rec["record"] = rec["wins"].astype(int).astype(str) + "-" + rec["losses"].astype(int).astype(str)
        return rec[["standard_name", "wins", "losses", "record"]]

    if {"Home", "Away", "Home_Score", "Away_Score"}.issubset(cols):
        df = games.copy()
        hs = pd.to_numeric(df["Home_Score"], errors="coerce")
        as_ = pd.to_numeric(df["Away_Score"], errors="coerce")
        played = hs.notna() & as_.notna()
        df = df.loc[played].copy()
        home_win = hs.loc[played] > as_.loc[played]

        home_rows = pd.DataFrame(
            {
                "standard_name": df["Home"].map(norm_team),
                "win": home_win.values,
            }
        )
        away_rows = pd.DataFrame(
            {
                "standard_name": df["Away"].map(norm_team),
                "win": (~home_win).values,
            }
        )
        both = pd.concat([home_rows, away_rows], ignore_index=True)
        rec = both.groupby("standard_name", as_index=False).agg(wins=("win", "sum"), games=("win", "size"))
        rec["losses"] = rec["games"] - rec["wins"]
        rec["record"] = rec["wins"].astype(int).astype(str) + "-" + rec["losses"].astype(int).astype(str)
        return rec[["standard_name", "wins", "losses", "record"]]

    return pd.DataFrame(columns=["standard_name", "wins", "losses", "record"])


def rank_avg_metric(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    a = pd.to_numeric(tmp["net"], errors="coerce")
    b = pd.to_numeric(tmp["bpi"], errors="coerce")
    c = pd.to_numeric(tmp["kenpom"], errors="coerce")
    tmp["avg_value"] = pd.concat([a, b, c], axis=1).mean(axis=1, skipna=False)
    tmp["avg"] = tmp["avg_value"].rank(method="min", ascending=True)
    tmp["avg"] = tmp["avg"].astype("Int64")
    return tmp


def main() -> int:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    team_alias = pd.read_csv("team_alias.csv")
    if "standard_name" not in team_alias.columns:
        raise RuntimeError("team_alias.csv must include a standard_name column")

    name_lookup = build_alias_lookup(team_alias)

    base = team_alias[["standard_name"]].dropna().copy()
    base["standard_name"] = base["standard_name"].astype(str).str.strip()

    net = read_csv_if_exists("data_raw/net.csv")
    kenpom = read_csv_if_exists("data_raw/kenpom.csv")
    bpi = read_csv_if_exists("data_raw/bpi.csv")
    ap = read_csv_if_exists("data_raw/ap.csv")
    sos = read_csv_if_exists("data_raw/sos.csv")

    def map_source(df: pd.DataFrame, name_col: str, rank_col: str) -> pd.DataFrame:
        if df.empty or name_col not in df.columns or rank_col not in df.columns:
            return pd.DataFrame(columns=["standard_name", rank_col])
        out = df[[name_col, rank_col]].copy()
        out["standard_name"] = out[name_col].astype(str).str.strip().map(lambda x: name_lookup.get(x, x))
        out[rank_col] = pd.to_numeric(out[rank_col], errors="coerce").astype("Int64")
        return out[["standard_name", rank_col]].dropna(subset=["standard_name"])

    net_m = map_source(net, "net_name", "net")
    kenpom_m = map_source(kenpom, "kenpom_name", "kenpom")
    bpi_m = map_source(bpi, "bpi_name", "bpi")
    ap_m = map_source(ap, "ap_name", "ap")
    sos_m = map_source(sos, "sos_name", "sos")

    games_path = newest_games_file()
    games = pd.read_csv(games_path) if games_path else pd.DataFrame()
    rec = compute_records(games, name_lookup)

    out = base.merge(rec[["standard_name", "record"]], on="standard_name", how="left")
    out["record"] = out["record"].fillna("0-0")

    out = out.merge(ap_m, on="standard_name", how="left")
    out = out.merge(net_m, on="standard_name", how="left")
    out = out.merge(kenpom_m, on="standard_name", how="left")
    out = out.merge(bpi_m, on="standard_name", how="left")
    out = out.merge(sos_m, on="standard_name", how="left")

    out = rank_avg_metric(out)

    out = out.rename(columns={"standard_name": "team"})
    out = out[["team", "record", "ap", "avg", "net", "kenpom", "bpi", "sos", "avg_value"]].copy()
    out["updated_at_utc"] = now

    os.makedirs("docs", exist_ok=True)
    out.to_csv("docs/rankings.csv", index=False)

    meta = pd.DataFrame([{"updated_at_utc": now, "games_file": games_path or ""}])
    meta.to_csv("docs/last_updated.csv", index=False)

    print(f"Wrote {len(out)} rows -> docs/rankings.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
