from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _load_alias_df(root: Path) -> pd.DataFrame | None:
    for p in [root / "team_alias.csv", root / "data_raw" / "team_alias.csv"]:
        if p.exists():
            return pd.read_csv(p, dtype=str).fillna("")
    return None


def _build_alias_lookup(alias_df: pd.DataFrame | None) -> dict[str, str]:
    if alias_df is None:
        return {}
    cols = set(alias_df.columns)
    if not {"standard_name"}.issubset(cols):
        return {}

    lookup: dict[str, str] = {}

    def add(a: str, standard: str) -> None:
        k = _norm(a)
        if k and k not in lookup:
            lookup[k] = standard

    name_cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
    for _, r in alias_df.iterrows():
        standard = str(r.get("standard_name", "")).strip()
        if not standard:
            continue
        for c in name_cols:
            v = str(r.get(c, "")).strip()
            if v:
                add(v, standard)
    return lookup


def _canon(name: str, lookup: dict[str, str]) -> str:
    raw = str(name).strip()
    if not raw:
        return ""
    return lookup.get(_norm(raw), raw)


def _pick_games_file(data_raw: Path) -> Path:
    candidates = [
        data_raw / "games_2024_clean_no_ids.csv",
        data_raw / "games_2024_clean.csv",
        data_raw / "games_2024.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise RuntimeError("Could not find a games file in data_raw/.")


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "snapshot_date" not in df.columns:
        return df
    d = df.copy()
    d["snapshot_date"] = d["snapshot_date"].astype(str)
    latest = d["snapshot_date"].max()
    return d[d["snapshot_date"] == latest].copy()


def _to_int_series(s: pd.Series) -> pd.Series:
    def f(x):
        if x is None:
            return pd.NA
        t = str(x).strip()
        if not t:
            return pd.NA
        if t.lower() in {"bpi", "kenpom", "net", "rank", "team"}:
            return pd.NA
        try:
            return int(float(t))
        except Exception:
            return pd.NA

    return s.apply(f)


def _load_rank_file(path: Path, preferred_cols: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["TeamCanon", "Key", "Rank"])

    df = pd.read_csv(path, dtype=str).fillna("")
    df = _latest_snapshot(df)

    cols_lower = {c.lower().strip(): c for c in df.columns}
    team_col = cols_lower.get("team", None)
    if team_col is None:
        raise RuntimeError(f"{path.name} missing Team column. Found {df.columns.tolist()}")

    rank_col = None
    for c in preferred_cols:
        if c.lower().strip() in cols_lower:
            rank_col = cols_lower[c.lower().strip()]
            break

    if rank_col is None:
        non_team = [c for c in df.columns if c != team_col and c.lower().strip() != "snapshot_date"]
        if len(non_team) == 1:
            rank_col = non_team[0]
        elif len(non_team) >= 2:
            rank_col = non_team[-1]
        else:
            raise RuntimeError(f"{path.name} missing rank column. Found {df.columns.tolist()}")

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    out["Rank"] = _to_int_series(out["Rank"])
    out = out.dropna(subset=["Rank"]).copy()
    out["Rank"] = out["Rank"].astype(int)
    return out


def main() -> None:
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_processed = root / "data_processed"
    data_processed.mkdir(parents=True, exist_ok=True)

    alias_df = _load_alias_df(root)
    lookup = _build_alias_lookup(alias_df)

    games_path = _pick_games_file(data_raw)
    games = pd.read_csv(games_path)

    if "Team" not in games.columns or "Opponent" not in games.columns:
        raise RuntimeError(f"{games_path.name} must contain Team and Opponent columns.")

    net = _load_rank_file(data_raw / "NET_Rank.csv", ["NET_Rank", "NET", "Rank"])
    kp = _load_rank_file(data_raw / "KenPom_Rank.csv", ["KenPom_Rank", "KenPom", "Rank"])
    bpi = _load_rank_file(data_raw / "BPI_Rank.csv", ["BPI_Rank", "BPI", "Rank"])

    for df in (net, kp, bpi):
        df["TeamCanon"] = df["Team"].astype(str).map(lambda x: _canon(x, lookup))
        df["Key"] = df["TeamCanon"].map(_norm)

    net_map = net[["Key", "Rank"]].rename(columns={"Rank": "NET"})
    kp_map = kp[["Key", "Rank"]].rename(columns={"Rank": "KenPom"})
    bpi_map = bpi[["Key", "Rank"]].rename(columns={"Rank": "BPI"})

    games["TeamCanon"] = games["Team"].astype(str).map(lambda x: _canon(x, lookup))
    games["OpponentCanon"] = games["Opponent"].astype(str).map(lambda x: _canon(x, lookup))
    games["TeamKey"] = games["TeamCanon"].map(_norm)
    games["OpponentKey"] = games["OpponentCanon"].map(_norm)

    out = games.merge(net_map, left_on="TeamKey", right_on="Key", how="left").drop(columns=["Key"])
    out = out.merge(kp_map, left_on="TeamKey", right_on="Key", how="left").drop(columns=["Key"])
    out = out.merge(bpi_map, left_on="TeamKey", right_on="Key", how="left").drop(columns=["Key"])

    out = out.rename(columns={"NET": "Team_NET", "KenPom": "Team_KenPom", "BPI": "Team_BPI"})

    opp_net = net_map.rename(columns={"NET": "Opponent_NET"})
    opp_kp = kp_map.rename(columns={"KenPom": "Opponent_KenPom"})
    opp_bpi = bpi_map.rename(columns={"BPI": "Opponent_BPI"})

    out = out.merge(opp_net, left_on="OpponentKey", right_on="Key", how="left").drop(columns=["Key"])
    out = out.merge(opp_kp, left_on="OpponentKey", right_on="Key", how="left").drop(columns=["Key"])
    out = out.merge(opp_bpi, left_on="OpponentKey", right_on="Key", how="left").drop(columns=["Key"])

    out = out.drop(columns=["TeamCanon", "OpponentCanon", "TeamKey", "OpponentKey"], errors="ignore")

    out_path = data_processed / "games_with_ranks.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(out)} rows")


if __name__ == "__main__":
    main()
