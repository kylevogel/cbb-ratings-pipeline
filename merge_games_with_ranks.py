from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import re


def _team_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _fuzzy_key(s: str) -> str:
    t = str(s).strip().lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _load_alias_map(root: Path) -> Dict[str, str]:
    for p in [root / "team_alias.csv", root / "data_raw" / "team_alias.csv"]:
        if p.exists():
            df = pd.read_csv(p, dtype=str).fillna("")
            if set(["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]).issubset(set(df.columns)):
                m: Dict[str, str] = {}
                for _, row in df.iterrows():
                    std = str(row.get("standard_name", "")).strip()
                    if not std:
                        continue
                    m[std.lower()] = std
                    for col in ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]:
                        v = str(row.get(col, "")).strip()
                        if v:
                            m[v.lower()] = std
                return m
            m: Dict[str, str] = {}
            if len(df.columns) >= 2:
                a_col = df.columns[0]
                c_col = df.columns[1]
                for a, c in zip(df[a_col].astype(str), df[c_col].astype(str)):
                    a2 = a.strip().lower()
                    c2 = c.strip()
                    if a2 and c2 and a2 not in m:
                        m[a2] = c2
            return m
    return {}


def _canonize_team(name: str, alias_map: Dict[str, str], fuzzy_to_canon: Optional[Dict[str, str]]) -> str:
    raw = str(name).strip() if name is not None else ""
    if not raw:
        return ""
    low = raw.lower()
    if low in alias_map:
        return alias_map[low]
    if fuzzy_to_canon is not None:
        fk = _fuzzy_key(raw)
        if fk in fuzzy_to_canon:
            return fuzzy_to_canon[fk]
    return raw


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "snapshot_date" not in df.columns:
        return df
    d = df.copy()
    d["snapshot_date"] = d["snapshot_date"].astype(str)
    latest = d["snapshot_date"].max()
    return d[d["snapshot_date"] == latest].copy()


def _to_int_or_blank(x):
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    try:
        return int(float(s))
    except Exception:
        return ""


def _load_rank_csv(path: Path, rank_candidates: List[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["Team", "Rank"])
    df = pd.read_csv(path)
    df = _latest_snapshot(df)
    cols = {c.lower().strip(): c for c in df.columns}
    team_col = cols.get("team", df.columns[0])
    rank_col = None
    for k in rank_candidates:
        if k in cols:
            rank_col = cols[k]
            break
    if rank_col is None:
        rank_col = df.columns[1] if len(df.columns) >= 2 else df.columns[0]
    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    out["Rank"] = out["Rank"].apply(_to_int_or_blank)
    out = out[out["Rank"] != ""].copy()
    out["Rank"] = out["Rank"].astype(int)
    return out


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


def main() -> None:
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_processed = root / "data_processed"
    data_processed.mkdir(parents=True, exist_ok=True)

    alias_map = _load_alias_map(root)

    net = _load_rank_csv(data_raw / "NET_Rank.csv", ["net_rank", "net"]).rename(columns={"Rank": "NET"})
    kp = _load_rank_csv(data_raw / "KenPom_Rank.csv", ["kenpom_rank", "kenpom"]).rename(columns={"Rank": "KenPom"})
    bpi = _load_rank_csv(data_raw / "BPI_Rank.csv", ["bpi_rank", "bpi"]).rename(columns={"Rank": "BPI"})

    net["TeamCanon"] = net["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, None))
    net["TeamKey"] = net["TeamCanon"].map(_team_key)

    net_fuzzy = {_fuzzy_key(t): t for t in net["TeamCanon"].astype(str).tolist() if str(t).strip()}

    def prep_rank(df: pd.DataFrame, col: str) -> pd.DataFrame:
        d = df.copy()
        if d.empty:
            return pd.DataFrame(columns=["TeamCanon", "TeamKey", col])
        d["TeamCanon"] = d["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, net_fuzzy))
        d["TeamKey"] = d["TeamCanon"].map(_team_key)
        return d[["TeamCanon", "TeamKey", col]].copy()

    kp_df = prep_rank(kp, "KenPom")
    bpi_df = prep_rank(bpi, "BPI")

    games_path = _pick_games_file(data_raw)
    games = pd.read_csv(games_path)

    if "Team" not in games.columns or "Opponent" not in games.columns:
        raise RuntimeError(f"{games_path.name} must contain Team and Opponent columns. Found: {games.columns.tolist()}")

    games["TeamCanon"] = games["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, net_fuzzy))
    games["OpponentCanon"] = games["Opponent"].astype(str).map(lambda x: _canonize_team(x, alias_map, net_fuzzy))
    games["TeamKey"] = games["TeamCanon"].map(_team_key)
    games["OpponentKey"] = games["OpponentCanon"].map(_team_key)

    base = games.merge(net[["TeamKey", "NET"]], on="TeamKey", how="left")
    base = base.merge(kp_df[["TeamKey", "KenPom"]], on="TeamKey", how="left")
    base = base.merge(bpi_df[["TeamKey", "BPI"]], on="TeamKey", how="left")

    base = base.rename(columns={"NET": "Team_NET", "KenPom": "Team_KenPom", "BPI": "Team_BPI"})

    opp_net = net.rename(columns={"TeamKey": "OpponentKey", "NET": "Opponent_NET"})[["OpponentKey", "Opponent_NET"]]
    opp_kp = kp_df.rename(columns={"TeamKey": "OpponentKey", "KenPom": "Opponent_KenPom"})[
        ["OpponentKey", "Opponent_KenPom"]
    ]
    opp_bpi = bpi_df.rename(columns={"TeamKey": "OpponentKey", "BPI": "Opponent_BPI"})[["OpponentKey", "Opponent_BPI"]]

    out = base.merge(opp_net, on="OpponentKey", how="left")
    out = out.merge(opp_kp, on="OpponentKey", how="left")
    out = out.merge(opp_bpi, on="OpponentKey", how="left")

    out = out.drop(columns=["TeamCanon", "OpponentCanon", "TeamKey", "OpponentKey"], errors="ignore")

    out_path = data_processed / "games_with_ranks.csv"
    out.to_csv(out_path, index=False)

    print(f"Wrote: {out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
