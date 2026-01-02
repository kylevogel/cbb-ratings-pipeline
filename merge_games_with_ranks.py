from __future__ import annotations

from pathlib import Path
import pandas as pd
import re


def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _load_alias_lookup(root: Path) -> dict[str, str]:
    for p in [root / "team_alias.csv", root / "data_raw" / "team_alias.csv"]:
        if p.exists():
            df = pd.read_csv(p, dtype=str).fillna("")
            if "standard_name" not in df.columns:
                return {}
            lookup: dict[str, str] = {}

            def add(a: str, canon: str) -> None:
                k = _key(a)
                if k and k not in lookup:
                    lookup[k] = canon

            cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
            for _, r in df.iterrows():
                canon = str(r.get("standard_name", "")).strip()
                if not canon:
                    continue
                for c in cols:
                    v = str(r.get(c, "")).strip()
                    if v:
                        add(v, canon)
            return lookup
    return {}


def _canon(name: str, lookup: dict[str, str]) -> str:
    raw = str(name).strip()
    if not raw:
        return ""
    return lookup.get(_key(raw), raw)


def _pick_games_file(data_raw: Path) -> Path:
    candidates = [
        data_raw / "games_2024_clean_no_ids.csv",
        data_raw / "games_2024_clean.csv",
        data_raw / "games_2024.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise RuntimeError("Could not find games file in data_raw/.")


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if "snapshot_date" not in df.columns or df.empty:
        return df
    d = df.copy()
    d["snapshot_date"] = d["snapshot_date"].astype(str)
    latest = d["snapshot_date"].max()
    return d[d["snapshot_date"] == latest].copy()


def _to_int(x):
    if x is None:
        return pd.NA
    s = str(x).strip()
    if not s or s.lower() in {"nr", "na", "none", "team"}:
        return pd.NA
    try:
        return int(float(s))
    except Exception:
        return pd.NA


def _load_rank(path: Path, prefer: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["Team", "Rank"])

    df = pd.read_csv(path, dtype=str).fillna("")
    df = _latest_snapshot(df)

    cols = {c.lower().strip(): c for c in df.columns}

    team_col = cols.get("team")
    if team_col is None:
        team_col = df.columns[0]

    rank_col = None
    for p in prefer:
        p2 = p.lower().strip()
        if p2 in cols:
            rank_col = cols[p2]
            break

    if rank_col is None:
        for c in df.columns:
            cl = str(c).lower()
            if c != team_col and cl != "snapshot_date" and "rank" in cl:
                rank_col = c
                break

    if rank_col is None:
        for c in df.columns:
            if c != team_col and str(c).lower().strip() != "snapshot_date":
                rank_col = c
                break

    if rank_col is None:
        raise RuntimeError(f"{path.name} missing rank column. Found {df.columns.tolist()}")

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    out["Rank"] = out["Rank"].map(_to_int)
    out = out.dropna(subset=["Rank"]).copy()
    out["Rank"] = out["Rank"].astype(int)
    return out


def main() -> None:
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_processed = root / "data_processed"
    data_processed.mkdir(parents=True, exist_ok=True)

    lookup = _load_alias_lookup(root)

    games_path = _pick_games_file(data_raw)
    games = pd.read_csv(games_path)

    if "Team" not in games.columns or "Opponent" not in games.columns:
        raise RuntimeError(f"{games_path.name} must contain Team and Opponent columns.")

    net = _load_rank(data_raw / "NET_Rank.csv", ["net_rank", "net", "net rank", "rank"])
    kp = _load_rank(data_raw / "KenPom_Rank.csv", ["kenpom_rank", "kenpom", "kenpom rank", "rank"])
    bpi = _load_rank(data_raw / "BPI_Rank.csv", ["bpi_rank", "bpi", "bpi rank", "rank"])

    for df in (net, kp, bpi):
        df["TeamCanon"] = df["Team"].astype(str).map(lambda x: _canon(x, lookup))
        df["TeamKey"] = df["TeamCanon"].map(_key)

    net_m = net[["TeamKey", "Rank"]].rename(columns={"Rank": "NET"})
    kp_m = kp[["TeamKey", "Rank"]].rename(columns={"Rank": "KenPom"})
    bpi_m = bpi[["TeamKey", "Rank"]].rename(columns={"Rank": "BPI"})

    games["TeamCanon"] = games["Team"].astype(str).map(lambda x: _canon(x, lookup))
    games["OpponentCanon"] = games["Opponent"].astype(str).map(lambda x: _canon(x, lookup))
    games["TeamKey"] = games["TeamCanon"].map(_key)
    games["OpponentKey"] = games["OpponentCanon"].map(_key)

    out = games.merge(net_m, on="TeamKey", how="left")
    out = out.merge(kp_m, on="TeamKey", how="left")
    out = out.merge(bpi_m, on="TeamKey", how="left")

    out = out.rename(columns={"NET": "Team_NET", "KenPom": "Team_KenPom", "BPI": "Team_BPI"})

    opp_net = net_m.rename(columns={"TeamKey": "OpponentKey", "NET": "Opponent_NET"})
    opp_kp = kp_m.rename(columns={"TeamKey": "OpponentKey", "KenPom": "Opponent_KenPom"})
    opp_bpi = bpi_m.rename(columns={"TeamKey": "OpponentKey", "BPI": "Opponent_BPI"})

    out = out.merge(opp_net, on="OpponentKey", how="left")
    out = out.merge(opp_kp, on="OpponentKey", how="left")
    out = out.merge(opp_bpi, on="OpponentKey", how="left")

    out = out.drop(columns=["TeamCanon", "OpponentCanon", "TeamKey", "OpponentKey"], errors="ignore")

    out_path = data_processed / "games_with_ranks.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote: {out_path} ({len(out)} rows)")


if __name__ == "__main__":
    main()
