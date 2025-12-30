import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


MISSING_OPP_NET = 366


def _norm(s):
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _team_key(s):
    t = _norm(s)
    t = re.sub(r"\buconn\b", "connecticut", t)
    t = re.sub(r"\bunc\b", "north carolina", t)
    t = re.sub(r"\bst\b", "state", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_alias_map(path: Path):
    alias_to_canon = {}
    if not path.exists():
        return alias_to_canon
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            if not parts:
                continue
            canon = parts[0]
            for p in parts:
                alias_to_canon[_norm(p)] = canon
    return alias_to_canon


def canon_team(name, alias_map):
    s = str(name).strip()
    return alias_map.get(_norm(s), s)


def _safe_read_csv(p: Path):
    try:
        return pd.read_csv(p)
    except Exception:
        try:
            return pd.read_csv(p, engine="python", on_bad_lines="skip")
        except Exception:
            return None


def _latest_file_by_contains(folder: Path, contains_tokens):
    toks = [t.lower() for t in contains_tokens]
    best = None
    best_mtime = None
    for p in folder.glob("*.csv"):
        name = p.name.lower()
        if all(t in name for t in toks):
            m = p.stat().st_mtime
            if best is None or m > best_mtime:
                best = p
                best_mtime = m
    return best


def load_rank(path: Path, team_col: str, rank_col: str, out_col: str, alias_map: dict):
    if path is None or not path.exists():
        return pd.DataFrame(columns=["Team", "TeamKey", out_col])
    df = _safe_read_csv(path)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Team", "TeamKey", out_col])
    if team_col not in df.columns or rank_col not in df.columns:
        return pd.DataFrame(columns=["Team", "TeamKey", out_col])

    out = df[[team_col, rank_col]].copy()
    out["Team"] = out[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    out["TeamKey"] = out["Team"].map(_team_key)
    out[out_col] = pd.to_numeric(out[rank_col], errors="coerce")

    out = out.dropna(subset=[out_col])
    out = out.drop_duplicates(subset=["TeamKey"], keep="first")
    return out[["Team", "TeamKey", out_col]]


def to_int_or_blank(v):
    if pd.isna(v):
        return ""
    try:
        return int(v)
    except Exception:
        return ""


def ap_val(v):
    if pd.isna(v):
        return "NR"
    try:
        return int(v)
    except Exception:
        return "NR"


def _load_records_completed_only(data_raw: Path, alias_map: dict):
    # Prefer your canonical file name if it exists
    games_path = data_raw / "games_2024_clean_no_ids.csv"
    if not games_path.exists():
        games_path = _latest_file_by_contains(data_raw, ["games"])

    if games_path is None or not games_path.exists():
        return pd.DataFrame(columns=["TeamKey", "Record"])

    g = _safe_read_csv(games_path)
    if g is None or g.empty:
        return pd.DataFrame(columns=["TeamKey", "Record"])

    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    win_col = cols.get("win?")
    if team_col is None:
        return pd.DataFrame(columns=["TeamKey", "Record"])

    tmp = g.copy()
    tmp["Team"] = tmp[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["Team"].map(_team_key)

    # only count completed games (Win? present OR scores present)
    if win_col is not None:
        v = tmp[win_col].astype(str).str.strip().str.lower()
        is_win = v.isin({"w", "win", "true", "t", "1", "yes", "y"})
        is_loss = v.isin({"l", "loss", "false", "f", "0", "no", "n"})
    else:
        ts_col = cols.get("team_score")
        os_col = cols.get("opponent_score")
        if ts_col is None or os_col is None:
            return pd.DataFrame(columns=["TeamKey", "Record"])
        ts = pd.to_numeric(tmp[ts_col], errors="coerce")
        os_ = pd.to_numeric(tmp[os_col], errors="coerce")
        is_win = (ts > os_) & ts.notna() & os_.notna()
        is_loss = (ts < os_) & ts.notna() & os_.notna()

    rec = (
        tmp.assign(_W=is_win.astype(int), _L=is_loss.astype(int))
        .groupby("TeamKey", as_index=False)[["_W", "_L"]]
        .sum()
    )
    rec["Record"] = rec["_W"].astype(int).astype(str) + "-" + rec["_L"].astype(int).astype(str)
    return rec[["TeamKey", "Record"]]


def _compute_sos_1_to_365(data_raw: Path, alias_map: dict, net_df: pd.DataFrame):
    """
    SOS = average opponent NET over ALL games in the games file (past + future).
    Opponent missing NET -> 366.
    Rank NET teams 1..365 with no gaps (lowest avg opp NET = rank 1).
    """
    net_base = net_df[["Team", "TeamKey", "NET"]].copy()
    net_base["NET"] = pd.to_numeric(net_base["NET"], errors="coerce")
    net_base = net_base.dropna(subset=["NET"])
    net_base = net_base[(net_base["NET"] >= 1) & (net_base["NET"] <= 365)]
    net_base = net_base.drop_duplicates(subset=["TeamKey"], keep="first")

    net_map = dict(zip(net_base["TeamKey"], net_base["NET"].astype(int)))

    games_path = data_raw / "games_2024_clean_no_ids.csv"
    if not games_path.exists():
        games_path = _latest_file_by_contains(data_raw, ["games"])

    if games_path is None or not games_path.exists():
        net_base["OppAvg"] = float(MISSING_OPP_NET)
        net_base = net_base.sort_values(["OppAvg", "NET", "Team"], ascending=[True, True, True]).reset_index(drop=True)
        net_base["SOS"] = range(1, len(net_base) + 1)
        return net_base[["TeamKey", "SOS"]]

    g = _safe_read_csv(games_path)
    if g is None or g.empty:
        net_base["OppAvg"] = float(MISSING_OPP_NET)
        net_base = net_base.sort_values(["OppAvg", "NET", "Team"], ascending=[True, True, True]).reset_index(drop=True)
        net_base["SOS"] = range(1, len(net_base) + 1)
        return net_base[["TeamKey", "SOS"]]

    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    opp_col = cols.get("opponent")
    if team_col is None or opp_col is None:
        net_base["OppAvg"] = float(MISSING_OPP_NET)
        net_base = net_base.sort_values(["OppAvg", "NET", "Team"], ascending=[True, True, True]).reset_index(drop=True)
        net_base["SOS"] = range(1, len(net_base) + 1)
        return net_base[["TeamKey", "SOS"]]

    tmp = g[[team_col, opp_col]].copy()
    tmp["TeamCanon"] = tmp[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    tmp["OppCanon"] = tmp[opp_col].astype(str).map(lambda x: canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)
    tmp["OppKey"] = tmp["OppCanon"].map(_team_key)

    # only compute SOS for NET universe teams
    net_keys = set(net_base["TeamKey"])
    tmp = tmp[tmp["TeamKey"].isin(net_keys)].copy()

    tmp["OppNET"] = tmp["OppKey"].map(net_map)
    tmp["OppNET"] = tmp["OppNET"].fillna(MISSING_OPP_NET).astype(int)

    opp_avg = tmp.groupby("TeamKey", as_index=False)["OppNET"].mean().rename(columns={"OppNET": "OppAvg"})
    net_base = net_base.merge(opp_avg, on="TeamKey", how="left")
    net_base["OppAvg"] = net_base["OppAvg"].fillna(float(MISSING_OPP_NET))

    net_base = net_base.sort_values(["OppAvg", "NET", "Team"], ascending=[True, True, True]).reset_index(drop=True)
    net_base["SOS"] = range(1, len(net_base) + 1)

    return net_base[["TeamKey", "SOS"]]


def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_path = data_raw / "team_alias.csv"
    if not alias_path.exists():
        alias_path = root / "team_alias.csv"
    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}

    net_path = data_raw / "NET_Rank.csv"
    kp_path = data_raw / "KenPom_Rank.csv"
    bpi_path = data_raw / "BPI_Rank.csv"
    ap_path = data_raw / "AP_Rank.csv"

    net = load_rank(net_path, "Team", "NET_Rank", "NET", alias_map)
    kp = load_rank(kp_path, "Team", "KenPom_Rank", "KenPom", alias_map)
    bpi = load_rank(bpi_path, "Team", "BPI_Rank", "BPI", alias_map)
    ap = load_rank(ap_path, "Team", "AP_Rank", "AP", alias_map)

    net["NET"] = pd.to_numeric(net["NET"], errors="coerce")
    net = net.dropna(subset=["NET"])
    net = net[(net["NET"] >= 1) & (net["NET"] <= 365)]
    net = net.drop_duplicates(subset=["TeamKey"], keep="first")

    rec = _load_records_completed_only(data_raw, alias_map)
    sos = _compute_sos_1_to_365(data_raw, alias_map, net)

    df = net[["Team", "TeamKey", "NET"]].copy()
    df = df.merge(rec, on="TeamKey", how="left")
    df = df.merge(sos, on="TeamKey", how="left")
    df = df.merge(kp[["TeamKey", "KenPom"]], on="TeamKey", how="left")
    df = df.merge(bpi[["TeamKey", "BPI"]], on="TeamKey", how="left")
    df = df.merge(ap[["TeamKey", "AP"]], on="TeamKey", how="left")

    df = df.sort_values("NET", na_position="last")

    df["Record"] = df["Record"].fillna("0-0")
    df["NET"] = df["NET"].apply(to_int_or_blank)
    df["SOS"] = df["SOS"].apply(to_int_or_blank)
    df["KenPom"] = df["KenPom"].apply(to_int_or_blank)
    df["BPI"] = df["BPI"].apply(to_int_or_blank)
    df["AP"] = df["AP"].apply(ap_val)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    payload = {
        "updated": ts,
        "last_updated": ts,
        "rows": df[["Team", "Record", "SOS", "NET", "KenPom", "BPI", "AP"]].to_dict(orient="records"),
    }

    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print("Wrote docs/data/rankings_current.json")


if __name__ == "__main__":
    main()
