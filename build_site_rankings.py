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
        return pd.DataFrame(columns=["Team", out_col])
    df = _safe_read_csv(path)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Team", out_col])
    if team_col not in df.columns or rank_col not in df.columns:
        return pd.DataFrame(columns=["Team", out_col])
    out = df[[team_col, rank_col]].copy()
    out["Team"] = out[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    out[out_col] = pd.to_numeric(out[rank_col], errors="coerce")
    out = out.dropna(subset=[out_col]).drop_duplicates(subset=["Team"], keep="first")
    return out[["Team", out_col]]


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


def _load_records(data_raw: Path, alias_map: dict):
    games_path = _latest_file_by_contains(data_raw, ["games"])
    if games_path is None:
        return pd.DataFrame(columns=["Team", "Record"])

    g = _safe_read_csv(games_path)
    if g is None or g.empty:
        return pd.DataFrame(columns=["Team", "Record"])

    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    win_col = cols.get("win?")

    if team_col is None:
        return pd.DataFrame(columns=["Team", "Record"])

    tmp = g.copy()
    tmp["Team"] = tmp[team_col].astype(str).map(lambda x: canon_team(x, alias_map))

    if win_col is not None:
        v = tmp[win_col].astype(str).str.strip().str.lower()
        is_win = v.isin({"w", "win", "true", "t", "1", "yes", "y"})
        is_loss = v.isin({"l", "loss", "false", "f", "0", "no", "n"})
    else:
        ts_col = cols.get("team_score")
        os_col = cols.get("opponent_score")
        if ts_col is None or os_col is None:
            return pd.DataFrame(columns=["Team", "Record"])
        ts = pd.to_numeric(tmp[ts_col], errors="coerce")
        os = pd.to_numeric(tmp[os_col], errors="coerce")
        is_win = (ts > os) & ts.notna() & os.notna()
        is_loss = (ts < os) & ts.notna() & os.notna()

    rec = (
        tmp.assign(_W=is_win.astype(int), _L=is_loss.astype(int))
        .groupby("Team", as_index=False)[["_W", "_L"]]
        .sum()
    )
    rec["Record"] = rec["_W"].astype(int).astype(str) + "-" + rec["_L"].astype(int).astype(str)
    return rec[["Team", "Record"]]


def _compute_sos_rank(data_raw: Path, alias_map: dict, net_df: pd.DataFrame):
    games_path = _latest_file_by_contains(data_raw, ["games"])
    if games_path is None:
        return pd.DataFrame(columns=["Team", "SOS"])

    g = _safe_read_csv(games_path)
    if g is None or g.empty:
        return pd.DataFrame(columns=["Team", "SOS"])

    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    opp_col = cols.get("opponent")
    if team_col is None or opp_col is None:
        return pd.DataFrame(columns=["Team", "SOS"])

    net_map = {}
    for _, r in net_df.iterrows():
        t = str(r["Team"])
        rk = pd.to_numeric(r["NET"], errors="coerce")
        if pd.isna(rk):
            continue
        net_map[_team_key(t)] = int(rk)

    tmp = g[[team_col, opp_col]].copy()
    tmp["TeamCanon"] = tmp[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    tmp["OppCanon"] = tmp[opp_col].astype(str).map(lambda x: canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)
    tmp["OppKey"] = tmp["OppCanon"].map(_team_key)

    tmp["OppNET"] = tmp["OppKey"].map(net_map)
    tmp["OppNET"] = tmp["OppNET"].fillna(MISSING_OPP_NET).astype(int)

    avg = tmp.groupby("TeamKey", as_index=False)["OppNET"].mean()
    avg = avg.sort_values("OppNET", ascending=True)
    avg["SOS"] = avg["OppNET"].rank(method="min", ascending=True).astype(int)

    net_teamkey_to_display = {}
    for _, r in net_df.iterrows():
        t = str(r["Team"])
        net_teamkey_to_display[_team_key(t)] = t

    avg["Team"] = avg["TeamKey"].map(lambda k: net_teamkey_to_display.get(k, k))
    out = avg[["Team", "SOS"]].drop_duplicates(subset=["Team"], keep="first")
    return out


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
    net = net.drop_duplicates(subset=["Team"], keep="first")

    rec = _load_records(data_raw, alias_map)
    sos = _compute_sos_rank(data_raw, alias_map, net)

    df = net.merge(rec, on="Team", how="left")
    df = df.merge(sos, on="Team", how="left")
    df = df.merge(kp, on="Team", how="left").merge(bpi, on="Team", how="left").merge(ap, on="Team", how="left")
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


if __name__ == "__main__":
    main()
