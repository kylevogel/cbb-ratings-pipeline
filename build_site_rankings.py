from pathlib import Path
import json
import re
from datetime import datetime, timezone

import pandas as pd


def _now_et_string():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _norm(s):
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _team_key(team_name: str) -> str:
    """
    Canonical join key for merging between sources.
    Handles the biggest mismatch you’re seeing: "St." vs "State".
    Also handles common shorthand: UNC, UConn, etc.
    """
    t = _norm(team_name)

    # common shorthands
    t = re.sub(r"\buconn\b", "connecticut", t)
    t = re.sub(r"\bunc\b", "north carolina", t)

    # st -> state (very common across rankings vs ESPN)
    # convert standalone "st" token to "state"
    t = re.sub(r"\bst\b", "state", t)

    # clean spaces again
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _find_latest_csv(search_root: Path, name_keywords):
    if not search_root.exists():
        return None
    candidates = []
    for p in search_root.rglob("*.csv"):
        fn = p.name.lower()
        ok = True
        for kw in name_keywords:
            if kw not in fn:
                ok = False
                break
        if ok:
            candidates.append(p)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]


def _safe_read_csv(path: Path):
    # alias file can have stray commas -> use python engine + skip bad lines
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.read_csv(path, dtype=str, engine="python", on_bad_lines="skip")


def _read_alias_map(base_dir: Path):
    """
    Returns mapping from TeamKey -> PreferredDisplayName
    We use this only to choose the display name, not the join key.
    """
    explicit = base_dir / "team_alias.csv"
    alias_file = explicit if explicit.exists() else None

    if alias_file is None:
        data_raw = base_dir / "data_raw"
        if data_raw.exists():
            for p in data_raw.rglob("*.csv"):
                if p.name.lower() == "team_alias.csv":
                    alias_file = p
                    break

    if alias_file is None:
        return {}

    df = _safe_read_csv(alias_file)
    if df is None or df.empty:
        return {}

    df = df.fillna("")
    cols = {c.lower().strip(): c for c in df.columns}

    # prefer ESPN->Team format if present
    if "espn" in cols and ("team" in cols or "canonical" in cols or "school" in cols):
        espn_col = cols["espn"]
        team_col = cols.get("team", cols.get("canonical", cols.get("school")))
        m = {}
        for _, r in df[[espn_col, team_col]].iterrows():
            a = str(r[espn_col]).strip()
            c = str(r[team_col]).strip()
            if a and c:
                m[_team_key(a)] = c
                m[_team_key(c)] = c
        return m

    # fallback: first col = alias, second col = canonical
    if df.shape[1] >= 2:
        c0, c1 = df.columns[0], df.columns[1]
        m = {}
        for _, r in df[[c0, c1]].iterrows():
            a = str(r[c0]).strip()
            c = str(r[c1]).strip()
            if a and c:
                m[_team_key(a)] = c
                m[_team_key(c)] = c
        return m

    return {}


def _coerce_rank_series(s):
    if s is None:
        return None
    s2 = s.copy()
    s2 = s2.replace({"NR": pd.NA, "": pd.NA, "None": pd.NA})
    s2 = pd.to_numeric(s2, errors="coerce")
    return s2


def _load_rank_file(path: Path, team_col_guess=("team", "school", "name"), rank_col_guess=("rank", "ranking", "rk")):
    df = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in df.columns}

    team_col = None
    for g in team_col_guess:
        if g in cols:
            team_col = cols[g]
            break
    if team_col is None:
        team_col = df.columns[0]

    rank_col = None
    for g in rank_col_guess:
        if g in cols:
            rank_col = cols[g]
            break
    if rank_col is None:
        for c in df.columns:
            lc = c.lower()
            if "rank" in lc or lc == "rk":
                rank_col = c
                break
    if rank_col is None:
        rank_col = df.columns[1] if df.shape[1] > 1 else df.columns[0]

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    out["TeamKey"] = out["Team"].map(_team_key)
    out["Rank"] = _coerce_rank_series(out["Rank"])
    return out


def _load_records(base_dir: Path):
    data_raw = base_dir / "data_raw"
    games_file = _find_latest_csv(data_raw, ["games"])
    if games_file is None:
        return {}

    g = pd.read_csv(games_file)
    colmap = {c.lower().strip(): c for c in g.columns}

    team_col = colmap.get("team")
    if team_col is None:
        return {}

    win_col = None
    for k in ("win?", "win", "result", "w/l", "wl"):
        if k in colmap:
            win_col = colmap[k]
            break

    ts_col = None
    os_col = None
    for k in ("team_score", "teamscore", "score", "pf", "points_for"):
        if k in colmap:
            ts_col = colmap[k]
            break
    for k in ("opponent_score", "opponentscore", "pa", "points_against"):
        if k in colmap:
            os_col = colmap[k]
            break

    tmp = g.copy()
    tmp["TeamRaw"] = tmp[team_col].astype(str)
    tmp["TeamKey"] = tmp["TeamRaw"].map(_team_key)

    if ts_col is not None and os_col is not None:
        ts = pd.to_numeric(tmp[ts_col], errors="coerce")
        os = pd.to_numeric(tmp[os_col], errors="coerce")
        valid = ts.notna() & os.notna()
        tmp = tmp.loc[valid].copy()
        tmp["_win"] = (pd.to_numeric(tmp[ts_col], errors="coerce") > pd.to_numeric(tmp[os_col], errors="coerce"))
    elif win_col is not None:
        w = tmp[win_col].astype(str).str.upper().str.strip()
        win_mask = w.isin(["W", "WIN", "TRUE", "1", "YES", "Y"])
        lose_mask = w.isin(["L", "LOSS", "FALSE", "0", "NO", "N"])
        tmp = tmp.loc[win_mask | lose_mask].copy()
        tmp["_win"] = win_mask.loc[tmp.index].astype(bool)
    else:
        return {}

    grp = tmp.groupby("TeamKey")["_win"].agg(["sum", "count"]).reset_index()
    grp["wins"] = grp["sum"].astype(int)
    grp["losses"] = (grp["count"] - grp["sum"]).astype(int)

    return {r["TeamKey"]: f'{int(r["wins"])}-{int(r["losses"])}' for _, r in grp.iterrows()}


def main():
    base_dir = Path(__file__).resolve().parent
    data_raw = base_dir / "data_raw"

    alias_display = _read_alias_map(base_dir)
    records = _load_records(base_dir)

    net_path = _find_latest_csv(data_raw, ["net"])
    kp_path = _find_latest_csv(data_raw, ["kenpom"])
    bpi_path = _find_latest_csv(data_raw, ["bpi"])
    ap_path = _find_latest_csv(data_raw, ["ap"])

    if net_path is None:
        raise FileNotFoundError("Could not find a NET rankings CSV in data_raw (filename must include 'net').")

    net = _load_rank_file(net_path)
    kp = _load_rank_file(kp_path) if kp_path else pd.DataFrame(columns=["Team", "Rank", "TeamKey"])
    bpi = _load_rank_file(bpi_path) if bpi_path else pd.DataFrame(columns=["Team", "Rank", "TeamKey"])
    ap = _load_rank_file(ap_path) if ap_path else pd.DataFrame(columns=["Team", "Rank", "TeamKey"])

    # build master list of keys
    all_keys = set(net["TeamKey"]).union(kp["TeamKey"]).union(bpi["TeamKey"]).union(ap["TeamKey"]).union(records.keys())

    out = pd.DataFrame({"TeamKey": sorted(all_keys)})

    # choose display name priority: alias map > NET > KenPom > BPI > AP > key
    net_name = net.groupby("TeamKey")["Team"].first()
    kp_name = kp.groupby("TeamKey")["Team"].first()
    bpi_name = bpi.groupby("TeamKey")["Team"].first()
    ap_name = ap.groupby("TeamKey")["Team"].first()

    def display_for_key(k):
        if k in alias_display:
            return alias_display[k]
        if k in net_name:
            return net_name[k]
        if k in kp_name:
            return kp_name[k]
        if k in bpi_name:
            return bpi_name[k]
        if k in ap_name:
            return ap_name[k]
        return k

    out["Team"] = out["TeamKey"].map(display_for_key)

    # records
    out["Record"] = out["TeamKey"].map(lambda k: records.get(k, "0-0"))

    # merge rankings on TeamKey
    net2 = net[["TeamKey", "Rank"]].rename(columns={"Rank": "NET"})
    kp2 = kp[["TeamKey", "Rank"]].rename(columns={"Rank": "KenPom"})
    bpi2 = bpi[["TeamKey", "Rank"]].rename(columns={"Rank": "BPI"})
    ap2 = ap[["TeamKey", "Rank"]].rename(columns={"Rank": "AP"})

    out = out.merge(net2, on="TeamKey", how="left")
    out = out.merge(kp2, on="TeamKey", how="left")
    out = out.merge(bpi2, on="TeamKey", how="left")
    out = out.merge(ap2, on="TeamKey", how="left")

    def to_display(x):
        if pd.isna(x):
            return "NR"
        try:
            return int(x)
        except Exception:
            return "NR"

    for c in ["NET", "KenPom", "BPI", "AP"]:
        out[c] = out[c].map(to_display)

    def sort_key(v):
        if v == "NR":
            return 10**9
        return int(v)

    out = out.sort_values(by=["NET", "KenPom", "BPI"], key=lambda s: s.map(sort_key), ascending=True)
    out = out[["Team", "Record", "NET", "KenPom", "BPI", "AP"]].reset_index(drop=True)

    payload = {
        "updated": _now_et_string(),
        "rows": out.to_dict(orient="records"),
    }

    docs_data = base_dir / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    (docs_data / "rankings_current.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
