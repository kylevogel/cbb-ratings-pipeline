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


def _read_alias_map(base_dir: Path):
    data_raw = base_dir / "data_raw"
    alias_file = None
    for p in list(data_raw.rglob("*.csv")) + list(base_dir.rglob("*.csv")):
        if "alias" in p.name.lower() and "team" in p.name.lower():
            alias_file = p
            break
    if alias_file is None:
        for p in list(data_raw.rglob("*.csv")) + list(base_dir.rglob("*.csv")):
            if "alias" in p.name.lower():
                alias_file = p
                break
    if alias_file is None:
        return {}

    df = pd.read_csv(alias_file)
    cols = [c.lower().strip() for c in df.columns]

    def pick(*options):
        for o in options:
            if o in cols:
                return df.columns[cols.index(o)]
        return None

    alias_col = pick("alias", "aliases", "alt", "alternate", "name", "team_alias")
    canon_col = pick("canonical", "canon", "team", "standard", "school", "official")

    m = {}
    if alias_col and canon_col:
        for _, r in df[[alias_col, canon_col]].dropna().iterrows():
            a = _norm(r[alias_col])
            c = str(r[canon_col]).strip()
            if a:
                m[a] = c
        return m

    if df.shape[1] >= 2:
        c0, c1 = df.columns[0], df.columns[1]
        for _, r in df[[c0, c1]].dropna().iterrows():
            a = _norm(r[c0])
            c = str(r[c1]).strip()
            if a:
                m[a] = c
        return m

    return {}


def _apply_alias(team, alias_map):
    nt = _norm(team)
    return alias_map.get(nt, str(team).strip())


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
            if "rank" in lc or lc in ("rk",):
                rank_col = c
                break
    if rank_col is None:
        rank_col = df.columns[1] if df.shape[1] > 1 else df.columns[0]

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    return out


def _load_records(base_dir: Path, alias_map):
    data_raw = base_dir / "data_raw"
    games_file = _find_latest_csv(data_raw, ["games"])
    if games_file is None:
        return {}

    g = pd.read_csv(games_file)

    colmap = {c.lower().strip(): c for c in g.columns}
    team_col = colmap.get("team", None)
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

    if team_col is None:
        return {}

    tmp = g.copy()
    tmp["Team"] = tmp[team_col].astype(str)

    if ts_col is not None and os_col is not None:
        ts = pd.to_numeric(tmp[ts_col], errors="coerce")
        os = pd.to_numeric(tmp[os_col], errors="coerce")
        valid = ts.notna() & os.notna()
        tmp = tmp.loc[valid].copy()
        tmp["_win"] = (pd.to_numeric(tmp[ts_col], errors="coerce") > pd.to_numeric(tmp[os_col], errors="coerce"))
    elif win_col is not None:
        w = tmp[win_col].astype(str).str.upper().str.strip()
        tmp["_win"] = w.isin(["W", "WIN", "TRUE", "1", "YES"])
        lose_mask = w.isin(["L", "LOSS", "FALSE", "0", "NO"])
        tmp = tmp.loc[tmp["_win"] | lose_mask].copy()
        tmp["_win"] = tmp["_win"].astype(bool)
    else:
        return {}

    tmp["TeamCanon"] = tmp["Team"].map(lambda x: _apply_alias(x, alias_map))
    grp = tmp.groupby("TeamCanon")["_win"].agg(["sum", "count"]).reset_index()
    grp["wins"] = grp["sum"].astype(int)
    grp["losses"] = (grp["count"] - grp["sum"]).astype(int)
    rec = {r["TeamCanon"]: f'{int(r["wins"])}-{int(r["losses"])}' for _, r in grp.iterrows()}
    return rec


def main():
    base_dir = Path(__file__).resolve().parent
    data_raw = base_dir / "data_raw"

    alias_map = _read_alias_map(base_dir)
    records = _load_records(base_dir, alias_map)

    net_path = _find_latest_csv(data_raw, ["net"])
    kp_path = _find_latest_csv(data_raw, ["kenpom"])
    bpi_path = _find_latest_csv(data_raw, ["bpi"])
    ap_path = _find_latest_csv(data_raw, ["ap"])

    if net_path is None:
        raise FileNotFoundError("Could not find a NET rankings CSV in data_raw (filename must include 'net').")

    net = _load_rank_file(net_path)
    kp = _load_rank_file(kp_path) if kp_path else pd.DataFrame(columns=["Team", "Rank"])
    bpi = _load_rank_file(bpi_path) if bpi_path else pd.DataFrame(columns=["Team", "Rank"])
    ap = _load_rank_file(ap_path) if ap_path else pd.DataFrame(columns=["Team", "Rank"])

    for df in (net, kp, bpi, ap):
        df["Team"] = df["Team"].map(lambda x: _apply_alias(x, alias_map))
        df["Rank"] = _coerce_rank_series(df["Rank"])

    out = pd.DataFrame({"Team": sorted(set(net["Team"]).union(kp["Team"]).union(bpi["Team"]).union(ap["Team"]).union(records.keys()))})
    out["Record"] = out["Team"].map(lambda t: records.get(t, "0-0"))

    out = out.merge(net.rename(columns={"Rank": "NET"}), on="Team", how="left")
    out = out.merge(kp.rename(columns={"Rank": "KenPom"}), on="Team", how="left")
    out = out.merge(bpi.rename(columns={"Rank": "BPI"}), on="Team", how="left")
    out = out.merge(ap.rename(columns={"Rank": "AP"}), on="Team", how="left")

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

    payload = {
        "updated": _now_et_string(),
        "rows": out[["Team", "Record", "NET", "KenPom", "BPI", "AP"]].to_dict(orient="records"),
    }

    docs_data = base_dir / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    (docs_data / "rankings_current.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
