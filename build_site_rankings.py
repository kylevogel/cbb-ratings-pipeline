from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
import re
import json
import pandas as pd


def _team_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _fuzzy_key(s: str) -> str:
    t = str(s).strip().lower()
    t = t.replace("&", " and ")
    t = t.replace(".", " ")
    t = t.replace("'", "")
    t = re.sub(r"^\s*st\s+", "saint ", t)
    t = re.sub(r"^\s*st\s*", "saint ", t)
    t = re.sub(r"\bmt\b", "mount", t)
    t = re.sub(r"\bft\b", "fort", t)
    t = re.sub(r"\bst\b", "state", t)
    t = re.sub(r"\s+", " ", t).strip()
    return re.sub(r"[^a-z0-9]+", "", t)


def _load_alias_map(root: Path) -> dict[str, str]:
    """Load team name mappings from team_alias.csv"""
    candidates = [
        root / "team_alias.csv",
        root / "data_raw" / "team_alias.csv",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return {}

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
    except Exception:
        return {}

    m: dict[str, str] = {}

    # Check for new format: espn_name, alternate_name, source
    if "espn_name" in df.columns:
        for _, row in df.iterrows():
            espn = str(row.get("espn_name", "")).strip()
            if not espn:
                continue
            
            # Map espn_name to itself (lowercase key -> proper case value)
            m[espn.lower()] = espn
            
            # Map alternate_name to espn_name
            alt = str(row.get("alternate_name", "")).strip()
            if alt:
                m[alt.lower()] = espn
        
        return m

    # Fallback: old format checks
    cols = {c.lower().strip(): c for c in df.columns}

    alias_col = None
    canon_col = None
    for k in ["alias", "from", "team", "name"]:
        if k in cols:
            alias_col = cols[k]
            break
    for k in ["canonical", "canon", "to", "standard"]:
        if k in cols:
            canon_col = cols[k]
            break

    if alias_col is not None and canon_col is not None:
        for a, c in zip(df[alias_col].astype(str), df[canon_col].astype(str)):
            a2 = a.strip().lower()
            c2 = c.strip()
            if a2 and c2:
                m[a2] = c2
                m[c2.strip().lower()] = c2
        return m

    if "standard_name" in cols:
        canon = cols["standard_name"]
        for _, row in df.iterrows():
            c2 = str(row.get(canon, "")).strip()
            if not c2:
                continue
            m[c2.lower()] = c2
            for col in df.columns:
                v = str(row.get(col, "")).strip()
                if v:
                    m[v.lower()] = c2
        return m

    if len(df.columns) >= 2:
        a_col = df.columns[0]
        c_col = df.columns[1]
        for a, c in zip(df[a_col].astype(str), df[c_col].astype(str)):
            a2 = a.strip().lower()
            c2 = c.strip()
            if a2 and c2:
                m[a2] = c2
                m[c2.strip().lower()] = c2
    return m


def _canonize_team(name: str, alias_map: dict[str, str], fuzzy_to_canon: dict[str, str] | None) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    k = raw.lower()
    if k in alias_map:
        return str(alias_map[k]).strip()
    fk = _fuzzy_key(raw)
    if fuzzy_to_canon is not None and fk in fuzzy_to_canon:
        return fuzzy_to_canon[fk]
    return raw


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    sd = cols.get("snapshot_date")
    if sd is None:
        return df
    try:
        m = df[sd].astype(str).max()
        return df[df[sd].astype(str) == m].copy()
    except Exception:
        return df


def _load_rank_csv(path: Path, wanted_rank_names: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _latest_snapshot(df)

    cols = {c.lower().strip(): c for c in df.columns}

    team_col = None
    for k in ["team", "school", "name"]:
        if k in cols:
            team_col = cols[k]
            break
    if team_col is None:
        team_col = df.columns[0]

    rank_col = None
    for w in wanted_rank_names:
        w2 = w.lower().strip()
        if w2 in cols:
            rank_col = cols[w2]
            break

    if rank_col is None:
        for c in df.columns:
            cl = str(c).lower()
            if "rank" in cl and "snapshot" not in cl:
                rank_col = c
                break

    if rank_col is None:
        for c in df.columns:
            if c != team_col and str(c).lower().strip() != "snapshot_date":
                rank_col = c
                break

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    return out


def _to_int_or_blank(x):
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    try:
        return int(float(x))
    except Exception:
        s = str(x).strip()
        if s == "" or s.lower() in {"nr", "na", "none"}:
            return ""
        try:
            return int(float(s))
        except Exception:
            return ""


def _ap_to_display(x):
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() in {"nr", "na", "none"}:
        return ""
    try:
        return int(float(s))
    except Exception:
        return ""


def _display_time_utc_minus5() -> str:
    dt = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    hour = dt.strftime("%I").lstrip("0") or "12"
    return f"{dt:%Y-%m-%d} {hour}:{dt:%M} {dt:%p} (UTC-5)"


def _load_record_from_games(path: Path, alias_map: dict[str, str], fuzzy_to_canon: dict[str, str] | None) -> pd.DataFrame:
    g = pd.read_csv(path)

    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    ts_col = cols.get("team_score")
    os_col = cols.get("opponent_score")

    if team_col is None or ts_col is None or os_col is None:
        return pd.DataFrame(columns=["TeamKey", "Record"])

    tmp = g[[team_col, ts_col, os_col]].copy()
    tmp.columns = ["Team", "Team_Score", "Opponent_Score"]
    tmp["Team_Score"] = pd.to_numeric(tmp["Team_Score"], errors="coerce")
    tmp["Opponent_Score"] = pd.to_numeric(tmp["Opponent_Score"], errors="coerce")
    tmp = tmp.dropna(subset=["Team_Score", "Opponent_Score"])

    tmp["TeamCanon"] = tmp["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, fuzzy_to_canon))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)

    tmp["W"] = (tmp["Team_Score"] > tmp["Opponent_Score"]).astype(int)
    tmp["L"] = (tmp["Team_Score"] < tmp["Opponent_Score"]).astype(int)

    rec = tmp.groupby("TeamKey", as_index=False)[["W", "L"]].sum()
    rec["Record"] = rec["W"].astype(int).astype(str) + "-" + rec["L"].astype(int).astype(str)
    return rec[["TeamKey", "Record"]]


def main() -> None:
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_map = _load_alias_map(root)
    print(f"Loaded {len(alias_map)} alias mappings")

    net_path = data_raw / "NET_Rank.csv"
    kp_path = data_raw / "KenPom_Rank.csv"
    bpi_path = data_raw / "BPI_Rank.csv"
    ap_path = data_raw / "AP_Rank.csv"
    sos_path = data_raw / "SOS_Rank.csv"
    games_path = data_raw / "games_2024_clean_no_ids.csv"

    if net_path.exists():
        net = pd.read_csv(net_path)
        net = _latest_snapshot(net)
        cols = {c.lower().strip(): c for c in net.columns}
        team_col = cols.get("team", net.columns[0])
        rank_col = cols.get("net_rank") or cols.get("net") or (net.columns[1] if len(net.columns) > 1 else net.columns[0])
        rec_col = cols.get("record")

        keep_cols = [team_col, rank_col] + ([rec_col] if rec_col is not None else [])
        net2 = net[keep_cols].copy()
        net2.columns = ["Team", "NET"] + (["Record"] if rec_col is not None else [])
    else:
        net2 = pd.DataFrame(columns=["Team", "NET", "Record"])

    net2["TeamCanon"] = net2["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, None))
    net2["TeamKey"] = net2["TeamCanon"].map(_team_key)
    net2["NET"] = net2["NET"].apply(_to_int_or_blank)
    if "Record" not in net2.columns:
        net2["Record"] = ""
    net2["Record"] = net2["Record"].fillna("")

    print(f"NET teams: {len(net2)}")

    net_fuzzy = {_fuzzy_key(t): t for t in net2["TeamCanon"].astype(str).tolist() if str(t).strip()}

    def prep(df: pd.DataFrame, col: str) -> pd.DataFrame:
        d = df.copy()
        if "Team" not in d.columns:
            return pd.DataFrame(columns=["TeamCanon", "TeamKey", col])
        d["TeamCanon"] = d["Team"].astype(str).map(lambda x: _canonize_team(x, alias_map, net_fuzzy))
        d["TeamKey"] = d["TeamCanon"].map(_team_key)
        if col not in d.columns:
            d[col] = ""
        return d[["TeamCanon", "TeamKey", col]].copy()

    kp = _load_rank_csv(kp_path, ["kenpom_rank", "kenpom"]) if kp_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    kp = kp.rename(columns={"Rank": "KenPom"})
    kp_df = prep(kp, "KenPom")
    print(f"KenPom teams: {len(kp_df)}")

    bpi = _load_rank_csv(bpi_path, ["bpi_rank", "bpi"]) if bpi_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    bpi = bpi.rename(columns={"Rank": "BPI"})
    bpi_df = prep(bpi, "BPI")
    print(f"BPI teams: {len(bpi_df)}")

    ap = _load_rank_csv(ap_path, ["ap_rank", "ap"]) if ap_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    ap = ap.rename(columns={"Rank": "AP"})
    ap_df = prep(ap, "AP")
    print(f"AP teams: {len(ap_df)}")

    sos = _load_rank_csv(sos_path, ["sos", "rank"]) if sos_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    sos = sos.rename(columns={"Rank": "SoS"})
    sos_df = prep(sos, "SoS")
    print(f"SoS teams: {len(sos_df)}")

    base = pd.concat(
        [
            net2[["TeamCanon", "TeamKey"]],
            kp_df[["TeamCanon", "TeamKey"]],
            bpi_df[["TeamCanon", "TeamKey"]],
            ap_df[["TeamCanon", "TeamKey"]],
            sos_df[["TeamCanon", "TeamKey"]],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["TeamKey"])

    df = base.copy()
    df = df.merge(net2[["TeamKey", "NET", "Record"]], on="TeamKey", how="left")
    df = df.merge(kp_df[["TeamKey", "KenPom"]], on="TeamKey", how="left")
    df = df.merge(bpi_df[["TeamKey", "BPI"]], on="TeamKey", how="left")
    df = df.merge(ap_df[["TeamKey", "AP"]], on="TeamKey", how="left")
    df = df.merge(sos_df[["TeamKey", "SoS"]], on="TeamKey", how="left")

    if games_path.exists():
        rec = _load_record_from_games(games_path, alias_map, net_fuzzy)
        df = df.merge(rec, on="TeamKey", how="left", suffixes=("", "_games"))
        if "Record_games" in df.columns:
            df["Record"] = df["Record"].fillna("")
            df["Record_games"] = df["Record_games"].fillna("")
            df["Record"] = df["Record"].where(df["Record"].astype(str).str.strip() != "", df["Record_games"])
            df = df.drop(columns=["Record_games"])

    df["NET"] = df["NET"].apply(_to_int_or_blank)
    df["KenPom"] = df["KenPom"].apply(_to_int_or_blank)
    df["BPI"] = df["BPI"].apply(_to_int_or_blank)
    df["SoS"] = df["SoS"].apply(_to_int_or_blank)
    df["AP"] = df["AP"].apply(_ap_to_display)
    df["Record"] = df["Record"].fillna("")

    # Filter to teams with at least one ranking (not just NET)
    has_ranking = (
        (df["NET"].astype(str).str.strip() != "") |
        (df["KenPom"].astype(str).str.strip() != "") |
        (df["BPI"].astype(str).str.strip() != "")
    )
    df = df[has_ranking].copy()
    
    # Sort by NET if available, otherwise by KenPom, otherwise by BPI
    df["sort_key"] = df["NET"].apply(lambda x: 999999 if x == "" else int(x))
    df = df.sort_values("sort_key").reset_index(drop=True)
    df = df.drop(columns=["sort_key"])

    df_out = pd.DataFrame(
        {
            "Team": df["TeamCanon"],
            "Record": df["Record"],
            "AP": df["AP"],
            "NET": df["NET"],
            "KenPom": df["KenPom"],
            "BPI": df["BPI"],
            "SoS": df["SoS"],
        }
    )

    print(f"Final output: {len(df_out)} teams")

    ts = _display_time_utc_minus5()
    payload = {"updated": ts, "last_updated": ts, "rows": df_out.to_dict(orient="records")}
    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote: {out_dir / 'rankings_current.json'}")


if __name__ == "__main__":
    main()
