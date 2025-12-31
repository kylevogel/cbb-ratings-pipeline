from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
import csv
import json
import re

import pandas as pd


def _team_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _load_alias_map(root: Path) -> dict[str, str]:
    candidates = [
        root / "data_raw" / "team_alias.csv",
        root / "team_alias.csv",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return {}

    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            return {}

        header = [str(h).strip() for h in header]
        header_l = [h.strip().lower() for h in header]
        if not header_l:
            return {}

        std_idx = None
        for k in ["standard_name", "standard", "canonical", "canon", "team"]:
            if k in header_l:
                std_idx = header_l.index(k)
                break
        if std_idx is None:
            std_idx = 0

        m: dict[str, str] = {}

        for row in r:
            if not row:
                continue

            if len(row) > len(header):
                row = row[: len(header) - 1] + [",".join(row[len(header) - 1 :])]
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))

            standard = str(row[std_idx]).strip()
            if not standard:
                continue

            for cell in row:
                a = str(cell).strip()
                if not a:
                    continue
                m[a.lower()] = standard

    return m


def _canon_team(name: str, alias_map: dict[str, str]) -> str:
    k = str(name).strip().lower()
    return alias_map.get(k, str(name).strip())


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    sd = cols.get("snapshot_date")
    if sd is None:
        return df
    try:
        mx = df[sd].astype(str).max()
        return df[df[sd].astype(str) == mx].copy()
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


def _load_kenpom(path: Path) -> pd.DataFrame:
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
    for k in ["kenpom_rank", "kenpom", "rk", "rank"]:
        if k in cols:
            rank_col = cols[k]
            break
    if rank_col is None and len(df.columns) >= 2:
        rank_col = df.columns[1]

    rec_col = None
    for k in ["w-l", "wl", "w_l", "record"]:
        if k in cols:
            rec_col = cols[k]
            break

    keep = [team_col, rank_col] + ([rec_col] if rec_col is not None else [])
    out = df[keep].copy()
    out.columns = ["Team", "KenPom"] + (["Record"] if rec_col is not None else [])
    return out


def _load_record_from_games(path: Path, alias_map: dict[str, str]) -> pd.DataFrame:
    g = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in g.columns}
    team_col = cols.get("team")
    ts_col = cols.get("team_score")
    os_col = cols.get("opponent_score")

    if team_col is None or ts_col is None or os_col is None:
        return pd.DataFrame(columns=["TeamKey", "Record_games"])

    tmp = g[[team_col, ts_col, os_col]].copy()
    tmp.columns = ["Team", "Team_Score", "Opponent_Score"]
    tmp["Team_Score"] = pd.to_numeric(tmp["Team_Score"], errors="coerce")
    tmp["Opponent_Score"] = pd.to_numeric(tmp["Opponent_Score"], errors="coerce")
    tmp = tmp.dropna(subset=["Team_Score", "Opponent_Score"])

    tmp["TeamCanon"] = tmp["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)

    tmp["W"] = (tmp["Team_Score"] > tmp["Opponent_Score"]).astype(int)
    tmp["L"] = (tmp["Team_Score"] < tmp["Opponent_Score"]).astype(int)

    rec = tmp.groupby("TeamKey", as_index=False)[["W", "L"]].sum()
    rec["Record_games"] = rec["W"].astype(int).astype(str) + "-" + rec["L"].astype(int).astype(str)
    return rec[["TeamKey", "Record_games"]]


def _to_int_or_blank(x):
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    try:
        return int(float(x))
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


def main() -> None:
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_map = _load_alias_map(root)

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
        net2 = net[[team_col, rank_col]].copy()
        net2.columns = ["Team", "NET"]
    else:
        net2 = pd.DataFrame(columns=["Team", "NET"])

    kp_raw = _load_kenpom(kp_path) if kp_path.exists() else pd.DataFrame(columns=["Team", "KenPom"])
    bpi = _load_rank_csv(bpi_path, ["bpi_rank", "bpi"]) if bpi_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    bpi = bpi.rename(columns={"Rank": "BPI"})
    ap = _load_rank_csv(ap_path, ["ap_rank", "ap"]) if ap_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    ap = ap.rename(columns={"Rank": "AP"})
    sos = _load_rank_csv(sos_path, ["sos", "rank"]) if sos_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    sos = sos.rename(columns={"Rank": "SoS"})

    def prep(df: pd.DataFrame, col: str) -> pd.DataFrame:
        d = df.copy()
        if "Team" not in d.columns:
            return pd.DataFrame(columns=["TeamCanon", "TeamKey", col])
        d["TeamCanon"] = d["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        d["TeamKey"] = d["TeamCanon"].map(_team_key)
        return d[["TeamCanon", "TeamKey", col]].copy()

    net_df = prep(net2, "NET")
    kp_rank_df = kp_raw[["Team", "KenPom"]].copy() if "KenPom" in kp_raw.columns else pd.DataFrame(columns=["Team", "KenPom"])
    kp_df = prep(kp_rank_df, "KenPom")
    bpi_df = prep(bpi.rename(columns={"BPI": "BPI"}), "BPI") if "BPI" in bpi.columns else pd.DataFrame(columns=["TeamCanon", "TeamKey", "BPI"])
    ap_df = prep(ap.rename(columns={"AP": "AP"}), "AP") if "AP" in ap.columns else pd.DataFrame(columns=["TeamCanon", "TeamKey", "AP"])
    sos_df = prep(sos.rename(columns={"SoS": "SoS"}), "SoS") if "SoS" in sos.columns else pd.DataFrame(columns=["TeamCanon", "TeamKey", "SoS"])

    base = pd.concat(
        [
            net_df[["TeamCanon", "TeamKey"]],
            kp_df[["TeamCanon", "TeamKey"]],
            bpi_df[["TeamCanon", "TeamKey"]],
            ap_df[["TeamCanon", "TeamKey"]],
            sos_df[["TeamCanon", "TeamKey"]],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["TeamKey"])

    df = base.copy()
    df = df.merge(net_df[["TeamKey", "NET"]], on="TeamKey", how="left")
    df = df.merge(kp_df[["TeamKey", "KenPom"]], on="TeamKey", how="left")
    df = df.merge(bpi_df[["TeamKey", "BPI"]], on="TeamKey", how="left")
    df = df.merge(ap_df[["TeamKey", "AP"]], on="TeamKey", how="left")
    df = df.merge(sos_df[["TeamKey", "SoS"]], on="TeamKey", how="left")

    rec_games = _load_record_from_games(games_path, alias_map) if games_path.exists() else pd.DataFrame(columns=["TeamKey", "Record_games"])
    df = df.merge(rec_games, on="TeamKey", how="left")

    if "Record" in kp_raw.columns:
        kp_rec = kp_raw[["Team", "Record"]].copy()
        kp_rec["TeamCanon"] = kp_rec["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        kp_rec["TeamKey"] = kp_rec["TeamCanon"].map(_team_key)
        kp_rec = kp_rec[["TeamKey", "Record"]].copy()
        kp_rec["Record"] = kp_rec["Record"].astype(str).str.strip()
        kp_rec.loc[kp_rec["Record"] == "", "Record"] = pd.NA
        df = df.merge(kp_rec.rename(columns={"Record": "Record_kp"}), on="TeamKey", how="left")
    else:
        df["Record_kp"] = pd.NA

    df["Record_games"] = df["Record_games"].astype(str).str.strip()
    df.loc[df["Record_games"] == "", "Record_games"] = pd.NA

    df["Record"] = df["Record_kp"].fillna(df["Record_games"]).fillna("0-0")

    df = df.sort_values("NET", na_position="last").reset_index(drop=True)

    df_out = pd.DataFrame(
        {
            "Team": df["TeamCanon"],
            "Record": df["Record"],
            "AP": df["AP"].apply(_ap_to_display),
            "NET": df["NET"].apply(_to_int_or_blank),
            "KenPom": df["KenPom"].apply(_to_int_or_blank),
            "BPI": df["BPI"].apply(_to_int_or_blank),
            "SoS": df["SoS"].apply(_to_int_or_blank),
        }
    )

    ts = _display_time_utc_minus5()
    payload = {"updated": ts, "last_updated": ts, "rows": df_out.to_dict(orient="records")}
    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
