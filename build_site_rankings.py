from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
import csv
import json
import re
import pandas as pd


def _team_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


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


def _load_alias_map(root: Path) -> dict[str, str]:
    candidates = [
        root / "data_raw" / "team_alias.csv",
        root / "team_alias.csv",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return {}

    m: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None)

        canon_idx = 0
        if header:
            h = [str(x).strip().lower() for x in header]
            for i, name in enumerate(h):
                if name in {"standard_name", "standard", "canonical", "canon", "team"}:
                    canon_idx = i
                    break

        for row in rdr:
            if not row:
                continue
            if canon_idx >= len(row):
                continue

            canon = str(row[canon_idx]).strip()
            if not canon:
                continue

            parts = []
            for i, v in enumerate(row):
                if i == canon_idx:
                    continue
                parts.append(v)

            extra = row[len(header) :] if header and len(row) > len(header) else []
            if extra:
                parts.extend(extra)

            vals = [canon]
            for p in parts:
                s = str(p).strip()
                if not s:
                    continue
                for tok in re.split(r"[|;/]", s):
                    t = tok.strip()
                    if t:
                        vals.append(t)

            for v in vals:
                k = str(v).strip().lower()
                if k:
                    m[k] = canon

    return m


def _canon_team(name: str, alias_map: dict[str, str]) -> str:
    k = str(name).strip().lower()
    return alias_map.get(k, str(name).strip())


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
            if "rank" in cl and "snapshot" not in cl and "date" not in cl:
                rank_col = c
                break

    if rank_col is None:
        for c in df.columns:
            if c != team_col and str(c).lower().strip() != "snapshot_date":
                rank_col = c
                break

    if rank_col is None:
        return pd.DataFrame(columns=["Team", "Rank"])

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    return out


def _load_record_from_kenpom(path: Path, alias_map: dict[str, str]) -> pd.DataFrame:
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

    rec_col = None
    for k in ["w-l", "w_l", "wl", "record"]:
        if k in cols:
            rec_col = cols[k]
            break
    if rec_col is None:
        for c in df.columns:
            cl = str(c).lower().replace(" ", "")
            if cl in {"w-l", "w_l", "wl"} or "w-l" in cl:
                rec_col = c
                break

    if rec_col is None:
        return pd.DataFrame(columns=["TeamKey", "Record"])

    tmp = df[[team_col, rec_col]].copy()
    tmp.columns = ["Team", "Record"]
    tmp["TeamCanon"] = tmp["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)
    tmp["Record"] = tmp["Record"].astype(str).str.strip()
    tmp = tmp[tmp["TeamKey"].astype(str).str.len() > 0]
    tmp = tmp[tmp["Record"].astype(str).str.len() > 0]
    tmp = tmp.drop_duplicates(subset=["TeamKey"], keep="first")
    return tmp[["TeamKey", "Record"]]


def _load_record_from_games(path: Path, alias_map: dict[str, str]) -> pd.DataFrame:
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

    tmp["TeamCanon"] = tmp["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
    tmp["TeamKey"] = tmp["TeamCanon"].map(_team_key)

    tmp["W"] = (tmp["Team_Score"] > tmp["Opponent_Score"]).astype(int)
    tmp["L"] = (tmp["Team_Score"] < tmp["Opponent_Score"]).astype(int)

    rec = tmp.groupby("TeamKey", as_index=False)[["W", "L"]].sum()
    rec["Record"] = rec["W"].astype(int).astype(str) + "-" + rec["L"].astype(int).astype(str)
    return rec[["TeamKey", "Record"]]


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

    if not net_path.exists():
        raise SystemExit("Missing data_raw/NET_Rank.csv")

    net = pd.read_csv(net_path)
    net = _latest_snapshot(net)

    net_cols = {c.lower().strip(): c for c in net.columns}
    net_team_col = net_cols.get("team", net.columns[0])

    net_rank_col = None
    for k in ["net_rank", "rank", "net"]:
        if k in net_cols:
            net_rank_col = net_cols[k]
            break
    if net_rank_col is None:
        if len(net.columns) > 1:
            net_rank_col = net.columns[1]
        else:
            raise SystemExit("Could not locate NET rank column")

    net2 = net[[net_team_col, net_rank_col]].copy()
    net2.columns = ["Team", "NET"]
    net2["NET"] = pd.to_numeric(net2["NET"], errors="coerce")
    net2 = net2.dropna(subset=["NET"])
    net2["NET"] = net2["NET"].astype(int)

    net2["TeamCanon"] = net2["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
    net2["TeamKey"] = net2["TeamCanon"].map(_team_key)
    net2 = net2[net2["TeamKey"].astype(str).str.len() > 0]
    net2 = net2.drop_duplicates(subset=["TeamKey"], keep="first")
    net2 = net2.sort_values("NET").reset_index(drop=True)

    base = net2[["TeamCanon", "TeamKey", "NET"]].copy()

    kp = _load_rank_csv(kp_path, ["kenpom_rank", "kenpom"]) if kp_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    kp = kp.rename(columns={"Rank": "KenPom"})
    if not kp.empty:
        kp["TeamCanon"] = kp["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        kp["TeamKey"] = kp["TeamCanon"].map(_team_key)
        kp = kp[["TeamKey", "KenPom"]]

    bpi = _load_rank_csv(bpi_path, ["bpi_rank", "bpi"]) if bpi_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    bpi = bpi.rename(columns={"Rank": "BPI"})
    if not bpi.empty:
        bpi["TeamCanon"] = bpi["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        bpi["TeamKey"] = bpi["TeamCanon"].map(_team_key)
        bpi = bpi[["TeamKey", "BPI"]]

    ap = _load_rank_csv(ap_path, ["ap_rank", "ap"]) if ap_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    ap = ap.rename(columns={"Rank": "AP"})
    if not ap.empty:
        ap["TeamCanon"] = ap["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        ap["TeamKey"] = ap["TeamCanon"].map(_team_key)
        ap = ap[["TeamKey", "AP"]]

    sos = _load_rank_csv(sos_path, ["sos", "rank"]) if sos_path.exists() else pd.DataFrame(columns=["Team", "Rank"])
    sos = sos.rename(columns={"Rank": "SOS"})
    if not sos.empty:
        sos["TeamCanon"] = sos["Team"].astype(str).map(lambda x: _canon_team(x, alias_map))
        sos["TeamKey"] = sos["TeamCanon"].map(_team_key)
        sos = sos[["TeamKey", "SOS"]]

    df = base.copy()
    if not kp.empty:
        df = df.merge(kp, on="TeamKey", how="left")
    else:
        df["KenPom"] = None

    if not bpi.empty:
        df = df.merge(bpi, on="TeamKey", how="left")
    else:
        df["BPI"] = None

    if not ap.empty:
        df = df.merge(ap, on="TeamKey", how="left")
    else:
        df["AP"] = None

    if not sos.empty:
        df = df.merge(sos, on="TeamKey", how="left")
    else:
        df["SOS"] = None

    rec_games = pd.DataFrame(columns=["TeamKey", "Record"])
    if games_path.exists():
        rec_games = _load_record_from_games(games_path, alias_map).rename(columns={"Record": "RecordGames"})

    rec_kp = pd.DataFrame(columns=["TeamKey", "Record"])
    if kp_path.exists():
        rec_kp = _load_record_from_kenpom(kp_path, alias_map).rename(columns={"Record": "RecordKP"})

    if not rec_games.empty:
        df = df.merge(rec_games, on="TeamKey", how="left")
    else:
        df["RecordGames"] = ""

    if not rec_kp.empty:
        df = df.merge(rec_kp, on="TeamKey", how="left")
    else:
        df["RecordKP"] = ""

    df["RecordKP"] = df["RecordKP"].fillna("").astype(str).str.strip()
    df["RecordGames"] = df["RecordGames"].fillna("").astype(str).str.strip()
    df["Record"] = df["RecordKP"].where(df["RecordKP"].str.len() > 0, df["RecordGames"]).fillna("")

    df = df.sort_values("NET").reset_index(drop=True)

    df_out = pd.DataFrame(
        {
            "Team": df["TeamCanon"],
            "Record": df["Record"],
            "AP": df["AP"].apply(_ap_to_display),
            "NET": df["NET"].apply(_to_int_or_blank),
            "KenPom": df["KenPom"].apply(_to_int_or_blank),
            "BPI": df["BPI"].apply(_to_int_or_blank),
            "SOS": df["SOS"].apply(_to_int_or_blank),
        }
    )

    ts = _display_time_utc_minus5()
    payload = {"updated": ts, "last_updated": ts, "rows": df_out.to_dict(orient="records")}
    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
