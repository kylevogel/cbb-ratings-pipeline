import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


def _norm(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


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


def _safe_read_csv(path: Path):
    if path is None or not Path(path).exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, engine="python", on_bad_lines="skip")
        except Exception:
            return None


def _pick_existing(data_raw: Path, preferred_names):
    for n in preferred_names:
        p = data_raw / n
        if p.exists():
            return p
    return None


def _pick_by_contains(data_raw: Path, substrings):
    subs = [s.lower() for s in substrings]
    matches = []
    for p in data_raw.glob("*.csv"):
        name = p.name.lower()
        if all(s in name for s in subs):
            matches.append(p)
    if not matches:
        return None
    matches = sorted(matches)
    return matches[-1]


def load_rank(path: Path, team_col: str, rank_col: str, out_col: str, alias_map: dict):
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=["Team", out_col])
    df = _safe_read_csv(Path(path))
    if df is None or df.empty:
        return pd.DataFrame(columns=["Team", out_col])
    if team_col not in df.columns or rank_col not in df.columns:
        return pd.DataFrame(columns=["Team", out_col])
    out = df[[team_col, rank_col]].copy()
    out["Team"] = out[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    out[out_col] = pd.to_numeric(out[rank_col], errors="coerce")
    out = out.dropna(subset=[out_col]).drop_duplicates(subset=["Team"], keep="first")
    return out[["Team", out_col]]


def load_records(data_raw: Path, alias_map: dict):
    p = _pick_by_contains(data_raw, ["games", "2024"])
    if p is None:
        p = _pick_by_contains(data_raw, ["games"])
    if p is None:
        return pd.DataFrame(columns=["Team", "Record"])

    df = _safe_read_csv(p)
    if df is None or df.empty or "Team" not in df.columns:
        return pd.DataFrame(columns=["Team", "Record"])

    tmp = df.copy()
    tmp["Team"] = tmp["Team"].astype(str).map(lambda x: canon_team(x, alias_map))

    win_col = "Win?" if "Win?" in tmp.columns else ("Win" if "Win" in tmp.columns else None)
    if win_col is not None:
        v = tmp[win_col].astype(str).str.strip().str.lower()
        is_win = v.isin({"w", "win", "true", "t", "1", "yes", "y"})
        is_loss = v.isin({"l", "loss", "false", "f", "0", "no", "n"})
    else:
        if "Team_Score" in tmp.columns and "Opponent_Score" in tmp.columns:
            ts = pd.to_numeric(tmp["Team_Score"], errors="coerce")
            os_ = pd.to_numeric(tmp["Opponent_Score"], errors="coerce")
            is_win = (ts > os_) & ts.notna() & os_.notna()
            is_loss = (ts < os_) & ts.notna() & os_.notna()
        else:
            return pd.DataFrame(columns=["Team", "Record"])

    rec = (
        tmp.assign(_W=is_win.astype(int), _L=is_loss.astype(int))
        .groupby("Team", as_index=False)[["_W", "_L"]]
        .sum()
    )
    rec["Record"] = rec["_W"].astype(int).astype(str) + "-" + rec["_L"].astype(int).astype(str)
    return rec[["Team", "Record"]]


def load_sos(data_raw: Path, alias_map: dict, net_df: pd.DataFrame):
    p = _pick_by_contains(data_raw, ["games", "2024"])
    if p is None:
        p = _pick_by_contains(data_raw, ["games"])
    if p is None:
        return pd.DataFrame(columns=["Team", "Strength of Schedule"])

    games = _safe_read_csv(p)
    if games is None or games.empty or "Team" not in games.columns or "Opponent" not in games.columns:
        return pd.DataFrame(columns=["Team", "Strength of Schedule"])

    games = games.copy()
    games["Team"] = games["Team"].astype(str).map(lambda x: canon_team(x, alias_map))
    games["Opponent"] = games["Opponent"].astype(str).map(lambda x: canon_team(x, alias_map))

    net_map = net_df.set_index("Team")["NET"].to_dict()
    games["Opp_NET"] = games["Opponent"].map(net_map)
    games["Opp_NET"] = pd.to_numeric(games["Opp_NET"], errors="coerce").fillna(366)

    avg = games.groupby("Team", as_index=False)["Opp_NET"].mean()
    avg = avg.rename(columns={"Opp_NET": "Opp_NET_AVG"})

    base = pd.DataFrame({"Team": net_df["Team"].astype(str).tolist()})
    base = base.merge(avg, on="Team", how="left")
    base["Opp_NET_AVG"] = base["Opp_NET_AVG"].fillna(366)

    base = base.sort_values(["Opp_NET_AVG", "Team"], ascending=[True, True]).reset_index(drop=True)
    base["Strength of Schedule"] = range(1, len(base) + 1)

    return base[["Team", "Strength of Schedule"]]


def _to_int_or_blank(v):
    if pd.isna(v):
        return ""
    try:
        return int(v)
    except Exception:
        return ""


def _ap_or_nr(v):
    if pd.isna(v):
        return "NR"
    try:
        return int(v)
    except Exception:
        return "NR"


def load_previous(out_path: Path):
    if not out_path.exists():
        return pd.DataFrame(columns=["Team", "KenPom", "BPI", "AP", "Record", "Strength of Schedule"])
    try:
        payload = json.loads(out_path.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        df = pd.DataFrame(rows)
        if "Team" not in df.columns:
            return pd.DataFrame(columns=["Team", "KenPom", "BPI", "AP", "Record", "Strength of Schedule"])
        keep = [c for c in ["Team", "KenPom", "BPI", "AP", "Record", "Strength of Schedule"] if c in df.columns]
        return df[keep].copy()
    except Exception:
        return pd.DataFrame(columns=["Team", "KenPom", "BPI", "AP", "Record", "Strength of Schedule"])


def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "rankings_current.json"

    alias_path = data_raw / "team_alias.csv"
    if not alias_path.exists():
        alias_path = root / "team_alias.csv"
    alias_map = load_alias_map(alias_path)

    net_path = _pick_existing(data_raw, ["NET_Rank.csv", "net_rank.csv", "NET.csv", "net.csv"])
    kp_path = _pick_existing(data_raw, ["KenPom_Rank.csv", "kenpom_rank.csv", "KenPom.csv", "kenpom.csv"])
    bpi_path = _pick_existing(data_raw, ["BPI_Rank.csv", "bpi_rank.csv", "BPI.csv", "bpi.csv"])
    ap_path = _pick_existing(data_raw, ["AP_Rank.csv", "ap_rank.csv", "AP.csv", "ap.csv"])

    if bpi_path is None:
        bpi_path = _pick_by_contains(data_raw, ["bpi"])
    if ap_path is None:
        ap_path = _pick_by_contains(data_raw, ["ap"])
    if kp_path is None:
        kp_path = _pick_by_contains(data_raw, ["kenpom"])
    if net_path is None:
        net_path = _pick_by_contains(data_raw, ["net"])

    net = load_rank(net_path, "Team", "NET_Rank", "NET", alias_map)
    if net.empty and net_path is not None:
        df_try = _safe_read_csv(net_path)
        if df_try is not None and "Team" in df_try.columns:
            for c in ["NET", "Rank", "NET Rank", "NET_Rank"]:
                if c in df_try.columns:
                    net = load_rank(net_path, "Team", c, "NET", alias_map)
                    break

    net["NET"] = pd.to_numeric(net["NET"], errors="coerce")
    net = net.dropna(subset=["NET"])
    net = net[(net["NET"] >= 1) & (net["NET"] <= 365)]
    net = net.drop_duplicates(subset=["Team"], keep="first")

    prev = load_previous(out_path)

    kp = load_rank(kp_path, "Team", "KenPom_Rank", "KenPom", alias_map)
    if kp.empty and kp_path is not None:
        df_try = _safe_read_csv(kp_path)
        if df_try is not None and "Team" in df_try.columns:
            for c in ["KenPom", "Rank", "KenPom Rank", "KenPom_Rank"]:
                if c in df_try.columns:
                    kp = load_rank(kp_path, "Team", c, "KenPom", alias_map)
                    break

    bpi = load_rank(bpi_path, "Team", "BPI_Rank", "BPI", alias_map)
    if bpi.empty and bpi_path is not None:
        df_try = _safe_read_csv(bpi_path)
        if df_try is not None and "Team" in df_try.columns:
            for c in ["BPI", "Rank", "BPI Rank", "BPI_Rank"]:
                if c in df_try.columns:
                    bpi = load_rank(bpi_path, "Team", c, "BPI", alias_map)
                    break

    ap = load_rank(ap_path, "Team", "AP_Rank", "AP", alias_map)
    if ap.empty and ap_path is not None:
        df_try = _safe_read_csv(ap_path)
        if df_try is not None and "Team" in df_try.columns:
            for c in ["AP", "Rank", "AP Rank", "AP_Rank"]:
                if c in df_try.columns:
                    ap = load_rank(ap_path, "Team", c, "AP", alias_map)
                    break

    rec = load_records(data_raw, alias_map)
    sos = load_sos(data_raw, alias_map, net)

    df = net.merge(sos, on="Team", how="left").merge(kp, on="Team", how="left").merge(bpi, on="Team", how="left").merge(ap, on="Team", how="left")

    if not rec.empty:
        df = df.merge(rec, on="Team", how="left")
    else:
        df["Record"] = ""

    for col in ["KenPom", "BPI", "AP", "Record", "Strength of Schedule"]:
        if col in prev.columns and col in df.columns:
            df[col] = df[col].where(
                df[col].notna() & (df[col].astype(str) != ""),
                prev.set_index("Team").reindex(df["Team"])[col].values,
            )

    df = df.sort_values("NET", na_position="last")

    df["Record"] = df["Record"].fillna("")
    df["Strength of Schedule"] = df["Strength of Schedule"].apply(_to_int_or_blank)
    df["NET"] = df["NET"].apply(_to_int_or_blank)
    df["KenPom"] = df["KenPom"].apply(_to_int_or_blank)
    df["BPI"] = df["BPI"].apply(_to_int_or_blank)
    df["AP"] = df["AP"].apply(_ap_or_nr)

    payload = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "rows": df[["Team", "Record", "Strength of Schedule", "NET", "KenPom", "BPI", "AP"]].to_dict(orient="records"),
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
