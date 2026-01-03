from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import json
import pandas as pd


def norm(s):
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ").strip().lower()
    s = s.replace("’", "'").replace("&", "and").replace(".", " ")
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def latest_snapshot(df):
    cols = {str(c).strip().lower(): c for c in df.columns}
    sd = cols.get("snapshot_date")
    if not sd:
        return df
    x = df[sd].astype(str)
    m = x.max()
    return df[x == m].copy()


def pick_team_and_rank(df, preferred_rank_names):
    df = latest_snapshot(df)
    cols = {str(c).strip().lower(): c for c in df.columns}

    team_col = None
    for k in ["team", "school", "name"]:
        if k in cols:
            team_col = cols[k]
            break
    if team_col is None:
        team_col = df.columns[0]

    rank_col = None
    for k in preferred_rank_names:
        if k in cols:
            rank_col = cols[k]
            break

    if rank_col is None:
        for c in df.columns:
            cl = str(c).strip().lower()
            if c != team_col and cl not in {"snapshot_date"} and "rank" in cl:
                rank_col = c
                break

    if rank_col is None:
        for c in df.columns:
            if c != team_col and str(c).strip().lower() != "snapshot_date":
                rank_col = c
                break

    out = df[[team_col, rank_col]].copy()
    out.columns = ["Team", "Rank"]
    return out


def to_int_or_blank(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    if s == "" or s.lower() in {"nr", "na", "none"}:
        return ""
    try:
        return int(float(s))
    except Exception:
        return ""


def build_alias_map(alias_df):
    m = {}
    for _, r in alias_df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        m[norm(std)] = std
        for col in alias_df.columns:
            v = str(r.get(col, "")).strip()
            if v:
                m[norm(v)] = std
    return m


def compute_avg_rank(df):
    tmp = df.copy()
    for c in ["NET", "KenPom", "BPI"]:
        tmp[c] = tmp[c].apply(to_int_or_blank)

    def avg_row(r):
        vals = []
        for c in ["NET", "KenPom", "BPI"]:
            v = r.get(c, "")
            if v != "":
                vals.append(float(v))
        return sum(vals) / len(vals) if vals else None

    tmp["_avg"] = tmp.apply(avg_row, axis=1)

    sortable = tmp[~tmp["_avg"].isna()].copy()
    sortable = sortable.sort_values(["_avg", "Team"], ascending=[True, True]).reset_index(drop=True)

    ranks = {}
    last_avg = None
    current_rank = 0
    seen = 0
    for _, r in sortable.iterrows():
        seen += 1
        a = r["_avg"]
        if last_avg is None or a != last_avg:
            current_rank = seen
            last_avg = a
        ranks[r["Team"]] = current_rank

    tmp["Avg of Metrics"] = tmp["Team"].map(lambda t: ranks.get(t, "NR"))
    tmp = tmp.drop(columns=["_avg"])
    return tmp


def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_path = root / "team_alias.csv"
    if not alias_path.exists():
        raise FileNotFoundError("Missing team_alias.csv at repo root")

    alias_df = pd.read_csv(alias_path, dtype=str).fillna("")
    if "standard_name" not in alias_df.columns:
        raise ValueError("team_alias.csv must include standard_name")

    alias_map = build_alias_map(alias_df)
    all_teams = alias_df["standard_name"].astype(str).map(lambda x: x.strip()).tolist()
    all_teams = [t for t in all_teams if t]

    def canon(team):
        return alias_map.get(norm(team), str(team).strip())

    base = pd.DataFrame({"Team": all_teams}).drop_duplicates()

    net_path = data_raw / "NET_Rank.csv"
    kp_path = data_raw / "KenPom_Rank.csv"
    bpi_path = data_raw / "BPI_Rank.csv"
    ap_path = data_raw / "AP_Rank.csv"
    sos_path = data_raw / "SOS_Rank.csv"

    if net_path.exists():
        net_df = pd.read_csv(net_path)
        net_df = latest_snapshot(net_df)
        cols = {str(c).strip().lower(): c for c in net_df.columns}
        team_col = cols.get("team", net_df.columns[0])
        net_col = cols.get("net", cols.get("net_rank", None))
        if net_col is None:
            net_col = next((c for c in net_df.columns if "net" in str(c).lower()), net_df.columns[1])
        rec_col = cols.get("record", None)
        use_cols = [team_col, net_col] + ([rec_col] if rec_col else [])
        net = net_df[use_cols].copy()
        net.columns = ["Team", "NET"] + (["Record"] if rec_col else [])
        net["Team"] = net["Team"].map(canon)
        net["NET"] = net["NET"].apply(to_int_or_blank)
        if "Record" not in net.columns:
            net["Record"] = ""
    else:
        net = pd.DataFrame(columns=["Team", "NET", "Record"])

    def load_rank(path, out_col, preferred):
        if not path.exists():
            return pd.DataFrame(columns=["Team", out_col])
        df = pd.read_csv(path)
        t = pick_team_and_rank(df, preferred)
        t["Team"] = t["Team"].map(canon)
        t[out_col] = t["Rank"].apply(to_int_or_blank)
        return t[["Team", out_col]]

    kp = load_rank(kp_path, "KenPom", ["kenpom", "kenpom_rank", "rank"])
    bpi = load_rank(bpi_path, "BPI", ["bpi", "bpi_rank", "rank"])
    ap = load_rank(ap_path, "AP", ["ap", "ap_rank", "rank"])
    sos = load_rank(sos_path, "SoS", ["sos", "sosa", "rank"])

    df = base.merge(net[["Team", "Record", "NET"]], on="Team", how="left")
    df = df.merge(kp, on="Team", how="left")
    df = df.merge(bpi, on="Team", how="left")
    df = df.merge(ap, on="Team", how="left")
    df = df.merge(sos, on="Team", how="left")

    for c in ["NET", "KenPom", "BPI", "AP", "SoS"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].apply(to_int_or_blank)

    df["Record"] = df["Record"].fillna("")

    df = compute_avg_rank(df)

    df["SoSa"] = df["SoS"]

    def sort_key(v):
        v = to_int_or_blank(v)
        return v if v != "" else 10**9

    df = df.sort_values(by="NET", key=lambda s: s.map(sort_key)).reset_index(drop=True)

    tz = ZoneInfo("America/New_York")
    updated = datetime.now(tz).strftime("%Y-%m-%d %-I:%M %p (ET)")

    payload = {
        "updated": updated,
        "rows": df[["Team", "Record", "Avg of Metrics", "AP", "NET", "KenPom", "BPI", "SoSa"]].to_dict(orient="records"),
    }

    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
