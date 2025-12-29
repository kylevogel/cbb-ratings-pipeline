import json
from datetime import datetime
from pathlib import Path
import pandas as pd

def pick_col(cols, candidates):
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    for c in cols:
        lc = c.lower()
        for cand in candidates:
            if cand.lower() in lc:
                return c
    return None

def norm(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = " ".join(s.split())
    return s

def build_alias_map(alias_path):
    df = pd.read_csv(alias_path)
    df.columns = [str(c).strip() for c in df.columns]
    canon_col = pick_col(df.columns, ["ESPN", "Team", "Canonical", "School"])
    if canon_col is None:
        canon_col = df.columns[0]

    m = {}
    canon_vals = df[canon_col].astype(str).map(lambda x: str(x).strip())
    for i in range(len(df)):
        canon = canon_vals.iloc[i]
        if not canon or canon.lower() == "nan":
            continue
        for c in df.columns:
            v = df.iloc[i][c]
            if pd.isna(v):
                continue
            key = norm(v)
            if key and key != "nan":
                m[key] = canon
    return canon_col, m, df

def infer_team_rank(df):
    df.columns = [str(c).strip() for c in df.columns]
    team_col = pick_col(df.columns, ["Team", "School"])
    rank_col = pick_col(df.columns, ["Rank", "RK", "BPI", "KenPom", "NET", "AP_Rank"])
    if team_col is None:
        team_col = df.columns[0]
    if rank_col is None:
        rank_col = df.columns[-1]
    return team_col, rank_col

def load_rank(path, out_col, alias_map):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["Team", out_col])

    df = pd.read_csv(p)
    team_col, rank_col = infer_team_rank(df)

    out = df[[team_col, rank_col]].copy()
    out.columns = ["TeamRaw", out_col]
    out["Team"] = out["TeamRaw"].map(lambda x: alias_map.get(norm(x), str(x).strip()))
    out[out_col] = pd.to_numeric(out[out_col], errors="coerce")
    out = out.dropna(subset=["Team"])
    out = out.drop_duplicates(subset=["Team"], keep="first")
    return out[["Team", out_col]]

def main():
    canon_col, alias_map, alias_df = build_alias_map("team_alias.csv")

    base = pd.DataFrame({"Team": alias_df[canon_col].astype(str).map(lambda x: str(x).strip())})
    base = base[base["Team"].str.lower() != "nan"]
    base = base.drop_duplicates(subset=["Team"], keep="first")

    net = load_rank("data_raw/NET_Rank.csv", "NET", alias_map)
    kp = load_rank("data_raw/KenPom_Rank.csv", "KenPom", alias_map)
    bpi = load_rank("data_raw/BPI_Rank.csv", "BPI", alias_map)
    ap = load_rank("data_raw/AP_Rank.csv", "AP", alias_map)

    df = base.merge(net, on="Team", how="left").merge(kp, on="Team", how="left").merge(bpi, on="Team", how="left").merge(ap, on="Team", how="left")

    def to_int_or_blank(v):
        if pd.isna(v):
            return ""
        return int(v)

    def ap_val(v):
        if pd.isna(v):
            return "NR"
        return int(v)

    df["NET"] = df["NET"].apply(to_int_or_blank)
    df["KenPom"] = df["KenPom"].apply(to_int_or_blank)
    df["BPI"] = df["BPI"].apply(to_int_or_blank)
    df["AP"] = df["AP"].apply(ap_val)

    out_dir = Path("docs") / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "rows": df.sort_values(by=["Team"]).to_dict(orient="records")
    }

    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False))

if __name__ == "__main__":
    main()
