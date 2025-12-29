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

def load_rank(path: Path, team_col: str, rank_col: str, out_col: str, alias_map: dict):
    if not path.exists():
        return pd.DataFrame(columns=["Team", out_col])

    df = pd.read_csv(path).copy()
    if team_col not in df.columns or rank_col not in df.columns:
        raise RuntimeError(f"{path.name} missing {team_col} or {rank_col}. Found {df.columns.tolist()}")

    out = df[[team_col, rank_col]].copy()
    out["Team"] = out[team_col].astype(str).map(lambda x: canon_team(x, alias_map))
    out[out_col] = pd.to_numeric(out[rank_col], errors="coerce")
    out = out.dropna(subset=[out_col]).drop_duplicates(subset=["Team"], keep="first")
    return out[["Team", out_col]]

def to_int_or_blank(v):
    if pd.isna(v):
        return ""
    return int(v)

def ap_val(v):
    if pd.isna(v):
        return "NR"
    return int(v)

def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    out_dir = root / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    alias_path = data_raw / "team_alias.csv"
    if not alias_path.exists():
        alias_path = root / "team_alias.csv"

    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}

    net = load_rank(data_raw / "NET_Rank.csv", "Team", "NET_Rank", "NET", alias_map)
    kp = load_rank(data_raw / "KenPom_Rank.csv", "Team", "KenPom_Rank", "KenPom", alias_map)
    bpi = load_rank(data_raw / "BPI_Rank.csv", "Team", "BPI_Rank", "BPI", alias_map)
    ap = load_rank(data_raw / "AP_Rank.csv", "Team", "AP_Rank", "AP", alias_map)

    df = net.merge(kp, on="Team", how="left").merge(bpi, on="Team", how="left").merge(ap, on="Team", how="left")
    df = df.sort_values("NET", na_position="last")

    df["NET"] = df["NET"].apply(to_int_or_blank)
    df["KenPom"] = df["KenPom"].apply(to_int_or_blank)
    df["BPI"] = df["BPI"].apply(to_int_or_blank)
    df["AP"] = df["AP"].apply(ap_val)

    payload = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "rows": df.to_dict(orient="records"),
    }

    (out_dir / "rankings_current.json").write_text(json.dumps(payload, ensure_ascii=False))
    print(f"Wrote {out_dir / 'rankings_current.json'}")

if __name__ == "__main__":
    main()
