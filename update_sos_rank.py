from pathlib import Path
import pandas as pd
import requests
import datetime as dt
import re
from io import StringIO
from difflib import SequenceMatcher

SEASON = 2026

def _root():
    return Path(__file__).resolve().parent

def _data_raw(root: Path):
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p

def _norm(s: str) -> str:
    s = str(s or "").lower().strip()
    s = s.replace("&", " and ")
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("-", " ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[().,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bcollege\b$", "", s).strip()
    return s

def _variants(name: str):
    base = _norm(name)
    out = {base}
    if " state" in base:
        out.add(base.replace(" state", " st"))
    if re.search(r"\bst\b", base):
        out.add(re.sub(r"\bst\b", "saint", base))
    if " saint " in f" {base} ":
        out.add(re.sub(r"\bsaint\b", "st", base))
    if base.endswith(" university"):
        out.add(base.replace(" university", " u"))
    if base.endswith(" u"):
        out.add(base.replace(" u", " university"))
    out2 = set()
    for v in out:
        out2.add(re.sub(r"\s+", " ", v).strip())
    return out2

def _build_lookup(alias_df: pd.DataFrame):
    lookup = {}
    standards = []

    if "standard_name" in alias_df.columns:
        standards = [x for x in alias_df["standard_name"].tolist() if str(x).strip()]

    cols = [c for c in ["standard_name", "net_name", "kenpom_name", "bpi_name", "game_log_name"] if c in alias_df.columns]

    for _, r in alias_df.iterrows():
        std = str(r.get("standard_name","")).strip()
        if not std:
            continue
        for c in cols:
            v = str(r.get(c,"")).strip()
            if not v:
                continue
            for k in _variants(v):
                lookup.setdefault(k, std)

    for std in standards:
        for k in _variants(std):
            lookup.setdefault(k, std)

    return lookup

def _best_suggestion(src: str, lookup_keys):
    s = _norm(src)
    best = None
    best_score = 0.0
    for k in lookup_keys:
        sc = SequenceMatcher(None, s, k).ratio()
        if sc > best_score:
            best_score = sc
            best = k
    return best, best_score

def _get_html(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.warrennolan.com/",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def _pick_table(tables):
    best = None
    best_len = -1
    for t in tables:
        df = t.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([str(x) for x in col if str(x) != "nan"]).strip() for col in df.columns]
        cols = [str(c).strip().lower() for c in df.columns]
        has_rank = any(c == "rank" or c.endswith(" rank") or "rank" == c for c in cols) or any("rank" in c for c in cols)
        has_team = any(c == "team" or c.endswith(" team") or "team" == c for c in cols) or any("team" in c for c in cols)
        if has_rank and has_team and len(df) > best_len:
            best = df
            best_len = len(df)
    return best

def _find_col(df: pd.DataFrame, needle: str):
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == needle:
            return c
    for c in df.columns:
        cl = str(c).strip().lower()
        if needle in cl:
            return c
    return None

def main():
    root = _root()
    data_raw = _data_raw(root)

    alias_path = root / "team_alias.csv"
    alias_df = pd.read_csv(alias_path, dtype=str).fillna("") if alias_path.exists() else pd.DataFrame(columns=["standard_name"])
    lookup = _build_lookup(alias_df)
    lookup_keys = list(lookup.keys())

    url = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"
    html = _get_html(url)

    tables = pd.read_html(StringIO(html))
    df = _pick_table(tables)
    if df is None or len(df) < 300:
        raise SystemExit(f"Could not find full SoS table on WarrenNolan. Found {0 if df is None else len(df)} rows.")

    rank_col = _find_col(df, "rank")
    team_col = _find_col(df, "team")
    if rank_col is None or team_col is None:
        raise SystemExit(f"Could not identify Rank/Team columns. Columns: {list(df.columns)}")

    out_rows = []
    unmatched = []

    for _, r in df.iterrows():
        team_src = str(r.get(team_col, "")).strip()
        rank_raw = str(r.get(rank_col, "")).strip()
        if not team_src:
            continue

        m = re.search(r"\d+", rank_raw)
        if not m:
            continue
        rank = int(m.group(0))

        std = lookup.get(_norm(team_src))
        if std is None:
            found = None
            for k in _variants(team_src):
                if k in lookup:
                    found = lookup[k]
                    break
            std = found

        if std is None:
            best_k, score = _best_suggestion(team_src, lookup_keys) if lookup_keys else (None, 0.0)
            sug = lookup.get(best_k) if best_k else ""
            unmatched.append({"source_team": team_src, "suggested_standard": sug, "match_score": score})
        else:
            out_rows.append({"Team": std, "SoS": rank})

    snap = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out = pd.DataFrame(out_rows).drop_duplicates(subset=["Team"], keep="first").sort_values("SoS").reset_index(drop=True)
    out.insert(0, "snapshot_date", snap)

    out_path = data_raw / "SOS_Rank.csv"
    out.to_csv(out_path, index=False)

    um_path = data_raw / "unmatched_sos_teams.csv"
    pd.DataFrame(unmatched).sort_values("match_score", ascending=False).to_csv(um_path, index=False)

    print(out_path.name)
    print(um_path.name)
    print("rows", len(out))
    print("unmatched", len(unmatched))

if __name__ == "__main__":
    main()
