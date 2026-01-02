import os
import re
import json
import difflib
import datetime as dt
from io import StringIO
from pathlib import Path

import pandas as pd
import requests


def _fuzzy_key(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("&", " and ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\b(university|college)\b", " ", s)
    s = re.sub(r"\bsaint\b", " st ", s)
    s = re.sub(r"\bstate\b", " st ", s)
    s = re.sub(r"\bnorth carolina\b", " nc ", s)
    s = re.sub(r"\bsouth carolina\b", " sc ", s)
    s = re.sub(r"\bnorth dakota\b", " nd ", s)
    s = re.sub(r"\bsouth dakota\b", " sd ", s)
    s = re.sub(r"\bnorth carolina\b", " nc ", s)
    s = re.sub(r"\bsouth carolina\b", " sc ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _best_match(src: str, standards_fk: list[tuple[str, str]]) -> tuple[str, float]:
    fk = _fuzzy_key(src)
    best_std = ""
    best_score = 0.0
    for std, std_fk in standards_fk:
        score = difflib.SequenceMatcher(None, fk, std_fk).ratio()
        if score > best_score:
            best_score = score
            best_std = std
    return best_std, best_score


def _load_alias_map(team_alias_path: Path) -> tuple[dict[str, str], list[tuple[str, str]]]:
    df = pd.read_csv(team_alias_path, dtype=str).fillna("")
    if "standard_name" not in df.columns:
        raise SystemExit("team_alias.csv must contain a 'standard_name' column")

    alias = {}
    standards = []
    seen_std = set()

    for _, r in df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue

        if std not in seen_std:
            standards.append(std)
            seen_std.add(std)

        for c in df.columns:
            v = str(r.get(c, "")).strip()
            if not v:
                continue
            alias[_fuzzy_key(v)] = std

    manual = {
        "umkc": "Kansas City",
        "uncg": "UNC Greensboro",
        "uta": "UT Arlington",
        "umass": "Massachusetts",
        "iu indianapolis": "IU Indy",
        "loyola maryland": "Loyola MD",
        "loyola md": "Loyola MD",
        "seattle u": "Seattle",
        "seattle university": "Seattle",
        "saint marys": "Saint Mary's",
        "saint marys college": "Saint Mary's",
        "mount saint marys": "Mount St. Mary's",
        "st marys": "Saint Mary's",
    }

    for k, v in manual.items():
        alias[_fuzzy_key(k)] = v

    standards_fk = [(std, _fuzzy_key(std)) for std in standards]
    return alias, standards_fk


def _pick_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        cols = [str(c).lower().strip() for c in t.columns]
        has_team = any("team" in c for c in cols)
        has_rank = any(c in ("rank", "rk", "#") or "rank" in c for c in cols)
        if has_team and has_rank and len(t) > 200:
            return t
    for t in tables:
        cols = [str(c).lower().strip() for c in t.columns]
        has_team = any("team" in c for c in cols)
        has_rank = any(c in ("rank", "rk", "#") or "rank" in c for c in cols)
        if has_team and has_rank:
            return t
    raise SystemExit("Could not find a SoS table with Team + Rank columns")


def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)

    season = int(os.getenv("SEASON", "2026"))
    url = f"https://www.warrennolan.com/basketball/{season}/sos-rpi-predict"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.warrennolan.com/",
    }

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"Failed to fetch SoS page: HTTP {r.status_code}")

    tables = pd.read_html(StringIO(r.text))
    t = _pick_table(tables)

    cols = {str(c).lower().strip(): c for c in t.columns}
    team_col = None
    for k, v in cols.items():
        if "team" in k:
            team_col = v
            break
    if team_col is None:
        raise SystemExit("Could not identify Team column")

    rank_col = None
    for k, v in cols.items():
        if k in ("rank", "rk", "#") or "rank" in k:
            rank_col = v
            break
    if rank_col is None:
        rank_col = t.columns[0]

    df = t[[rank_col, team_col]].copy()
    df.columns = ["SoS", "Team"]

    df["Team"] = df["Team"].astype(str).str.strip()
    df["SoS"] = pd.to_numeric(df["SoS"], errors="coerce")
    df = df.dropna(subset=["SoS", "Team"])
    df["SoS"] = df["SoS"].astype(int)

    team_alias_path = root / "team_alias.csv"
    alias_map, standards_fk = _load_alias_map(team_alias_path)

    matched_rows = []
    unmatched_rows = []

    for _, row in df.iterrows():
        src = str(row["Team"]).strip()
        rk = int(row["SoS"])

        fk = _fuzzy_key(src)
        std = alias_map.get(fk, "")
        best_std = ""
        best_score = 0.0

        if std:
            matched_rows.append((std, rk))
            continue

        best_std, best_score = _best_match(src, standards_fk)

        if best_score >= 0.86:
            matched_rows.append((best_std, rk))
        else:
            unmatched_rows.append((src, best_std, best_score))

    out = pd.DataFrame(matched_rows, columns=["Team", "SoS"])
    out = out.drop_duplicates(subset=["Team"], keep="first").copy()
    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out.insert(0, "snapshot_date", snapshot_date)

    out_path = data_raw / "SOS_Rank.csv"
    out.to_csv(out_path, index=False)

    um = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_path = data_raw / "unmatched_sos_teams.csv"
    um.to_csv(um_path, index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print(f"Pulled source teams: {len(df)}")
    print(f"Matched: {len(out)}")
    print(f"Unmatched: {len(um)}")

    if len(um) > 0:
        print(um.head(25).to_string(index=False))


if __name__ == "__main__":
    main()
