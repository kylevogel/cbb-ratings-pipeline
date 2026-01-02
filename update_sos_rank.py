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
        "seattle university": "Seattle",
        "seattle u": "Seattle",
        "saint marys college": "Saint Mary's",
        "mount saint marys": "Mount St. Mary's",
        "saint johns": "St. John's",
        "nc state": "N.C. State",
    }
    for k, v in manual.items():
        alias[_fuzzy_key(k)] = v

    standards_fk = [(std, _fuzzy_key(std)) for std in standards]
    return alias, standards_fk


def _extract_next_data(html: str) -> dict | None:
    m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.S)
    if not m:
        return None
    raw = m.group(1).strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _walk(obj):
    stack = [obj]
    while stack:
        x = stack.pop()
        yield x
        if isinstance(x, dict):
            for v in x.values():
                stack.append(v)
        elif isinstance(x, list):
            for v in x:
                stack.append(v)


def _get_team_str(x):
    if isinstance(x, str):
        return x.strip()
    if isinstance(x, dict):
        for k in ["team", "teamName", "team_name", "name", "school", "displayName", "shortName"]:
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in x.values():
            t = _get_team_str(v)
            if t:
                return t
    if isinstance(x, list):
        for v in x:
            t = _get_team_str(v)
            if t:
                return t
    return ""


def _get_rank_int(d):
    if not isinstance(d, dict):
        return None
    for k in ["rank", "rk", "sosRank", "sos_rank", "sosrank", "Rank", "Rk"]:
        v = d.get(k)
        try:
            if v is None:
                continue
            iv = int(str(v).strip())
            if iv > 0:
                return iv
        except Exception:
            pass
    for k, v in d.items():
        if isinstance(k, str) and "rank" in k.lower():
            try:
                iv = int(str(v).strip())
                if iv > 0:
                    return iv
            except Exception:
                pass
    return None


def _candidate_rows_from_next_data(next_data: dict) -> list[tuple[int, str]]:
    best = []
    for x in _walk(next_data):
        if not isinstance(x, list) or len(x) < 250:
            continue
        if not all(isinstance(v, dict) for v in x[:25]):
            continue

        rows = []
        for d in x:
            rk = _get_rank_int(d)
            team = _get_team_str(d)
            if rk is None or not team:
                continue
            rows.append((rk, team))

        if len(rows) > len(best):
            best = rows

    return best


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

    source_rows = []
    next_data = _extract_next_data(r.text)
    if next_data is not None:
        cand = _candidate_rows_from_next_data(next_data)
        if cand:
            source_rows = cand

    if not source_rows:
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

        tmp = t[[rank_col, team_col]].copy()
        tmp.columns = ["SoS", "Team"]
        tmp["Team"] = tmp["Team"].astype(str).str.strip()
        tmp["SoS"] = pd.to_numeric(tmp["SoS"], errors="coerce")
        tmp = tmp.dropna(subset=["SoS", "Team"])
        tmp["SoS"] = tmp["SoS"].astype(int)
        source_rows = [(int(a), str(b)) for a, b in zip(tmp["SoS"].tolist(), tmp["Team"].tolist())]

    df = pd.DataFrame(source_rows, columns=["SoS", "Team"]).dropna()
    df["Team"] = df["Team"].astype(str).str.strip()
    df["SoS"] = pd.to_numeric(df["SoS"], errors="coerce")
    df = df.dropna(subset=["SoS", "Team"])
    df["SoS"] = df["SoS"].astype(int)
    df = df[df["SoS"] > 0].copy()

    if len(df) < 330:
        raise SystemExit(f"SoS scrape too small ({len(df)} rows). Page is likely JS-loaded; scraper needs different extraction.")

    team_alias_path = root / "team_alias.csv"
    alias_map, standards_fk = _load_alias_map(team_alias_path)

    matched_rows = []
    unmatched_rows = []

    for _, row in df.iterrows():
        src = str(row["Team"]).strip()
        rk = int(row["SoS"])
        fk = _fuzzy_key(src)

        std = alias_map.get(fk, "")
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


if __name__ == "__main__":
    main()
