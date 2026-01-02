from __future__ import annotations

import datetime as dt
import re
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


SEASON = 2026
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[\.\,\(\)'\u2019\u2018\u2013\u2014\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _norm(s))


def _variants(s: str) -> List[str]:
    n = _norm(s)
    outs = {n}

    repls = [
        (" state", " st"),
        (" st", " state"),
        ("mount ", "mt "),
        ("mt ", "mount "),
        ("saint ", "st "),
        ("st ", "saint "),
        (" maryland", " md"),
        (" md", " maryland"),
        ("  ", " "),
    ]

    for a, b in repls:
        outs.add(n.replace(a, b).strip())

    outs2 = set()
    for x in outs:
        outs2.add(_key(x))
    return sorted({v for v in outs2 if v})


def _read_alias(root: Path) -> pd.DataFrame:
    path = root / "team_alias.csv"
    if not path.exists():
        raise SystemExit("team_alias.csv not found at repo root.")
    return pd.read_csv(path).fillna("")


def _build_alias_map(alias_df: pd.DataFrame) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for _, r in alias_df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        for col in ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]:
            v = str(r.get(col, "")).strip()
            if v:
                m[_key(v)] = std
    return m


def _standards(alias_df: pd.DataFrame) -> List[str]:
    s = [str(x).strip() for x in alias_df.get("standard_name", pd.Series([], dtype=str)).tolist()]
    return [x for x in s if x]


def _suggest(source: str, standards: List[str]) -> Tuple[str, float]:
    s0 = _norm(source)
    if not s0:
        return "", 0.0
    best = ("", 0.0)
    for std in standards:
        sc = SequenceMatcher(None, s0, _norm(std)).ratio()
        if sc > best[1]:
            best = (std, sc)
    return best[0], float(best[1])


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _find_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        if any("rank" == c or c.startswith("rank") for c in cols):
            score += 1
        if any("team" in c for c in cols):
            score += 1
        if score > best_score and len(t) > 50:
            best = t
            best_score = score
    if best is None:
        raise SystemExit("Could not find SoS table on Warren Nolan page.")
    return best


def _auto_match(source_team: str, alias_map: Dict[str, str], standard_variants: List[Tuple[str, str]]) -> Optional[str]:
    fk = _key(source_team)
    if not fk:
        return None
    if fk in alias_map:
        return alias_map[fk]

    best_std: Optional[str] = None
    best_len = 0

    for vkey, std in standard_variants:
        if not vkey:
            continue
        if fk == vkey:
            return std
        if fk.startswith(vkey) or vkey in fk:
            if len(vkey) > best_len:
                best_len = len(vkey)
                best_std = std

    return best_std


def main() -> None:
    root = _root()
    data_raw = _data_raw(root)

    alias_df = _read_alias(root)
    alias_map = _build_alias_map(alias_df)
    standards = _standards(alias_df)

    standard_variants: List[Tuple[str, str]] = []
    for std in standards:
        for v in _variants(std):
            standard_variants.append((v, std))
    standard_variants = sorted(set(standard_variants), key=lambda x: (-len(x[0]), x[1]))

    s = _session()
    r = s.get(URL, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"Failed to fetch SoS page: HTTP {r.status_code}")
    html = r.text or ""

    t = _find_table(html)
    cols_map = {str(c).strip().lower(): c for c in t.columns}

    team_col = None
    for k, orig in cols_map.items():
        if "team" in k:
            team_col = orig
            break
    rank_col = None
    for k, orig in cols_map.items():
        if k == "rank" or k.startswith("rank"):
            rank_col = orig
            break

    if team_col is None or rank_col is None:
        raise SystemExit("SoS table missing required Team/Rank columns.")

    df0 = t[[rank_col, team_col]].copy()
    df0.columns = ["Rank", "Team_raw"]
    df0["Rank"] = pd.to_numeric(df0["Rank"], errors="coerce")
    df0 = df0.dropna(subset=["Rank", "Team_raw"]).copy()
    df0["Rank"] = df0["Rank"].astype(int)
    df0["Team_raw"] = df0["Team_raw"].astype(str).str.strip()
    df0 = df0[df0["Team_raw"] != ""].copy()

    matched_rows: List[Tuple[str, int]] = []
    unmatched_rows: List[Tuple[str, str, float]] = []

    for _, row in df0.iterrows():
        src = row["Team_raw"]
        rk = int(row["Rank"])
        std = _auto_match(src, alias_map, standard_variants)
        if std:
            matched_rows.append((std, rk))
        else:
            sug, sc = _suggest(src, standards)
            unmatched_rows.append((src, sug, sc))

    out = pd.DataFrame(matched_rows, columns=["Team", "SOS_Rank"])
    out = out.sort_values("SOS_Rank", ascending=True).drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    out.insert(0, "snapshot_date", snapshot_date)

    out_path = data_raw / "SOS_Rank.csv"
    out.to_csv(out_path, index=False)

    unmatched_path = data_raw / "unmatched_sos_teams.csv"
    pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"]).to_csv(
        unmatched_path, index=False
    )

    print(out_path.name)
    print(unmatched_path.name)
    print(f"Pulled source teams: {len(df0)}")
    print(f"Matched: {len(out)}")
    print(f"Unmatched: {len(unmatched_rows)}")


if __name__ == "__main__":
    main()
