from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests


SEASON = 2026
TIMEOUT = 30


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw_dir(root: Path) -> Path:
    d = root / "data_raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _norm(s: str) -> str:
    s = str(s or "").strip().lower()
    s = s.replace("&", " and ")
    s = s.replace("'", "")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bst\.?\b", "st", s)
    s = re.sub(r"\bstate\b", "st", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _load_alias_df(root: Path) -> pd.DataFrame:
    p = root / "team_alias.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing team_alias.csv at {p}")
    df = pd.read_csv(p, dtype=str, keep_default_na=False, na_filter=False)
    for c in ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]:
        if c not in df.columns:
            raise ValueError(f"team_alias.csv missing column: {c}")
    return df


def _build_lookup(alias_df: pd.DataFrame) -> Dict[str, str]:
    lookup: Dict[str, str] = {}

    def add(v: str, standard: str) -> None:
        k = _norm(v)
        if k and k not in lookup:
            lookup[k] = standard

    for _, r in alias_df.iterrows():
        standard = str(r.get("standard_name", "")).strip()
        if not standard:
            continue
        add(standard, standard)
        add(r.get("bpi_name", ""), standard)
        add(r.get("net_name", ""), standard)
        add(r.get("kenpom_name", ""), standard)
        add(r.get("game_log_name", ""), standard)

    return lookup


@dataclass
class MatchResult:
    standard: str
    score: float


def _suggest(source: str, standards: List[str]) -> MatchResult:
    s0 = _norm(source)
    best = MatchResult("", 0.0)
    for st in standards:
        sc = SequenceMatcher(None, s0, _norm(st)).ratio()
        if sc > best.score:
            best = MatchResult(st, sc)
    return best


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


def _fetch_html(season: int) -> str:
    url = f"https://www.espn.com/mens-college-basketball/bpi/_/view/overview/season/{season}/group/50"
    r = _session().get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _pick_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        score = 0
        if any("team" in c for c in cols):
            score += 3
        if any(c == "bpi" or c.endswith("bpi") or "bpi" in c for c in cols):
            score += 3
        if any("rk" in c or c in {"rnk", "rank"} for c in cols):
            score += 1
        if score > best_score:
            best = t
            best_score = score
    if best is None:
        raise RuntimeError("Could not find a BPI table on the ESPN page.")
    return best


def _clean_team_cell(x: str) -> str:
    s = str(x or "").strip()
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    s = re.sub(r"\s+\d+\s*-\s*\d+\s*$", "", s)
    s = re.sub(r"\s+\d+\s*-\s*\d+\s*\([^)]*\)\s*$", "", s)
    return s.strip()


def _extract_rows(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    t = _pick_table(tables)

    cols = {str(c).strip().lower(): c for c in t.columns}
    team_col = None
    for k in ["team", "school", "name"]:
        if k in cols:
            team_col = cols[k]
            break
    if team_col is None:
        for c in t.columns:
            if "team" in str(c).lower():
                team_col = c
                break
    if team_col is None:
        team_col = t.columns[0]

    out = pd.DataFrame()
    out["TeamSrc"] = t[team_col].astype(str).map(_clean_team_cell)

    rk_col = None
    for k in ["rk", "rnk", "rank"]:
        if k in cols:
            rk_col = cols[k]
            break
    if rk_col is not None:
        out["BPI_Rank"] = pd.to_numeric(t[rk_col], errors="coerce")
    else:
        out["BPI_Rank"] = range(1, len(out) + 1)

    out = out.dropna(subset=["TeamSrc"]).copy()
    out["TeamSrc"] = out["TeamSrc"].astype(str).str.strip()
    out = out[out["TeamSrc"] != ""].copy()
    out["BPI_Rank"] = pd.to_numeric(out["BPI_Rank"], errors="coerce")
    out = out.dropna(subset=["BPI_Rank"]).copy()
    out["BPI_Rank"] = out["BPI_Rank"].astype(int)

    return out[["TeamSrc", "BPI_Rank"]]


def main() -> None:
    root = _root()
    data_raw = _data_raw_dir(root)

    alias_df = _load_alias_df(root)
    lookup = _build_lookup(alias_df)
    standards = alias_df["standard_name"].astype(str).tolist()

    html = _fetch_html(SEASON)
    raw = _extract_rows(html)

    snapshot_date = dt.date.today().isoformat()

    matched_rows: List[Tuple[str, str, int]] = []
    unmatched_rows: List[Tuple[str, str, float]] = []

    for _, r in raw.iterrows():
        src = str(r["TeamSrc"]).strip()
        rk = int(r["BPI_Rank"])
        standard = lookup.get(_norm(src), "")
        if not standard:
            sug = _suggest(src, standards)
            unmatched_rows.append((src, sug.standard, float(sug.score)))
            continue
        matched_rows.append((snapshot_date, standard, rk))

    out_path = data_raw / "BPI_Rank.csv"
    pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "BPI_Rank"]).to_csv(out_path, index=False)

    unmatched_path = data_raw / "unmatched_bpi_teams.csv"
    pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"]).to_csv(
        unmatched_path, index=False
    )

    print(out_path.name)
    print(unmatched_path.name)
    print(f"ESPN rows scraped: {len(raw)}")
    print(f"Matched: {len(matched_rows)}")
    print(f"Unmatched: {len(unmatched_rows)}")


if __name__ == "__main__":
    main()
