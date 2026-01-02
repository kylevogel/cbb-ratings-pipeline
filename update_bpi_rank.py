from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests


SEASON = 2026
GROUP = 50


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
        (" university", ""),
        (" college", ""),
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


@dataclass
class MatchResult:
    standard: str
    score: float


def _suggest(source: str, standards: List[str]) -> MatchResult:
    s0 = _norm(source)
    if not s0:
        return MatchResult("", 0.0)
    best = ("", 0.0)
    for std in standards:
        sc = SequenceMatcher(None, s0, _norm(std)).ratio()
        if sc > best[1]:
            best = (std, sc)
    return MatchResult(best[0], float(best[1]))


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _extract_rows(obj: Any) -> List[Tuple[str, Optional[int], Optional[float]]]:
    out: List[Tuple[str, Optional[int], Optional[float]]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            team_name: Optional[str] = None
            rank: Optional[int] = None
            rating: Optional[float] = None

            if "team" in x and isinstance(x["team"], dict):
                td = x["team"]
                team_name = td.get("displayName") or td.get("shortDisplayName") or td.get("name")

            if team_name is None and "name" in x and isinstance(x["name"], str):
                team_name = x["name"]

            for rk_key in ["rank", "bpiRank", "bpi_rank", "ranking", "currentRank"]:
                if rk_key in x:
                    try:
                        rank = int(x[rk_key])
                        break
                    except Exception:
                        pass

            for rt_key in ["bpi", "rating", "bpiValue", "bpi_value"]:
                if rt_key in x:
                    try:
                        rating = float(x[rt_key])
                        break
                    except Exception:
                        pass

            if team_name and rank is not None:
                out.append((str(team_name), rank, rating))

            for v in x.values():
                walk(v)

        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    seen = set()
    dedup: List[Tuple[str, Optional[int], Optional[float]]] = []
    for t, r, b in out:
        k = (t, r)
        if k not in seen:
            seen.add(k)
            dedup.append((t, r, b))
    return dedup


def _paged_collect_json(s: requests.Session, base: str) -> List[Tuple[str, Optional[int], Optional[float]]]:
    best: List[Tuple[str, Optional[int], Optional[float]]] = []
    for page in range(1, 40):
        params = {"season": SEASON, "group": GROUP, "page": page}
        try:
            r = s.get(base, params=params, timeout=25)
        except Exception:
            break
        if r.status_code != 200:
            break
        try:
            obj = r.json()
        except Exception:
            break
        rows = _extract_rows(obj)
        if len(rows) > len(best):
            best = rows
        if not rows:
            break
        if page > 2 and len(rows) == len(best) and len(rows) < 50:
            break
        if len(best) >= 360:
            break
    return best


def _discover_api_candidates(html: str) -> List[str]:
    html = (html or "").replace("\\u002F", "/")
    urls = set(re.findall(r"https?://[^\s\"'<>]+", html))
    rels = set(re.findall(r"/apis/[A-Za-z0-9_\-\/\.\?\=\&]+", html))
    out = []
    for u in urls:
        if "espn.com" in u and ("bpi" in u.lower() or "powerindex" in u.lower()) and "apis" in u.lower():
            out.append(u.split("#")[0])
    for u in rels:
        ul = u.lower()
        if ("bpi" in ul or "powerindex" in ul) and "apis" in ul:
            out.append("https://site.web.api.espn.com" + u.split("#")[0])
    return list(dict.fromkeys(out))


def _fetch_bpi_rows() -> List[Tuple[str, Optional[int], Optional[float]]]:
    s = _session()
    page_url = f"https://www.espn.com/mens-college-basketball/bpi/_/view/overview/season/{SEASON}/group/{GROUP}"
    html = ""
    try:
        r = s.get(page_url, timeout=25, headers={"Referer": "https://www.espn.com/"})
        if r.status_code == 200:
            html = r.text or ""
    except Exception:
        html = ""

    candidates = _discover_api_candidates(html)
    candidates.extend(
        [
            "https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi",
            "https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/powerindex",
            "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/bpi",
        ]
    )

    best: List[Tuple[str, Optional[int], Optional[float]]] = []
    seen = set()
    cands = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            cands.append(c)

    for base in cands:
        rows = _paged_collect_json(s, base)
        if len(rows) > len(best):
            best = rows
        if len(best) >= 360:
            break

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

    source_rows = _fetch_bpi_rows()
    source_rows = [(t, r, b) for (t, r, b) in source_rows if t and r is not None]

    matched_rows: List[Tuple[str, int, Optional[float]]] = []
    unmatched_rows: List[Tuple[str, str, float]] = []

    for team, rank, rating in source_rows:
        std = _auto_match(team, alias_map, standard_variants)
        if std:
            matched_rows.append((std, int(rank), rating))
        else:
            sug = _suggest(team, standards)
            unmatched_rows.append((team, sug.standard, sug.score))

    df = pd.DataFrame(matched_rows, columns=["Team", "BPI_Rank", "BPI"])
    df = df.sort_values("BPI_Rank", ascending=True).drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    df.insert(0, "snapshot_date", snapshot_date)

    out_path = data_raw / "BPI_Rank.csv"
    df.to_csv(out_path, index=False)

    unmatched_path = data_raw / "unmatched_bpi_teams.csv"
    pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"]).to_csv(
        unmatched_path, index=False
    )

    print(out_path.name)
    print(unmatched_path.name)
    print(f"Pulled source teams: {len(source_rows)}")
    print(f"Matched: {len(df)}")
    print(f"Unmatched: {len(unmatched_rows)}")


if __name__ == "__main__":
    main()
