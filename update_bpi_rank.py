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


def _discover_api_candidates(html: str) -> List[str]()_
