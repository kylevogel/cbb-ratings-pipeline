from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


SEASON = 2026
GROUP = 50
TIMEOUT = 30


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw_dir(root: Path) -> Path:
    d = root / "data_raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"\bstate\b", "st", s)
    s = re.sub(r"\bst\.\b", "st", s)
    s = re.sub(r"\bsaint\b", "st", s)
    s = s.replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _load_alias_df(root: Path) -> pd.DataFrame:
    p = root / "team_alias.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing team_alias.csv at {p}")
    df = pd.read_csv(p, dtype=str).fillna("")
    needed = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"team_alias.csv missing column: {c}")
    return df


def _build_lookup(alias_df: pd.DataFrame) -> Dict[str, str]:
    lookup: Dict[str, str] = {}

    def add(a: str, standard: str) -> None:
        a2 = _norm(a)
        if a2 and a2 not in lookup:
            lookup[a2] = standard

    for _, r in alias_df.iterrows():
        standard = str(r["standard_name"]).strip()
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


def _get(url: str) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r


def _extract_team_names_from_json(obj: Any) -> List[str]:
    out: List[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            team = x.get("team")
            if isinstance(team, dict):
                name = team.get("displayName") or team.get("shortDisplayName") or team.get("name")
                if isinstance(name, str) and name.strip():
                    out.append(name.strip())
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)

    seen = set()
    deduped = []
    for t in out:
        k = _norm(t)
        if k and k not in seen:
            seen.add(k)
            deduped.append(t)
    return deduped


def _try_endpoints(season: int, group: int) -> List[str]:
    candidates = [
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?season={season}&group={group}&limit=500",
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/powerindex?season={season}&group={group}&limit=500",
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/bpi?season={season}&group={group}",
    ]
    for url in candidates:
        try:
            r = _get(url)
            if "application/json" not in r.headers.get("Content-Type", "") and not r.text.strip().startswith("{"):
                continue
            data = r.json()
            teams = _extract_team_names_from_json(data)
            if len(teams) >= 300:
                return teams
        except Exception:
            continue
    return []


def _extract_team_names_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    names: List[str] = []
    for a in soup.select('a[href*="/mens-college-basketball/team/"]'):
        t = a.get_text(" ", strip=True)
        if t:
            names.append(t)
    seen = set()
    out = []
    for n in names:
        k = _norm(n)
        if k and k not in seen:
            seen.add(k)
            out.append(n)
    return out


def _try_paged_html(season: int, group: int) -> List[str]:
    base = f"https://www.espn.com/mens-college-basketball/bpi/_/view/overview/season/{season}/group/{group}"
    patterns = [
        lambda i: f"{base}/page/{i}",
        lambda i: f"{base}?page={i}",
        lambda i: f"{base}?offset={(i-1)*50}",
    ]
    all_names: List[str] = []
    seen = set()

    for pattern in patterns:
        all_names = []
        seen = set()
        for i in range(1, 30):
            try:
                r = _get(pattern(i))
                page_names = _extract_team_names_from_html(r.text)
                before = len(seen)
                for n in page_names:
                    k = _norm(n)
                    if k and k not in seen:
                        seen.add(k)
                        all_names.append(n)
                if len(seen) == before:
                    break
            except Exception:
                break
        if len(all_names) >= 300:
            return all_names

    try:
        r = _get(base)
        names = _extract_team_names_from_html(r.text)
        return names
    except Exception:
        return []


def _fetch_bpi_team_order(season: int, group: int) -> List[str]:
    teams = _try_endpoints(season, group)
    if teams:
        return teams
    teams = _try_paged_html(season, group)
    return teams


def main() -> None:
    root = _root()
    data_raw = _data_raw_dir(root)

    alias_df = _load_alias_df(root)
    lookup = _build_lookup(alias_df)
    standards = alias_df["standard_name"].astype(str).tolist()

    source_teams = _fetch_bpi_team_order(SEASON, GROUP)

    snapshot_date = dt.date.today().isoformat()

    matched_rows: List[Tuple[str, str, int]] = []
    unmatched_rows: List[Tuple[str, str, float]] = []

    for idx, src in enumerate(source_teams, start=1):
        key = _norm(src)
        standard = lookup.get(key, "")
        if not standard:
            sug = _suggest(src, standards)
            unmatched_rows.append((src, sug.standard, float(sug.score)))
            continue
        matched_rows.append((snapshot_date, standard, idx))

    out_path = data_raw / "BPI_Rank.csv"
    pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "BPI"]).to_csv(out_path, index=False)

    unmatched_path = data_raw / "unmatched_bpi_teams.csv"
    pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"]).to_csv(
        unmatched_path, index=False
    )

    print(out_path.name)
    print(unmatched_path.name)
    print(f"Pulled source teams: {len(source_teams)}")
    print(f"Matched: {len(matched_rows)}")
    print(f"Unmatched: {len(unmatched_rows)}")


if __name__ == "__main__":
    main()
