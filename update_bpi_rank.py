from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


SEASON = 2026
MAX_PAGES = 12
GROUP = 50


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.espn.com/",
            "Connection": "keep-alive",
        }
    )
    return s


def _norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 &.'()-]+", "", s)
    return s


def _canon(s: str) -> str:
    return _norm(s)


def _fuzzy_key(s: str) -> str:
    s = _norm(s)
    s = s.replace("&", "and")
    s = re.sub(r"[.'()\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _load_alias_df(root: Path) -> pd.DataFrame:
    p = root / "team_alias.csv"
    if not p.exists():
        return pd.DataFrame(columns=["standard_name"])
    df = pd.read_csv(p)
    return df


def _build_lookup(alias_df: pd.DataFrame) -> Dict[str, str]:
    lookup: Dict[str, str] = {}

    def add(name: str, standard: str) -> None:
        k = _norm(str(name or ""))
        if k:
            lookup[k] = standard

    for _, r in alias_df.iterrows():
        standard = str(r.get("standard_name", "")).strip()
        if not standard:
            continue
        add(standard, standard)
        add(r.get("kenpom_name", ""), standard)
        add(r.get("bpi_name", ""), standard)
        add(r.get("net_name", ""), standard)
        add(r.get("game_log_name", ""), standard)

    return lookup


@dataclass
class MatchResult:
    standard: str
    score: float


def _suggest(source: str, standards: List[str]) -> MatchResult:
    s0 = _norm(source)
    best = ("", 0.0)
    for st in standards:
        sc = SequenceMatcher(None, s0, _norm(st)).ratio()
        if sc > best[1]:
            best = (st, sc)
    return MatchResult(standard=best[0], score=best[1])


def _extract_team_names_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    names: List[str] = []
    for a in soup.select('a[href*="/mens-college-basketball/team/"]'):
        t = a.get_text(" ", strip=True)
        if not t:
            continue
        if len(t) > 45:
            continue
        names.append(t)

    seen = set()
    out: List[str] = []
    for n in names:
        k = _fuzzy_key(n)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(n)
    return out


def _page_url(page: int) -> str:
    base = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi"
    if page <= 1:
        return base
    return f"{base}/page/{page}"


def _fetch_bpi_team_order() -> List[str]:
    s = _session()
    all_names: List[str] = []
    seen = set()

    for page in range(1, MAX_PAGES + 1):
        url = _page_url(page)
        r = s.get(url, timeout=25)
        if r.status_code != 200 or not r.text:
            break
        page_names = _extract_team_names_from_html(r.text)

        new = []
        for n in page_names:
            k = _fuzzy_key(n)
            if k and k not in seen:
                seen.add(k)
                new.append(n)

        if len(new) == 0:
            break

        all_names.extend(new)

        if len(all_names) >= 360:
            break

    return all_names


def main() -> None:
    root = _root()
    data_raw = _data_raw(root)

    alias_df = _load_alias_df(root)
    lookup = _build_lookup(alias_df)

    standards = []
    if not alias_df.empty and "standard_name" in alias_df.columns:
        standards = [str(x).strip() for x in alias_df["standard_name"].tolist() if str(x).strip()]
    standard_keys = {_norm(s): s for s in standards}

    source_teams = _fetch_bpi_team_order()

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()

    matched_rows: List[Tuple[str, str, int]] = []
    unmatched_rows: List[Tuple[str, str, float]] = []

    for idx, src in enumerate(source_teams, start=1):
        raw = str(src or "").strip()
        if not raw:
            continue

        k = _norm(raw)
        standard = lookup.get(k)

        if not standard:
            fk = _fuzzy_key(raw)
            if fk in standard_keys:
                standard = standard_keys[fk]

        if not standard:
            sug = _suggest(raw, standards)
            unmatched_rows.append((raw, sug.standard, float(sug.score)))
            continue

        matched_rows.append((snapshot_date, standard, idx))

    out_path = data_raw / "BPI_Rank.csv"
    pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "BPI_Rank"]).to_csv(out_path, index=False)

    unmatched_path = data_raw / "unmatched_bpi_teams.csv"
    pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"]).to_csv(
        unmatched_path, index=False
    )

    print(out_path.name)
    print(unmatched_path.name)
    print(f"Pulled source teams: {len(source_teams)}")
    print(f"Matched: {len(matched_rows)}")
    print(f"Unmatched: {len(unmatched_rows)}")

    if len(matched_rows) < 300:
        raise SystemExit(f"BPI scrape too small ({len(matched_rows)} matched). ESPN pagination/layout may have changed.")


if __name__ == "__main__":
    main()
