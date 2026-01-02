from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


SEASON = 2026
GROUP = 50


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw_dir(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _load_alias_df(root: Path) -> pd.DataFrame:
    for p in [root / "team_alias.csv", root / "data_raw" / "team_alias.csv"]:
        if p.exists():
            df = pd.read_csv(p, dtype=str).fillna("")
            needed = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
            for c in needed:
                if c not in df.columns:
                    raise ValueError(f"team_alias.csv missing column: {c}")
            return df
    raise FileNotFoundError("team_alias.csv not found in project root or data_raw/")


def _build_lookup(alias_df: pd.DataFrame) -> Dict[str, str]:
    lookup: Dict[str, str] = {}

    def add(a: str, standard: str) -> None:
        a2 = _norm(a)
        if a2 and a2 not in lookup:
            lookup[a2] = standard

    for _, r in alias_df.iterrows():
        standard = str(r.get("standard_name", "")).strip()
        if not standard:
            continue
        add(standard, standard)
        for col in ["kenpom_name", "bpi_name", "net_name", "game_log_name"]:
            v = str(r.get(col, "")).strip()
            if v:
                add(v, standard)
    return lookup


def _canon(name: str, lookup: Dict[str, str]) -> str:
    k = _norm(name)
    return lookup.get(k, str(name).strip())


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return s


def _is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def _extract_urls_from_html(html: str) -> List[str]:
    urls = set(re.findall(r"https?://[^\s\"'<>]+", html))
    out = []
    for u in urls:
        if "espn" not in u:
            continue
        if "/apis/" not in u:
            continue
        if "bpi" in u.lower() or "powerindex" in u.lower():
            out.append(u)
    return sorted(set(out))


def _safe_json_loads(blob: str) -> Optional[Any]:
    try:
        return json.loads(blob)
    except Exception:
        return None


def _extract_embedded_json(html: str) -> List[Any]:
    out: List[Any] = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script"):
        if not tag.string:
            continue
        t = tag.string.strip()
        if not t:
            continue
        if "__espnfitt__" in t:
            m = re.search(r"__espnfitt__\s*=\s*({.*?})\s*;?\s*$", t, flags=re.DOTALL)
            if m:
                j = _safe_json_loads(m.group(1))
                if j is not None:
                    out.append(j)
        if "application/json" in (tag.get("type") or ""):
            j = _safe_json_loads(t)
            if j is not None:
                out.append(j)
    return out


def _walk(obj: Any) -> Iterable[Any]:
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


def _discover_api_urls(html: str) -> List[str]:
    urls = set(_extract_urls_from_html(html))
    for j in _extract_embedded_json(html):
        for x in _walk(j):
            if isinstance(x, str) and _is_url(x):
                xl = x.lower()
                if "espn" in xl and "/apis/" in xl and ("bpi" in xl or "powerindex" in xl):
                    urls.add(x)
    return sorted(urls)


def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _coerce_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _pick_team_name(t: Any) -> Optional[str]:
    if isinstance(t, dict):
        for k in ["displayName", "shortDisplayName", "name", "abbreviation"]:
            v = t.get(k)
            if v:
                return str(v).strip()
    if isinstance(t, str) and t.strip():
        return t.strip()
    return None


def _pick_stat(stats: Any, keys: List[str]) -> Optional[float]:
    if isinstance(stats, dict):
        for k in keys:
            if k in stats:
                v = _coerce_float(stats.get(k))
                if v is not None:
                    return v
    if isinstance(stats, list):
        for s in stats:
            if not isinstance(s, dict):
                continue
            n = str(s.get("name", "")).strip().lower()
            a = str(s.get("abbreviation", "")).strip().lower()
            lab = str(s.get("label", "")).strip().lower()
            if any(k in n for k in keys) or any(k in a for k in keys) or any(k in lab for k in keys):
                v = _coerce_float(s.get("value") or s.get("displayValue") or s.get("rawValue"))
                if v is not None:
                    return v
    return None


def _parse_bpi_payload(data: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def add(team: str, rank: Optional[int], rating: Optional[float]) -> None:
        if not team:
            return
        out.append({"team": team, "rank": rank, "rating": rating})

    if isinstance(data, dict) and isinstance(data.get("items"), list):
        for it in data["items"]:
            if not isinstance(it, dict):
                continue
            team = _pick_team_name(it.get("team") or it.get("athlete") or it.get("franchise") or it.get("club"))
            if not team:
                team = _pick_team_name(it.get("name"))
            rank = _coerce_int(it.get("rank") or it.get("currentRank") or it.get("seed"))
            rating = _pick_stat(it.get("stats") or it.get("values") or it.get("metrics"), ["bpi", "power", "rating"])
            add(team or "", rank, rating)

    if isinstance(data, dict) and isinstance(data.get("rankings"), list):
        for r in data["rankings"]:
            if not isinstance(r, dict):
                continue
            ranks = r.get("ranks") or r.get("items") or r.get("entries")
            if not isinstance(ranks, list):
                continue
            for it in ranks:
                if not isinstance(it, dict):
                    continue
                team = _pick_team_name(it.get("team"))
                rank = _coerce_int(it.get("rank") or it.get("currentRank"))
                rating = _pick_stat(it.get("stats") or it.get("values"), ["bpi", "power", "rating"])
                add(team or "", rank, rating)

    if not out:
        for x in _walk(data):
            if not isinstance(x, dict):
                continue
            if "team" in x and any(k in x for k in ["rank", "currentRank", "stats", "values", "metrics"]):
                team = _pick_team_name(x.get("team"))
                rank = _coerce_int(x.get("rank") or x.get("currentRank"))
                rating = _pick_stat(x.get("stats") or x.get("values") or x.get("metrics"), ["bpi", "power", "rating"])
                if team:
                    add(team, rank, rating)

    dedup: Dict[str, Dict[str, Any]] = {}
    for r in out:
        k = _norm(r["team"])
        if not k:
            continue
        if k not in dedup:
            dedup[k] = r
        else:
            if dedup[k].get("rank") is None and r.get("rank") is not None:
                dedup[k]["rank"] = r["rank"]
            if dedup[k].get("rating") is None and r.get("rating") is not None:
                dedup[k]["rating"] = r["rating"]
    return list(dedup.values())


def _finalize_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["Team", "BPI"])
    if "rank" not in df.columns:
        df["rank"] = None
    if df["rank"].isna().all():
        if "rating" in df.columns and df["rating"].notna().any():
            df = df.sort_values("rating", ascending=False, na_position="last")
            df["rank"] = range(1, len(df) + 1)
        else:
            df["rank"] = range(1, len(df) + 1)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df = df.dropna(subset=["rank"])
    df["rank"] = df["rank"].astype(int)
    df = df.sort_values("rank")
    return df.rename(columns={"team": "Team", "rank": "BPI"})[["Team", "BPI"]].reset_index(drop=True)


def _fetch_bpi_rows(season: int, group: int) -> List[Dict[str, Any]]:
    s = _session()

    page_urls = [
        f"https://www.espn.com/mens-college-basketball/bpi/_/view/overview/season/{season}/group/{group}",
        f"https://www.espn.com/mens-college-basketball/bpi/_/view/resume/season/{season}/group/{group}",
        f"https://www.espn.com/mens-college-basketball/bpi/_/view/tournament/season/{season}/group/{group}",
        f"https://www.espn.com/mens-college-basketball/bpi/_/view/overview/group/{group}",
    ]

    html = ""
    for u in page_urls:
        try:
            r = s.get(u, timeout=20)
            if r.status_code == 200 and r.text:
                html = r.text
                break
        except Exception:
            continue

    api_urls: List[str] = []
    if html:
        api_urls.extend(_discover_api_urls(html))

    guesses = [
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/bpi?season={season}&group={group}",
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?season={season}&group={group}&limit=500",
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/powerindex?season={season}&group={group}&limit=500",
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/powerindex?season={season}&group={group}&limit=500&sort=bpi:desc",
        f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?season={season}&group={group}&limit=500&sort=bpi:desc",
    ]

    tried = set()
    for u in api_urls + guesses:
        if u in tried:
            continue
        tried.add(u)
        try:
            r = s.get(u, timeout=20)
            if r.status_code != 200:
                continue
            data = r.json()
            rows = _parse_bpi_payload(data)
            if len(rows) >= 300:
                return rows
        except Exception:
            continue

    if html:
        try:
            tables = pd.read_html(html)
            best = None
            for t in tables:
                cols = [str(c).strip().lower() for c in t.columns]
                if any("team" in c for c in cols) and any("bpi" in c for c in cols):
                    best = t
                    break
            if best is not None:
                team_col = None
                bpi_col = None
                for c in best.columns:
                    cl = str(c).strip().lower()
                    if "team" in cl:
                        team_col = c
                    if cl == "bpi" or "bpi" in cl:
                        bpi_col = c
                if team_col is not None and bpi_col is not None:
                    out = []
                    for _, r in best.iterrows():
                        team = str(r.get(team_col, "")).strip()
                        rank = _coerce_int(r.get(bpi_col))
                        if team and rank is not None:
                            out.append({"team": team, "rank": rank, "rating": None})
                    if len(out) >= 300:
                        return out
        except Exception:
            pass

    return []


@dataclass
class Suggestion:
    incoming: str
    best_standard: str
    ratio: float


def _suggest(incoming: str, standards: List[str]) -> Optional[Suggestion]:
    inc = str(incoming).strip()
    if not inc:
        return None
    inc_n = _norm(inc)
    best = ("", 0.0)
    for s in standards:
        r = SequenceMatcher(a=inc_n, b=_norm(s)).ratio()
        if r > best[1]:
            best = (s, r)
    if best[0]:
        return Suggestion(incoming=inc, best_standard=best[0], ratio=float(best[1]))
    return None


def main() -> None:
    root = _root()
    data_raw = _data_raw_dir(root)

    alias_df = _load_alias_df(root)
    lookup = _build_lookup(alias_df)
    standards = alias_df["standard_name"].astype(str).tolist()

    rows = _fetch_bpi_rows(SEASON, GROUP)
    df = _finalize_rows(rows)

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    df.insert(0, "snapshot_date", snapshot_date)

    df["Team"] = df["Team"].astype(str).map(lambda x: _canon(x, lookup))

    out_path = data_raw / "BPI_Rank.csv"
    df.to_csv(out_path, index=False)

    unmatched = []
    known = set(_norm(s) for s in standards)
    for t in df["Team"].astype(str).tolist():
        if _norm(t) not in known:
            s = _suggest(t, standards)
            if s is not None:
                unmatched.append({"incoming_team": s.incoming, "suggested_standard": s.best_standard, "similarity": s.ratio})

    um_path = data_raw / "unmatched_bpi_teams.csv"
    pd.DataFrame(unmatched).sort_values(["similarity", "incoming_team"], ascending=[False, True]).to_csv(um_path, index=False)

    if len(df) < 300:
        raise SystemExit(f"BPI scrape returned only {len(df)} teams; expected ~364. See {out_path}.")


if __name__ == "__main__":
    main()
