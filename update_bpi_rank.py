#!/usr/bin/env python3
import os
import re
import json
from io import StringIO

import requests
import pandas as pd
from bs4 import BeautifulSoup

ESPN_BPI_BASE = "https://www.espn.com/mens-college-basketball/bpi"
TEAM_API_FMT = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espn.com/",
}

ID_RE = re.compile(r"/mens-college-basketball/team/_/id/(\d+)/")


def fetch(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def get_short_team_name(team_id: int, cache: dict[int, str]) -> str:
    if team_id in cache:
        return cache[team_id]

    url = TEAM_API_FMT.format(team_id=team_id)
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()

    team = data.get("team") or {}
    name = (
        team.get("shortDisplayName")
        or team.get("displayName")
        or team.get("name")
        or str(team_id)
    )

    cache[team_id] = name
    return name


def extract_team_ids_in_order(html: str) -> list[int]:
    soup = BeautifulSoup(html, "html.parser")
    ids = []
    seen = set()

    for a in soup.select('a[href*="/mens-college-basketball/team/_/id/"]'):
        href = a.get("href") or ""
        m = ID_RE.search(href)
        if not m:
            continue
        tid = int(m.group(1))
        if tid in seen:
            continue
        seen.add(tid)
        ids.append(tid)

    return ids


def extract_bpi_rank_column(html: str) -> pd.Series:
    tables = pd.read_html(StringIO(html))
    target = None

    def norm(s: str) -> str:
        return re.sub(r"[^a-z]", "", str(s).lower())

    for t in tables:
        cols = [str(c) for c in t.columns]
        if any(norm(c) == "bpirk" for c in cols) or any("BPI RK" in str(c).upper() for c in cols):
            target = t
            break

    if target is None:
        raise RuntimeError("Could not find a table with a BPI RK column on this page.")

    col_name = None
    for c in target.columns:
        if norm(c) == "bpirk" or "BPI RK" in str(c).upper():
            col_name = c
            break

    if col_name is None:
        raise RuntimeError("Found a candidate table, but could not locate the BPI RK column name.")

    ranks = (
        target[col_name]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )

    ranks = pd.to_numeric(ranks, errors="coerce").dropna().astype(int)
    return ranks.reset_index(drop=True)


def page_url(page: int) -> str:
    if page == 1:
        return ESPN_BPI_BASE
    return f"{ESPN_BPI_BASE}/_/view/bpi/page/{page}"


def fetch_all_bpi_pages(max_pages: int = 20) -> pd.DataFrame:
    team_cache: dict[int, str] = {}
    all_rows = []

    for page in range(1, max_pages + 1):
        url = page_url(page)
        html = fetch(url)

        team_ids = extract_team_ids_in_order(html)
        ranks = extract_bpi_rank_column(html)

        if len(ranks) == 0:
            break

        if len(team_ids) < len(ranks):
            raise RuntimeError(
                f"Page {page}: found {len(team_ids)} team ids but {len(ranks)} rank rows. ESPN layout likely changed."
            )

        team_ids = team_ids[: len(ranks)]
        teams = [get_short_team_name(tid, team_cache) for tid in team_ids]

        df_page = pd.DataFrame({"bpi_rank": ranks.values, "team_bpi": teams})
        all_rows.append(df_page)

        if len(ranks) < 50:
            break

    if not all_rows:
        raise RuntimeError("Failed to scrape any BPI pages from ESPN.")

    df = pd.concat(all_rows, ignore_index=True)
    df = df.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)
    return df


def main():
    print("Fetching ESPN BPI Rankings")
    df = fetch_all_bpi_pages()
    os.makedirs("data_raw", exist_ok=True)
    out_path = "data_raw/bpi_rankings.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df)} rows to {out_path}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
