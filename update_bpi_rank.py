"""
Fetch ESPN BPI ranks and write:
  data_raw/bpi_rankings.csv  with columns: bpi_rank, team_bpi

ESPN's BPI page shows team names in a separate Team/Conf list and
the "POWER INDEX PROJECTIONS" table (with BPI RK) without team names.
This script pairs the team list (by ESPN team id order) with the BPI RK
rows by row.
"""

import os
import time
import re
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = "data_raw/bpi_rankings.csv"
BASE_URL = "https://www.espn.com/mens-college-basketball/bpi"
PAGE_URL = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{}"
TEAM_API = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

TEAM_ID_RE = re.compile(r"/mens-college-basketball/team/_/id/(\d+)/")


def _get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def _extract_team_ids_in_order(html: str) -> list[int]:
    """
    Extract ESPN team ids from the Team/Conf list.
    We take IDs in DOM order, de-dup preserving order.
    """
    soup = BeautifulSoup(html, "html.parser")
    ids = []
    seen = set()

    for a in soup.find_all("a", href=True):
        m = TEAM_ID_RE.search(a["href"])
        if not m:
            continue
        tid = int(m.group(1))
        if tid in seen:
            continue
        seen.add(tid)
        ids.append(tid)

    return ids


def _team_short_name(team_id: int, cache: dict[int, str]) -> str:
    if team_id in cache:
        return cache[team_id]

    url = TEAM_API.format(team_id)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    team = data.get("team") or {}
    name = team.get("displayName") or team.get("shortDisplayName") or team.get("name") or str(team_id)

    cache[team_id] = name
    return name


def _find_projections_table_with_bpi_rk(html: str) -> pd.DataFrame:
    """
    Find the POWER INDEX PROJECTIONS table via read_html and return the table
    that contains a "BPI RK" column.
    """
    tables = pd.read_html(StringIO(html))
    for t in tables:
        cols = [str(c).strip().upper() for c in t.columns]
        if any("BPI RK" == c or c.endswith("BPI RK") or "BPI RK" in c for c in cols):
            return t
    raise RuntimeError("Could not locate projections table containing 'BPI RK'.")


def _extract_bpi_ranks(html: str) -> pd.Series:
    t = _find_projections_table_with_bpi_rk(html)

    bpi_rk_col = None
    for c in t.columns:
        if "BPI RK" in str(c).upper():
            bpi_rk_col = c
            break
    if bpi_rk_col is None:
        raise RuntimeError("Found BPI table but could not identify the BPI RK column.")

    ranks = pd.to_numeric(t[bpi_rk_col], errors="coerce").dropna().astype(int).reset_index(drop=True)
    return ranks


def _page_url(page: int) -> str:
    return BASE_URL if page == 1 else PAGE_URL.format(page)


def fetch_all_bpi(max_pages: int = 25, sleep_s: float = 0.25) -> pd.DataFrame:
    cache: dict[int, str] = {}
    all_rows: list[pd.DataFrame] = []
    seen_ranks = set()

    for page in range(1, max_pages + 1):
        html = _get(_page_url(page))

        ranks = _extract_bpi_ranks(html)
        if len(ranks) == 0:
            break

        team_ids = _extract_team_ids_in_order(html)

        if len(team_ids) < len(ranks):
            raise RuntimeError(
                f"Page {page}: only found {len(team_ids)} team ids but {len(ranks)} BPI ranks. ESPN layout changed."
            )

        team_ids = team_ids[: len(ranks)]
        team_names = [_team_short_name(tid, cache) for tid in team_ids]

        df_page = pd.DataFrame({"bpi_rank": ranks.values, "team_bpi": team_names})

        df_page = df_page[~df_page["bpi_rank"].isin(seen_ranks)]
        if df_page.empty:
            break

        seen_ranks.update(df_page["bpi_rank"].tolist())
        all_rows.append(df_page)

        if len(ranks) < 50:
            break

        time.sleep(sleep_s)

    if not all_rows:
        raise RuntimeError("Failed to scrape any BPI pages from ESPN.")

    out = pd.concat(all_rows, ignore_index=True)
    out = out.sort_values("bpi_rank").drop_duplicates(subset=["bpi_rank"], keep="first").reset_index(drop=True)
    return out


def _existing_file_ok(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_csv(path)
        return {"bpi_rank", "team_bpi"}.issubset(df.columns) and len(df) >= 300 and df["team_bpi"].astype(str).str.len().mean() > 3
    except Exception:
        return False


def main():
    os.makedirs("data_raw", exist_ok=True)
    try:
        df = fetch_all_bpi()
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"Wrote {OUTPUT_PATH} ({len(df)} rows)")
        print(df.head(10).to_string(index=False))
    except Exception as e:
        if _existing_file_ok(OUTPUT_PATH):
            print(f"Warning: BPI scrape failed ({e}). Keeping existing {OUTPUT_PATH}.")
            return
        raise


if __name__ == "__main__":
    main()
