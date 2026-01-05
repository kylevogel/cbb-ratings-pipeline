#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings (full D1).
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
import requests
import pandas as pd
from io import StringIO


BASE_URL = "https://www.espn.com.br/basquete/universitario-masculino/bpi/_/vs-division/overview/pagina/{}"


def _clean_team_name(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()

    s = re.sub(r"\s+", " ", s)

    m = re.match(r"^(.+?)([A-Z]{2,6})$", s)
    if m:
        base, abbr = m.group(1), m.group(2)

        if base.upper().endswith(abbr):
            s = base
        elif any(ch.islower() for ch in base) or any(ch in base for ch in " .&'()-"):
            s = base
        elif len(s) > 8:
            s = base

    return s.strip()


def _extract_table_df(html: str) -> pd.DataFrame | None:
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None

    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any(c in ("rk", "rank") for c in cols) and any("team" in c for c in cols):
            return t

    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any(c in ("rk", "rank") for c in cols):
            if len(t.columns) >= 2:
                return t

    return None


def scrape_bpi_rankings(max_pages: int = 20) -> pd.DataFrame | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    rows = []
    seen_ranks = set()

    print("Fetching ESPN BPI rankings (full D1)...")

    for page in range(1, max_pages + 1):
        url = BASE_URL.format(page)
        print(f"Fetching BPI page {page}: {url}")

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"  Non-200 status on page {page}: {resp.status_code}")
            break

        table = _extract_table_df(resp.text)
        if table is None or table.empty:
            print(f"  Could not parse BPI table on page {page}")
            break

        cols_lower = {str(c).strip().lower(): c for c in table.columns}
        rank_col = cols_lower.get("rk") or cols_lower.get("rank") or list(table.columns)[0]

        team_col = None
        for c in table.columns:
            if "team" in str(c).strip().lower():
                team_col = c
                break
        if team_col is None:
            team_col = list(table.columns)[1] if len(table.columns) >= 2 else list(table.columns)[0]

        added_this_page = 0

        for _, r in table.iterrows():
            rank_raw = r.get(rank_col, "")
            team_raw = r.get(team_col, "")

            rank_str = re.sub(r"[^\d]", "", str(rank_raw))
            if not rank_str:
                continue
            rank = int(rank_str)

            team = _clean_team_name(team_raw)
            if not team:
                continue

            if rank not in seen_ranks:
                seen_ranks.add(rank)
                rows.append({"bpi_rank": rank, "team_bpi": team})
                added_this_page += 1

        print(f"  Parsed {len(table)} rows, added {added_this_page} new ranks (total now {len(rows)})")

        if added_this_page == 0:
            break

        if len(rows) >= 365:
            break

    if not rows:
        return None

    df = pd.DataFrame(rows).drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")

    if len(df) > 365:
        df = df[df["bpi_rank"].between(1, 365)].copy().sort_values("bpi_rank")

    return df


def main():
    df = scrape_bpi_rankings(max_pages=20)

    os.makedirs("data_raw", exist_ok=True)

    if df is None or df.empty:
        print("Failed to fetch full BPI. Wrote empty data_raw/bpi_rankings.csv")
        pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv("data_raw/bpi_rankings.csv", index=False)
        raise SystemExit(1)

    df.to_csv("data_raw/bpi_rankings.csv", index=False)
    print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")

    if len(df) < 365:
        print(f"Warning: expected ~365 teams, got {len(df)}")


if __name__ == "__main__":
    main()
