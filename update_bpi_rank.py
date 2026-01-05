#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings (full D1).
Outputs: data_raw/bpi_rankings.csv

Why this exists:
- ESPN's main US BPI page often only yields partial/Top-50 content server-side.
- ESPN Brazil pages are frequently server-rendered for all D1 teams across pages.
- Some pages (notably the last page) may NOT contain a real <table>, so pd.read_html fails.
  This script uses a fallback text-line parser to capture those rows.
"""

import os
import re
import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO


BASE_URL_TEMPLATE = "https://www.espn.com.br/basquete/universitario-masculino/bpi/_/vs-division/overview/pagina/{page}"


def _is_short_upper_token(tok: str) -> bool:
    """
    Team codes and some conference abbreviations are short and all-uppercase (may include digits/&/-/.).
    """
    if not tok or len(tok) > 6:
        return False
    # Must contain at least one A-Z
    if not re.search(r"[A-Z]", tok):
        return False
    # Must be "uppercase" (no lowercase letters)
    return tok.upper() == tok


def parse_bpi_rows_from_text(html_text: str) -> pd.DataFrame:
    """
    Fallback parser for ESPN pages that do not contain a proper HTML table.
    Extracts rows from text lines like:
      "351 NJIT NJIT Am. East 6-10 351º 169º 286º--"
      "365 Mississippi Valley State MVSU SWAC 1-14 365º 48º 310º--"
    """
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    rows = []

    for line in lines:
        # Must start with a rank number
        if not re.match(r"^\d{1,3}\s+", line):
            continue

        # ESPN uses "º" (Portuguese ordinal) in later columns; helps confirm it's a data row
        if ("º" not in line) and ("°" not in line):
            continue

        tokens = line.split()
        if len(tokens) < 6:
            continue

        # First token = rank
        try:
            rank = int(tokens[0])
        except Exception:
            continue

        # Find the record token W-L
        record_idx = None
        for i in range(1, len(tokens)):
            if re.match(r"^\d{1,2}-\d{1,2}$", tokens[i]):
                record_idx = i
                break
        if record_idx is None:
            continue

        # Identify short-uppercase tokens between rank and record
        upper_idxs = []
        for i in range(1, record_idx):
            if _is_short_upper_token(tokens[i]):
                upper_idxs.append(i)

        if not upper_idxs:
            continue

        # If the token right before record is uppercase, it's often the CONFERENCE abbreviation (SEC, SWAC, ACC, etc.)
        # In that case, the team code is typically the previous short-uppercase token.
        if (record_idx - 1) in upper_idxs and len(upper_idxs) >= 2:
            team_code_idx = upper_idxs[-2]
        else:
            team_code_idx = upper_idxs[-1]

        team_name = " ".join(tokens[1:team_code_idx]).strip()

        if not team_name:
            continue

        if 1 <= rank <= 400:
            rows.append({"bpi_rank": rank, "team_bpi": team_name})

    if not rows:
        return pd.DataFrame(columns=["bpi_rank", "team_bpi"])

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)
    return df


def parse_bpi_rows_from_tables(html_text: str) -> pd.DataFrame:
    """
    Try to parse an actual HTML table with pandas first.
    If ESPN renders a real table, this will work; otherwise returns empty.
    """
    try:
        tables = pd.read_html(StringIO(html_text))
    except Exception:
        return pd.DataFrame(columns=["bpi_rank", "team_bpi"])

    best = None

    for t in tables:
        if t is None or t.empty:
            continue

        # Normalize column names
        cols = [str(c).strip().lower() for c in t.columns]
        # Heuristic: must contain a rank-like column and a team-like column
        has_rank = any(c in ("rk", "rank") or "rk" == c for c in cols) or any("rk" == c.replace(".", "") for c in cols)
        has_team = any("team" in c for c in cols)

        if not (has_rank and has_team):
            continue

        # Find likely team/rank columns
        rank_col = None
        team_col = None

        for orig, c in zip(t.columns, cols):
            if c in ("rk", "rank") or c.replace(".", "") == "rk":
                rank_col = orig
            if team_col is None and "team" in c:
                team_col = orig

        if rank_col is None or team_col is None:
            continue

        tmp = t[[team_col, rank_col]].copy()
        tmp.columns = ["team_bpi", "bpi_rank"]
        tmp["bpi_rank"] = pd.to_numeric(tmp["bpi_rank"], errors="coerce")
        tmp = tmp.dropna()
        tmp["bpi_rank"] = tmp["bpi_rank"].astype(int)
        tmp["team_bpi"] = tmp["team_bpi"].astype(str).str.strip()
        tmp = tmp[tmp["team_bpi"] != ""]
        tmp = tmp.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)

        # Choose the largest plausible table found
        if best is None or len(tmp) > len(best):
            best = tmp

    if best is None:
        return pd.DataFrame(columns=["bpi_rank", "team_bpi"])
    return best


def scrape_bpi_rankings_full(max_pages: int = 12) -> pd.DataFrame:
    """
    Scrape full NCAA D1 BPI by iterating pages until we reach rank 365
    (or until we stop getting new ranks).
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    all_rows = []
    seen_ranks = set()
    last_new_count = -1

    for page in range(1, max_pages + 1):
        url = BASE_URL_TEMPLATE.format(page=page)
        print(f"Fetching BPI page {page}: {url}")

        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            print(f"Page {page} returned HTTP {resp.status_code} — stopping.")
            break

        html = resp.text

        # 1) Try real table parsing
        df_table = parse_bpi_rows_from_tables(html)

        # 2) Fallback to text-line parsing if needed
        if df_table.empty:
            df_page = parse_bpi_rows_from_text(html)
        else:
            df_page = df_table

        if df_page.empty:
            print(f"Could not parse BPI rows on page {page} — stopping.")
            break

        # Add only new ranks
        new = 0
        for _, r in df_page.iterrows():
            rk = int(r["bpi_rank"])
            if rk not in seen_ranks:
                seen_ranks.add(rk)
                all_rows.append({"bpi_rank": rk, "team_bpi": r["team_bpi"]})
                new += 1

        print(f"  Parsed {len(df_page)} rows, added {new} new ranks (total now {len(seen_ranks)})")

        # Stop conditions:
        if max(seen_ranks) >= 365 and len(seen_ranks) >= 365:
            break

        # If we stop making progress, bail
        if new == 0:
            if last_new_count == 0:
                print("No new ranks found on consecutive pages — stopping.")
                break
            last_new_count = 0
        else:
            last_new_count = new

    if not all_rows:
        return pd.DataFrame(columns=["bpi_rank", "team_bpi"])

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)
    return df


def main():
    print("Fetching ESPN BPI rankings (full D1)...")
    df = scrape_bpi_rankings_full(max_pages=12)

    os.makedirs("data_raw", exist_ok=True)

    if df is None or df.empty:
        print("Failed to fetch BPI rankings — wrote empty data_raw/bpi_rankings.csv")
        pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv("data_raw/bpi_rankings.csv", index=False)
        sys.exit(1)

    df.to_csv("data_raw/bpi_rankings.csv", index=False)
    print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")

    # Validate completeness
    ranks = set(df["bpi_rank"].tolist())
    missing = [r for r in range(1, 366) if r not in ranks]

    if missing:
        print(f"WARNING: Missing {len(missing)} BPI ranks: {missing[:50]}{'...' if len(missing) > 50 else ''}")
        # Treat as failure so update_all.py reports it clearly
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
