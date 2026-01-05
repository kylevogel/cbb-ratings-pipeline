#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings.
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
from io import StringIO

import pandas as pd
import requests


def _clean_bpi_team_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)

    s = re.sub(r"([a-z0-9\)\]\}'\.])([A-Z]{2,6})$", r"\1 \2", s)

    tokens = s.split(" ")
    if len(tokens) >= 2:
        last = tokens[-1]
        prev = tokens[-2]

        if last == prev:
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]{2,6}", last):
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]{1,3}-[A-Z]{1,3}", last):
            tokens = tokens[:-1]
        elif re.fullmatch(r"[A-Z]&[A-Z]", last):
            tokens = tokens[:-1]

    s = " ".join(tokens).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _pick_table(tables):
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        has_rank = any(c in ("rk", "rank", "#") or "rk" == c for c in cols) or (
            len(cols) >= 1 and ("rk" in cols[0] or "rank" in cols[0] or cols[0] == "#")
        )
        has_team = any(("team" in c) or (c == "time") or ("equipe" in c) for c in cols)
        if has_rank and has_team:
            return t

    for t in tables:
        if len(t.columns) >= 2:
            return t

    return None


def _extract_rank_team(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    cols_lower = [str(c).strip().lower() for c in cols]

    rank_col = None
    for c, cl in zip(cols, cols_lower):
        if cl in ("rk", "rank", "#") or cl.startswith("rk") or "rank" == cl:
            rank_col = c
            break
    if rank_col is None:
        rank_col = cols[0]

    team_col = None
    for c, cl in zip(cols, cols_lower):
        if ("team" in cl) or (cl == "time") or ("equipe" in cl):
            team_col = c
            break
    if team_col is None:
        team_col = cols[1] if len(cols) > 1 else cols[0]

    out = df[[rank_col, team_col]].copy()
    out.columns = ["bpi_rank", "team_bpi"]

    out["bpi_rank"] = pd.to_numeric(out["bpi_rank"], errors="coerce")
    out = out.dropna(subset=["bpi_rank"])
    out["bpi_rank"] = out["bpi_rank"].astype(int)

    out["team_bpi"] = out["team_bpi"].astype(str).map(_clean_bpi_team_text)
    out = out[out["team_bpi"].str.len() > 0]

    out = out.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")
    return out


def scrape_bpi_rankings():
    base = "https://www.espn.com.br/basquete/universitario-masculino/bpi/_/vs-division/overview/pagina/{page}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    rows = []
    seen_ranks = set()

    for page in range(1, 15):
        url = base.format(page=page)
        print(f"Fetching BPI page {page}: {url}")

        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"  HTTP {resp.status_code} on page {page}")
            break

        try:
            tables = pd.read_html(StringIO(resp.text))
        except Exception:
            print(f"  Could not read_html on page {page}")
            break

        t = _pick_table(tables)
        if t is None:
            print(f"  Could not locate BPI table on page {page}")
            break

        parsed = _extract_rank_team(t)
        added = 0
        for r in parsed.to_dict("records"):
            rk = int(r["bpi_rank"])
            if rk not in seen_ranks:
                seen_ranks.add(rk)
                rows.append(r)
                added += 1

        print(f"  Parsed {len(parsed)} rows, added {added} new ranks (total now {len(rows)})")

        if added == 0:
            break

        if len(rows) >= 365:
            break

    if not rows:
        return None

    df = pd.DataFrame(rows).drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")

    if len(df) >= 365:
        df = df[df["bpi_rank"] <= 365]

    return df


def main():
    print("Fetching ESPN BPI rankings (full D1)...")
    df = scrape_bpi_rankings()

    os.makedirs("data_raw", exist_ok=True)

    if df is None or df.empty:
        pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv("data_raw/bpi_rankings.csv", index=False)
        print("Failed to fetch BPI rankings. Wrote empty data_raw/bpi_rankings.csv")
        raise SystemExit(1)

    df.to_csv("data_raw/bpi_rankings.csv", index=False)
    print(f"Saved {len(df)} BPI rankings to data_raw/bpi_rankings.csv")

    bad = df["team_bpi"].astype(str).str.contains(r"[a-z0-9\)\]'\.][A-Z]{2,6}$", regex=True)
    if bad.any():
        print("\nWarning: some team_bpi values still look like they have a trailing abbrev:")
        print(df.loc[bad, ["bpi_rank", "team_bpi"]].head(30).to_string(index=False))


if __name__ == "__main__":
    main()
