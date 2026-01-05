#!/usr/bin/env python3
"""
Scrape ESPN BPI (Basketball Power Index) rankings (full D1).
Outputs: data_raw/bpi_rankings.csv
"""

import os
import re
from io import StringIO

import pandas as pd
import requests


def _extract_rank_team_table(html: str) -> pd.DataFrame | None:
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None

    for t in tables:
        if t is None or t.empty:
            continue

        cols = [str(c).strip() for c in t.columns]
        cols_l = [c.lower() for c in cols]

        if not any(c in ("rk", "rank") for c in cols_l):
            continue
        if not any("team" in c for c in cols_l):
            continue

        rank_col = None
        team_col = None

        for c, cl in zip(cols, cols_l):
            if cl in ("rk", "rank"):
                rank_col = c
                break

        for c, cl in zip(cols, cols_l):
            if "team" in cl:
                team_col = c
                break

        if rank_col is None or team_col is None:
            continue

        out = t[[rank_col, team_col]].copy()
        out.columns = ["bpi_rank", "team_bpi"]

        out["bpi_rank"] = (
            out["bpi_rank"]
            .astype(str)
            .apply(lambda x: re.sub(r"[^\d]", "", x))
        )
        out["bpi_rank"] = pd.to_numeric(out["bpi_rank"], errors="coerce")

        out["team_bpi"] = out["team_bpi"].astype(str).str.strip()

        out = out.dropna(subset=["bpi_rank", "team_bpi"])
        out["bpi_rank"] = out["bpi_rank"].astype(int)

        out = out[out["bpi_rank"] > 0]
        out = out[out["team_bpi"].str.len() > 0]

        if len(out) >= 20:
            return out[["bpi_rank", "team_bpi"]]

    return None


def scrape_bpi_rankings(max_pages: int = 12) -> pd.DataFrame | None:
    base_url = "https://www.espn.com.br/basquete/universitario-masculino/bpi/_/vs-division/overview/pagina/{}"

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    frames = []
    for page in range(1, max_pages + 1):
        url = base_url.format(page)
        print(f"Fetching BPI page {page}: {url}")

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"Request failed on page {page}: {e}")
            break

        df_page = _extract_rank_team_table(resp.text)
        if df_page is None or df_page.empty:
            print(f"Could not parse BPI table on page {page}")
            break

        frames.append(df_page)

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank")

    df = df[df["bpi_rank"].between(1, 400)]
    df = df.drop_duplicates(subset=["bpi_rank"]).sort_values("bpi_rank").reset_index(drop=True)

    return df


def main():
    print("Fetching ESPN BPI rankings (full D1)...")
    df = scrape_bpi_rankings()

    os.makedirs("data_raw", exist_ok=True)
    out_path = "data_raw/bpi_rankings.csv"

    if df is not None and not df.empty and len(df) >= 300:
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} BPI rankings to {out_path}")
        return

    print("Failed to fetch full BPI (expected ~365).")
    if os.path.exists(out_path):
        print(f"Keeping existing {out_path} (not overwriting).")
    else:
        pd.DataFrame(columns=["bpi_rank", "team_bpi"]).to_csv(out_path, index=False)
        print(f"Wrote empty {out_path}.")


if __name__ == "__main__":
    main()
