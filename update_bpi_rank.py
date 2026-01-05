#!/usr/bin/env python3
"""
Update ESPN BPI rankings.

Outputs:
  - data_raw/bpi_rankings.csv with columns: bpi_rank, team_bpi
"""

import os
import re
import time
import unicodedata
import pandas as pd
import requests

OUTPUT_PATH = "data_raw/bpi_rankings.csv"
BASE_URL = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in col if str(x).lower() != "nan"]).strip()
            for col in df.columns.to_list()
        ]
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _clean_team_name(s: str) -> str:
    if s is None:
        return ""

    s = str(s).strip()

    # Normalize unicode (San José -> San Jose, fancy apostrophes, etc.)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    # Fix common "duplicated acronym" cases with no separator: BYUBYU, UCLAUCLA, etc.
    # If string is exactly two identical halves, keep one half.
    if len(s) % 2 == 0:
        half = len(s) // 2
        if s[:half] == s[half:]:
            s = s[:half]

    # Fix common "duplicated acronym" with space: "BYU BYU"
    parts = s.split(" ")
    if len(parts) == 2 and parts[0] == parts[1]:
        s = parts[0]

    # Remove trailing appended abbreviation with NO separator, e.g.:
    # "Texas A&MTA&M" -> "Texas A&M"
    # "William & MaryW&M" -> "William & Mary"
    # "Miami (OH)M-" -> "Miami (OH)"
    # "Loyola MarylandL-" -> "Loyola Maryland"
    #
    # Heuristic: if the end looks like a short all-caps abbreviation and it's glued on, drop it.
    m = re.match(r"^(.*?)([A-Z][A-Z&\-\.\']{1,6})$", s)
    if m:
        base, abbr = m.group(1), m.group(2)
        # Only drop if it looks like a glued-on suffix (base does not end with space)
        # and base is a plausible team name (contains lowercase OR space OR punctuation)
        if base and not base.endswith(" ") and (
            any(ch.islower() for ch in base) or (" " in base) or ("&" in base) or ("(" in base) or ("." in base)
        ):
            s = base.strip()

    # Final whitespace cleanup
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _pick_bpi_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for t in tables:
        t = _flatten_columns(t.copy())
        cols_upper = [c.upper() for c in t.columns]
        has_team = any("TEAM" == c or c.startswith("TEAM") for c in cols_upper)
        has_bpi_rk = any("BPI RK" in c for c in cols_upper)
        if has_team and has_bpi_rk:
            return t
    return None


def _pick_team_col(df: pd.DataFrame) -> str:
    # Candidate columns that look like Team columns
    team_cols = [c for c in df.columns if c.strip().upper().startswith("TEAM")]
    if not team_cols:
        raise RuntimeError("Could not find a TEAM column in the BPI table.")

    # If multiple, choose the one with longer average strings (usually the actual team name)
    best_col = None
    best_score = -1.0
    for c in team_cols:
        series = df[c].astype(str).fillna("")
        avg_len = series.map(len).mean()
        # Reward columns that contain spaces/apostrophes (more likely full names)
        has_space = series.str.contains(r"\s").mean()
        score = avg_len + 10 * has_space
        if score > best_score:
            best_score = score
            best_col = c

    return best_col if best_col is not None else team_cols[0]


def fetch_all_bpi_pages(max_pages: int = 40, sleep_s: float = 0.35) -> pd.DataFrame:
    all_rows: list[pd.DataFrame] = []

    for page in range(1, max_pages + 1):
        url = BASE_URL.format(page)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        tables = pd.read_html(resp.text)
        bpi_table = _pick_bpi_table(tables)
        if bpi_table is None or bpi_table.empty:
            break

        bpi_table = _flatten_columns(bpi_table)

        # Columns
        team_col = _pick_team_col(bpi_table)
        rk_col = next(c for c in bpi_table.columns if "BPI RK" in c.upper())

        page_df = bpi_table[[team_col, rk_col]].copy()
        page_df.columns = ["team_bpi", "bpi_rank"]

        page_df["bpi_rank"] = pd.to_numeric(page_df["bpi_rank"], errors="coerce")
        page_df["team_bpi"] = page_df["team_bpi"].astype(str).map(_clean_team_name)

        page_df = page_df.dropna(subset=["bpi_rank"])
        page_df = page_df[page_df["team_bpi"].astype(bool)]
        if page_df.empty:
            break

        all_rows.append(page_df)

        # be polite
        time.sleep(sleep_s)

    if not all_rows:
        raise RuntimeError("Failed to scrape any BPI pages from ESPN.")

    out = pd.concat(all_rows, ignore_index=True)
    out = out.drop_duplicates(subset=["team_bpi"], keep="first")
    out = out.sort_values("bpi_rank").reset_index(drop=True)
    out["bpi_rank"] = out["bpi_rank"].astype(int)

    return out


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df = fetch_all_bpi_pages()

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(df)} rows to {OUTPUT_PATH}")
    print(f"Rank range: {df['bpi_rank'].min()}–{df['bpi_rank'].max()}")

    # Quick sanity: show any still-suspicious names
    weird = df[df["team_bpi"].str.contains(r"[A-Z]{2,}\1", regex=True)]
    if len(weird) > 0:
        print("\nSuspicious duplicated-acronym names (check these):")
        print(weird.head(25).to_string(index=False))


if __name__ == "__main__":
    main()
