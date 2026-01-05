#!/usr/bin/env python3
import os
import re
from io import StringIO
import pandas as pd
import requests
import unicodedata

OUTPUT_PATH = "data_raw/bpi_rankings.csv"

BASE_PAGE_1 = "https://www.espn.com/mens-college-basketball/bpi"
BASE_PAGE_N = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def _strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _normalize_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    s = _strip_diacritics(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_bpi_team_name(raw) -> str:
    s = _normalize_text(raw)
    if not s:
        return s

    s = re.sub(r"\bA&\b", "A&M", s)

    s_nospace = s.replace(" ", "")
    if len(s_nospace) >= 6 and len(s_nospace) % 2 == 0:
        half = len(s_nospace) // 2
        if s_nospace[:half].upper() == s_nospace[half:].upper():
            return s_nospace[:half]

    m = re.match(r"^(.*?)([A-Z][A-Z0-9&'.-]{1,6})$", s)
    if m:
        base = m.group(1).strip()
        if len(base) >= 3:
            s = base

    s = re.sub(r"\s+", " ", s).strip()
    return s


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            " ".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in out.columns.values
        ]
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _pick_team_and_rank_columns(df: pd.DataFrame):
    cols = list(df.columns)

    rank_candidates = []
    for c in cols:
        num = pd.to_numeric(df[c], errors="coerce")
        if num.notna().mean() < 0.75:
            continue
        mn = float(num.min()) if num.notna().any() else None
        mx = float(num.max()) if num.notna().any() else None
        if mn is None or mx is None:
            continue
        if mn >= 1 and mx <= 400 and num.nunique(dropna=True) > 40:
            rank_candidates.append((c, num.notna().mean(), num.nunique(dropna=True)))

    team_candidates = []
    for c in cols:
        ser = df[c].astype(str).map(_normalize_text)
        has_letters = ser.str.contains(r"[A-Za-z]", regex=True, na=False).mean()
        if has_letters < 0.65:
            continue
        nunq = ser[ser != ""].nunique()
        if nunq > 40:
            team_candidates.append((c, has_letters, nunq))

    if not rank_candidates or not team_candidates:
        return None

    rank_col = sorted(rank_candidates, key=lambda x: (x[1], x[2]), reverse=True)[0][0]
    team_col = sorted(team_candidates, key=lambda x: (x[1], x[2]), reverse=True)[0][0]
    return team_col, rank_col


def _extract_bpi_from_html(html: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return pd.DataFrame()

    best = None
    for t in tables:
        df = _flatten_columns(t)
        picked = _pick_team_and_rank_columns(df)
        if not picked:
            continue
        team_col, rank_col = picked

        tmp = df[[rank_col, team_col]].copy()
        tmp.columns = ["bpi_rank", "team_bpi"]
        tmp["bpi_rank"] = pd.to_numeric(tmp["bpi_rank"], errors="coerce")
        tmp["team_bpi"] = tmp["team_bpi"].map(_clean_bpi_team_name)
        tmp = tmp.dropna(subset=["bpi_rank"])
        tmp = tmp[tmp["team_bpi"].astype(str).str.len() > 0]
        tmp["bpi_rank"] = tmp["bpi_rank"].astype(int)

        if len(tmp) >= 30:
            best = tmp
            break

    return best if best is not None else pd.DataFrame()


def fetch_all_bpi_pages(max_pages: int = 20) -> pd.DataFrame:
    session = requests.Session()

    all_rows = []
    seen_ranks = set()

    for page in range(1, max_pages + 1):
        url = BASE_PAGE_1 if page == 1 else BASE_PAGE_N.format(page)
        resp = session.get(url, headers=HEADERS, timeout=30)
        html = resp.text or ""

        df = _extract_bpi_from_html(html)
        if df.empty:
            break

        df = df[~df["bpi_rank"].isin(seen_ranks)]
        if df.empty:
            break

        seen_ranks.update(df["bpi_rank"].tolist())
        all_rows.append(df)

    if not all_rows:
        raise RuntimeError("Failed to scrape any BPI pages from ESPN.")

    out = pd.concat(all_rows, ignore_index=True)
    out = out.sort_values("bpi_rank").drop_duplicates(subset=["bpi_rank"], keep="first")
    return out


def _existing_file_is_usable(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        df = pd.read_csv(path)
        if {"bpi_rank", "team_bpi"}.issubset(df.columns) and len(df) >= 300:
            return True
        return False
    except Exception:
        return False


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    try:
        df = fetch_all_bpi_pages()
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"Wrote {OUTPUT_PATH} ({len(df)} rows)")
    except Exception as e:
        if _existing_file_is_usable(OUTPUT_PATH):
            print(f"Warning: BPI scrape failed ({e}). Keeping existing {OUTPUT_PATH} and continuing.")
            return
        raise


if __name__ == "__main__":
    main()
