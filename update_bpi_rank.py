from pathlib import Path
import datetime as dt
import re
import io
import pandas as pd
import requests

SEASON = 2026
GROUP = 50

def _root():
    return Path(__file__).resolve().parent

def _data_raw(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p

def _norm(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _load_alias_map(path: Path):
    alias_to_canon = {}
    if not path.exists():
        return alias_to_canon
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            if not parts:
                continue
            canon = parts[0]
            for p in parts:
                alias_to_canon[_norm(p)] = canon
    return alias_to_canon

def _apply_alias(name: str, alias: dict):
    k = _norm(name)
    return alias.get(k, name)

def _session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.espn.com/",
        }
    )
    return s

def _pick_table(tables):
    best = None
    best_score = -1
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        has_team = any("team" in c for c in cols)
        has_rank = any(("bpi" in c and "rk" in c) or c == "bpi rk" for c in cols)
        if not (has_team and has_rank):
            continue
        score = (1 if has_team else 0) + (1 if has_rank else 0) + len(t)
        if score > best_score:
            best_score = score
            best = t
    return best

def _extract_team_rank(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c).strip() for c in df.columns]
    cols_l = [c.lower() for c in cols]

    team_idx = None
    rank_idx = None

    for i, c in enumerate(cols_l):
        if "team" in c:
            team_idx = i
            break

    for i, c in enumerate(cols_l):
        if c == "bpi rk" or ("bpi" in c and "rk" in c):
            rank_idx = i
            break

    if team_idx is None or rank_idx is None:
        return pd.DataFrame(columns=["Team", "BPI_Rank"])

    out = df.iloc[:, [team_idx, rank_idx]].copy()
    out.columns = ["Team", "BPI_Rank"]

    out["Team"] = out["Team"].astype(str).str.replace(r"\s*\(.*?\)\s*$", "", regex=True).str.strip()
    out["BPI_Rank"] = pd.to_numeric(out["BPI_Rank"], errors="coerce")
    out = out.dropna(subset=["BPI_Rank"])
    out["BPI_Rank"] = out["BPI_Rank"].astype(int)
    out = out.drop_duplicates(subset=["Team"], keep="first")
    return out

def _page_urls(page: int):
    urls = []
    if page == 1:
        urls += [
            f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/season/{SEASON}/group/{GROUP}",
            f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/season/{SEASON}",
            "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi",
            "https://www.espn.com/mens-college-basketball/bpi",
        ]
    else:
        urls += [
            f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/season/{SEASON}/group/{GROUP}/page/{page}",
            f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/season/{SEASON}/page/{page}",
            f"https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{page}",
            f"https://www.espn.com/mens-college-basketball/bpi/_/page/{page}/view/bpi",
        ]
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _fetch_page(page: int, s: requests.Session) -> pd.DataFrame:
    for url in _page_urls(page):
        try:
            r = s.get(url, timeout=25)
            if r.status_code != 200 or not r.text:
                continue
            tables = pd.read_html(io.StringIO(r.text))
            t = _pick_table(tables)
            if t is None:
                continue
            out = _extract_team_rank(t)
            if len(out) >= 20:
                return out
        except Exception:
            continue
    return pd.DataFrame(columns=["Team", "BPI_Rank"])

def main():
    root = _root()
    data_raw = _data_raw(root)

    alias = _load_alias_map(root / "team_alias.csv")

    s = _session()

    all_rows = []
    prev_len = 0
    empty_streak = 0

    for page in range(1, 25):
        dfp = _fetch_page(page, s)
        if dfp.empty:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        empty_streak = 0
        all_rows.append(dfp)

        cur = pd.concat(all_rows, ignore_index=True)
        cur = cur.drop_duplicates(subset=["Team"], keep="first")
        if len(cur) == prev_len:
            break
        prev_len = len(cur)

        if len(cur) >= 365:
            break

    if not all_rows:
        raise SystemExit("BPI scrape failed: no tables found.")

    df = pd.concat(all_rows, ignore_index=True)
    df["Team"] = df["Team"].astype(str).map(lambda x: _apply_alias(x, alias))
    df = df.sort_values("BPI_Rank").drop_duplicates(subset=["Team"], keep="first")
    df = df.reset_index(drop=True)

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    df.insert(0, "snapshot_date", snapshot_date)

    out_path = data_raw / "BPI_Rank.csv"
    df.to_csv(out_path, index=False)

    print(f"Wrote {out_path} with {len(df)} teams")

    if len(df) < 300:
        raise SystemExit(f"BPI scrape returned only {len(df)} teams. ESPN page structure may have changed.")

if __name__ == "__main__":
    main()
