import os
import re
import sys
import tempfile
import requests
import pandas as pd
from bs4 import BeautifulSoup


URL = "https://kenpom.com/"


def _atomic_write_csv(df: pd.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    d = os.path.dirname(out_path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".csv", newline="") as f:
        tmp = f.name
        df.to_csv(tmp, index=False)
    os.replace(tmp, out_path)


def _fetch_html(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://kenpom.com/",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def _parse_from_tables(soup: BeautifulSoup) -> pd.DataFrame | None:
    tables = soup.find_all("table")
    best_rows = []

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        header_text = " ".join(rows[0].get_text(" ", strip=True).split())
        header_lc = header_text.lower()
        if "rk" not in header_lc and "rank" not in header_lc:
            continue
        if "team" not in header_lc:
            continue

        parsed = []
        for tr in rows[1:]:
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            rk_txt = tds[0].get_text(" ", strip=True)
            team_txt = tds[1].get_text(" ", strip=True)

            rk_txt = rk_txt.replace("\xa0", " ").strip()
            team_txt = team_txt.replace("\xa0", " ").strip()

            if not rk_txt.isdigit():
                continue
            rk = int(rk_txt)
            if not team_txt:
                continue

            parsed.append((team_txt, rk))

        if len(parsed) > len(best_rows):
            best_rows = parsed

    if len(best_rows) >= 300:
        df = pd.DataFrame(best_rows, columns=["kenpom_name", "kenpom_rank"]).sort_values("kenpom_rank")
        return df.reset_index(drop=True)

    return None


def _parse_fallback_text(soup: BeautifulSoup) -> pd.DataFrame | None:
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    joined = "\n".join(lines)

    pat = re.compile(
        r"^\s*(\d{1,3})\s+([A-Za-z].+?)\s+(?:[A-Z]{2,6}|Amer|A10|A-Sun|WCC|CUSA|ASun|MVC|Horz|Slnd|Pat|Ivy|B10|B12|P12|SEC|ACC|MWC|WAC|CAA|MAC|SB|BE|BW|AE|NEC|Sum|BSky|OVC|SC|MAAC|PL|SWAC|MEAC)\s+(\d{1,2}-\d{1,2})",
        re.MULTILINE,
    )

    rows = []
    for m in pat.finditer(joined):
        rk = int(m.group(1))
        team = m.group(2).strip()
        rows.append((team, rk))

    if len(rows) >= 300:
        df = pd.DataFrame(rows, columns=["kenpom_name", "kenpom_rank"]).drop_duplicates("kenpom_rank")
        df = df.sort_values("kenpom_rank").reset_index(drop=True)
        return df

    return None


def main() -> int:
    out_path = os.path.join("data_raw", "kenpom.csv")

    try:
        html = _fetch_html(URL)
        soup = BeautifulSoup(html, "lxml")

        df = _parse_from_tables(soup)
        if df is None:
            df = _parse_fallback_text(soup)

        if df is None or df.empty:
            raise RuntimeError("Could not parse KenPom rankings")

        _atomic_write_csv(df, out_path)
        print(f"Wrote {len(df)} rows -> {out_path}")
        return 0

    except Exception as e:
        print(f"update_kenpom_rank failed: {e}", file=sys.stderr)
        if os.path.exists(out_path):
            print(f"Keeping existing file -> {out_path}")
            return 0
        empty = pd.DataFrame(columns=["kenpom_name", "kenpom_rank"])
        _atomic_write_csv(empty, out_path)
        print(f"Wrote 0 rows -> {out_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
