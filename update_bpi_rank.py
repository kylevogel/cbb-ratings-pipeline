import os
import sys
import tempfile
import requests
import pandas as pd
from bs4 import BeautifulSoup


URL = "https://www.espn.com/mens-college-basketball/bpi"


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
        "Referer": "https://www.espn.com/",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def _clean_tokens(soup: BeautifulSoup) -> list[str]:
    text = soup.get_text("\n")
    toks = []
    for ln in text.splitlines():
        t = ln.replace("\xa0", " ").strip()
        if not t:
            continue
        toks.append(t)
    return toks


def _idx(tokens: list[str], target: str) -> int:
    tl = target.strip().lower()
    for i, t in enumerate(tokens):
        if t.strip().lower() == tl:
            return i
    raise ValueError(f"Token not found: {target}")


def main() -> int:
    out_path = os.path.join("data_raw", "bpi.csv")

    try:
        html = _fetch_html(URL)
        soup = BeautifulSoup(html, "lxml")
        tokens = _clean_tokens(soup)

        a = _idx(tokens, "Team CONF") + 1
        b = _idx(tokens, "POWER INDEX PROJECTIONS")
        seg = tokens[a:b]

        teams = []
        i = 0
        while i + 1 < len(seg):
            team = seg[i]
            conf = seg[i + 1]

            if team.startswith("Image:"):
                team = team.replace("Image:", "").strip()

            if team and conf:
                teams.append(team)

            i += 2

        if len(teams) < 300:
            raise RuntimeError(f"Could not extract team list (got {len(teams)})")

        header_end = _idx(tokens, "REM SOS RK") + 1
        data = tokens[header_end:]

        cols_per_team = 10
        need = len(teams) * cols_per_team
        if len(data) < need:
            data = data[: (len(data) // cols_per_team) * cols_per_team]
            teams = teams[: len(data) // cols_per_team]

        rows = []
        j = 0
        for team in teams:
            chunk = data[j : j + cols_per_team]
            j += cols_per_team

            wl = chunk[0]
            bpi_val = chunk[1]
            bpi_rk = chunk[2]

            try:
                bpi_rk_i = int(str(bpi_rk).replace("#", "").strip())
            except Exception:
                continue

            try:
                bpi_f = float(str(bpi_val).replace("+", "").strip())
            except Exception:
                bpi_f = None

            rows.append((team, bpi_rk_i, bpi_f, wl))

        df = pd.DataFrame(rows, columns=["bpi_name", "bpi_rank", "bpi", "wl"]).drop_duplicates("bpi_rank")
        df = df.sort_values("bpi_rank").reset_index(drop=True)

        if len(df) < 300:
            raise RuntimeError(f"Parsed too few BPI rows ({len(df)})")

        _atomic_write_csv(df[["bpi_name", "bpi_rank", "bpi"]], out_path)
        print(f"Wrote {len(df)} rows -> {out_path}")
        return 0

    except Exception as e:
        print(f"update_bpi_rank failed: {e}", file=sys.stderr)
        if os.path.exists(out_path):
            print(f"Keeping existing file -> {out_path}")
            return 0
        empty = pd.DataFrame(columns=["bpi_name", "bpi_rank", "bpi"])
        _atomic_write_csv(empty, out_path)
        print(f"Wrote 0 rows -> {out_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
