import datetime as dt
import io
import re
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests


SEASON = 2026
SOURCE_URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.warrennolan.com/",
        }
    )
    return s


def _get(url: str, s: requests.Session, timeout: int = 25) -> str:
    r = s.get(url, timeout=timeout)
    if r.status_code != 200:
        return ""
    return r.text or ""


def _discover_candidates(html: str) -> list[str]:
    cands = []

    for m in re.findall(r'["\'](\/_ajax\/[^"\']+)["\']', html or ""):
        if "sos" in m and "predict" in m:
            cands.append("https://www.warrennolan.com" + m)

    guesses = [
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict/",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict?sort=rank",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict?view=all",
    ]
    cands.extend(guesses)

    out = []
    seen = set()
    for u in cands:
        u2 = u.strip()
        if u2 and u2 not in seen:
            seen.add(u2)
            out.append(u2)
    return out


def _pick_table(html: str) -> pd.DataFrame:
    if not html:
        return pd.DataFrame()

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        return pd.DataFrame()

    best = pd.DataFrame()
    best_rows = 0

    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("team" in c for c in cols) and any(c == "rank" or "rank" in c for c in cols):
            if len(t) > best_rows:
                best = t.copy()
                best_rows = len(t)

    return best


def _norm(s: str) -> str:
    x = (s or "").lower().strip()
    x = x.replace("&", " and ")
    x = re.sub(r"[\.\,\(\)\[\]\{\}\-\/']", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    x = re.sub(r"\bstate\b", "st", x)
    x = re.sub(r"\bst\b", "st", x)
    x = re.sub(r"\bsaint\b", "st", x)
    x = re.sub(r"\bmt\b", "mount", x)

    x = re.sub(r"\bnc\b", "north carolina", x)
    x = re.sub(r"\bsc\b", "south carolina", x)
    x = re.sub(r"\bnd\b", "north dakota", x)
    x = re.sub(r"\bsd\b", "south dakota", x)
    x = re.sub(r"\bnm\b", "new mexico", x)

    x = re.sub(r"\s+", " ", x).strip()
    return x
