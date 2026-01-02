from pathlib import Path
from datetime import date
import re
import csv
import difflib
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://www.espn.com.br/mens-college-basketball/bpi"
OUT_CSV = Path("data_raw") / "BPI_Rank.csv"
UNMATCHED_CSV = Path("data_raw") / "unmatched_bpi_teams.csv"

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _candidate_norms(raw: str) -> set[str]:
    raw = "" if raw is None else str(raw).strip()
    out = set()
    if not raw:
        return out
    out.add(_norm(raw))
    parts = raw.split()
    for k in range(1, min(4, len(parts))):
        out.add(_norm(" ".join(parts[:-k])))
    out.add(_norm(raw.replace(".", "")))
    out.add(_norm(raw.replace("St.", "State")))
    out.add(_norm(raw.replace("State", "St.")))
    out.add(_norm(raw.replace("Univ.", "University")))
    out.add(_norm(raw.replace("Univ", "University")))
    out.add(_norm(raw.replace("Mt.", "Mount")))
    out.add(_norm(raw.replace("Saint", "St.")))
    out.add(_norm(raw.replace("St.", "Saint")))
    out = {x for x in out if x}
    return out

def _fetch(url: str) -> str:
    r = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.text

def _max_page(html: str) -> int:
    nums = [int(x) for x in re.findall(r"/pagina/(\d+)", html)]
    return max(nums) if nums else 1

def _parse_bpi_table(html: str) -> list[tuple[int, str]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        return []
    rows = table.find_all("tr")
    out = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        rk_txt = tds[0].get_text(" ", strip=True)
        team_txt = tds[1].get_text(" ", strip=True)
        rk_txt = rk_txt.replace("#", "").strip()
        if not rk_txt.isdigit():
            continue
        rk = int(rk_txt)
        team_txt = re.sub(r"\s+", " ", team_txt).strip()
        if not team_txt:
            continue
        out.append((rk, team_txt))
    return out

def _find_alias_path(root: Path) -> Path:
    p1 = root / "team_alias.csv"
    p2 = root / "data_raw" / "team_alias.csv"
    if p1.exists():
        return p1
    if p2.exists():
        return p2
    raise FileNotFoundError("Could not find team_alias.csv in repo root or data_raw/")

def _load_alias_map(root: Path) -> tuple[dict[str, str], list[str], dict[str, str]]:
    alias_path = _find_alias_path(root)
    df = pd.read_csv(alias_path, dtype=str).fillna("")
    needed = {"standard_name", "bpi_name"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"team_alias.csv missing columns: {sorted(missing)}")

    norm_to_standard = {}
    standard_norms = []
    norm_to_standard_primary = {}

    for _, row in df.iterrows():
        std = str(row["standard_name"]).strip()
        bpi = str(row["bpi_name"]).strip()
        if not std:
            continue

        std_norm = _norm(std)
        if std_norm:
            standard_norms.append(std_norm)
            norm_to_standard_primary[std_norm] = std

        for cand in _candidate_norms(std):
            norm_to_standard[cand] = std

        if bpi:
            for cand in _candidate_norms(bpi):
                norm_to_standard[cand] = std

    return norm_to_standard, standard_norms, norm_to_standard_primary

def _best_suggestion(src: str, standard_norms: list[str], norm_to_standard_primary: dict[str, str]) -> tuple[str, float]:
    src_norm = _norm(src)
    if not src_norm or not standard_norms:
        return "", 0.0
    matches = difflib.get_close_matches(src_norm, standard_norms, n=1, cutoff=0.0)
    if not matches:
        return "", 0.0
    best = matches[0]
    score = difflib.SequenceMatcher(None, src_norm, best).ratio()
    return norm_to_standard_primary.get(best, ""), float(score)

def main():
    root = Path(__file__).resolve().parent
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    html1 = _fetch(BASE_URL)
    maxp = _max_page(html1)

    scraped = {}
    for p in range(1, maxp + 1):
        url = BASE_URL if p == 1 else f"{BASE_URL}/_/pagina/{p}"
        html = html1 if p == 1 else _fetch(url)
        pairs = _parse_bpi_table(html)
        for rk, team in pairs:
            if team not in scraped or rk < scraped[team]:
                scraped[team] = rk

    norm_to_standard, standard_norms, norm_to_standard_primary = _load_alias_map(root)

    snapshot = date.today().isoformat()
    out_rows = []
    unmatched = []

    for team, rk in sorted(scraped.items(), key=lambda x: x[1]):
        std = ""
        for cand in _candidate_norms(team):
            if cand in norm_to_standard:
                std = norm_to_standard[cand]
                break

        if not std:
            sug, score = _best_suggestion(team, standard_norms, norm_to_standard_primary)
            unmatched.append((team, sug, score))
            continue

        out_rows.append((snapshot, std, int(rk)))

    out_rows = sorted(out_rows, key=lambda x: x[2])

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["snapshot_date", "Team", "BPI"])
        w.writerows(out_rows)

    with UNMATCHED_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_team", "suggested_standard", "match_score"])
        for a, b, c in unmatched:
            w.writerow([a, b, f"{c:.3f}"])

    print("BPI_Rank.csv")
    print(str(UNMATCHED_CSV))
    print("snapshot_date,Team,BPI")
    for r in out_rows[:5]:
        print(f"{r[0]},{r[1]},{r[2]}")

if __name__ == "__main__":
    main()
