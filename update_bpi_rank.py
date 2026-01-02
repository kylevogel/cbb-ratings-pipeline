import re
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher


ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data_raw"
TEAM_ALIAS_PATH = ROOT / "team_alias.csv"

BASE_URL = "https://www.espn.com/mens-college-basketball/bpi"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _load_alias_maps():
    df = pd.read_csv(TEAM_ALIAS_PATH, dtype=str).fillna("")
    cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
    for c in cols:
        if c not in df.columns:
            raise ValueError(f"team_alias.csv missing column: {c}")

    alias_to_standard = {}
    standard_names = []

    for _, row in df.iterrows():
        standard = row["standard_name"].strip()
        if not standard:
            continue
        standard_names.append(standard)

        for col in cols:
            val = row[col].strip()
            if not val:
                continue

            parts = [val]
            if col == "game_log_name" or "," in val:
                parts = [p.strip() for p in val.split(",")]

            for p in parts:
                if not p:
                    continue
                alias_to_standard[_norm(p)] = standard

        alias_to_standard[_norm(standard)] = standard

    return alias_to_standard, standard_names


def _best_standard_suggestion(source_team: str, standard_names: list[str]):
    src = _norm(source_team)
    best_name = ""
    best_score = 0.0
    for st in standard_names:
        score = SequenceMatcher(None, src, _norm(st)).ratio()
        if score > best_score:
            best_score = score
            best_name = st
    return best_name, best_score


def _get_group_ids():
    r = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    ids = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/mens-college-basketball/bpi/_/group/(\d+)", href)
        if m:
            ids.add(int(m.group(1)))

    return sorted(ids)


def _extract_table_rows(html: str):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    target = None

    for t in tables:
        text = t.get_text(" ", strip=True)
        if "BPI RK" in text and "POWER INDEX PROJECTIONS" in text:
            target = t
            break

    if target is None:
        for t in tables:
            if "BPI RK" in t.get_text(" ", strip=True):
                target = t
                break

    if target is None:
        return []

    thead = target.find("thead")
    if not thead:
        return []

    headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
    rk_idx = None
    for i, h in enumerate(headers):
        if h.strip().upper() == "BPI RK":
            rk_idx = i
            break

    if rk_idx is None:
        return []

    tbody = target.find("tbody")
    if not tbody:
        return []

    out = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) <= rk_idx:
            continue

        team = ""
        team_a = tr.find("a", href=True)
        if team_a:
            team = team_a.get_text(" ", strip=True)

        rk_txt = tds[rk_idx].get_text(" ", strip=True)
        rk_txt = rk_txt.replace("th", "").replace("st", "").replace("nd", "").replace("rd", "")
        rk_txt = rk_txt.strip()

        if not team or not rk_txt.isdigit():
            continue

        out.append((team, int(rk_txt)))

    return out


def _scrape_all_bpi_ranks():
    group_ids = _get_group_ids()
    if not group_ids:
        raise RuntimeError("Could not find any ESPN conference group ids on the BPI page.")

    ranks = {}
    for gid in group_ids:
        url = f"{BASE_URL}/_/group/{gid}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()

        rows = _extract_table_rows(r.text)
        for team, rk in rows:
            if team not in ranks or rk < ranks[team]:
                ranks[team] = rk

    return ranks


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias_to_standard, standard_names = _load_alias_maps()
    ranks = _scrape_all_bpi_ranks()

    snap = date.today().isoformat()
    out_rows = []
    unmatched_rows = []

    for source_team, rk in sorted(ranks.items(), key=lambda x: x[1]):
        key = _norm(source_team)
        standard = alias_to_standard.get(key, "")

        if not standard:
            sugg, score = _best_standard_suggestion(source_team, standard_names)
            unmatched_rows.append(
                {"source_team": source_team, "suggested_standard": sugg, "match_score": round(score, 4)}
            )
            continue

        out_rows.append({"snapshot_date": snap, "Team": standard, "BPI": int(rk)})

    out_df = pd.DataFrame(out_rows).sort_values("BPI", ascending=True)
    out_path = DATA_RAW / "BPI_Rank.csv"
    out_df.to_csv(out_path, index=False)

    unmatched_path = DATA_RAW / "unmatched_bpi_teams.csv"
    if unmatched_rows:
        pd.DataFrame(unmatched_rows).sort_values("match_score", ascending=False).to_csv(unmatched_path, index=False)
    else:
        pd.DataFrame(columns=["source_team", "suggested_standard", "match_score"]).to_csv(unmatched_path, index=False)

    print("BPI_Rank.csv")
    print("unmatched_bpi_teams.csv")
    print(out_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
