from pathlib import Path
from datetime import date
from difflib import SequenceMatcher
import json
import pandas as pd
import requests

DATA_RAW = Path("data_raw")
ALIAS_PATH = Path("team_alias.csv")
OUT_PATH = DATA_RAW / "BPI_Rank.csv"
UNMATCHED_PATH = DATA_RAW / "unmatched_bpi_teams.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

URLS = [
    "https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?region=us&lang=en&limit=400",
    "https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?lang=en&region=us&limit=400",
    "https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/bpi?limit=400",
]

def norm(s):
    return str(s or "").strip().casefold().replace("’", "'")

def ratio(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def _extract_name(team_obj):
    if isinstance(team_obj, dict):
        for k in ("displayName", "shortDisplayName", "name", "abbreviation"):
            v = team_obj.get(k)
            if v:
                return str(v).strip()
        if "team" in team_obj and isinstance(team_obj["team"], dict):
            return _extract_name(team_obj["team"])
    return ""

def _extract_rank(d):
    if not isinstance(d, dict):
        return None
    for k in ("rank", "currentRank", "bpiRank", "rk"):
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            try:
                return int(v)
            except:
                pass
    if "stats" in d and isinstance(d["stats"], list):
        for s in d["stats"]:
            if isinstance(s, dict) and str(s.get("name", "")).strip().lower() in ("rank", "bpi rank"):
                try:
                    return int(s.get("value"))
                except:
                    pass
    return None

def extract_team_ranks(obj):
    out = []
    if isinstance(obj, dict):
        team_name = ""
        if "team" in obj:
            team_name = _extract_name(obj.get("team"))
        if not team_name and "name" in obj and isinstance(obj.get("name"), str):
            team_name = obj.get("name").strip()
        rk = _extract_rank(obj)
        if team_name and rk is not None:
            out.append((team_name, rk))
        for v in obj.values():
            out.extend(extract_team_ranks(v))
    elif isinstance(obj, list):
        for x in obj:
            out.extend(extract_team_ranks(x))
    return out

def fetch_bpi():
    last_err = None
    for url in URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            ct = (r.headers.get("content-type") or "").lower()
            print(f"Trying ESPN BPI URL: {url}")
            print(f"Status: {r.status_code} Content-Type: {ct}")
            if r.status_code != 200:
                last_err = f"status {r.status_code}"
                continue

            if "json" in ct:
                data = r.json()
            else:
                txt = r.text.strip()
                try:
                    data = json.loads(txt)
                except:
                    last_err = "non-json response"
                    continue

            pairs = extract_team_ranks(data)
            best = {}
            for name, rk in pairs:
                if not name:
                    continue
                if name not in best or rk < best[name]:
                    best[name] = rk

            if len(best) >= 300:
                return best

            last_err = f"parsed {len(best)} teams"
        except Exception as e:
            last_err = str(e)

    return {}

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    if "standard_name" not in alias.columns:
        raise SystemExit("team_alias.csv missing standard_name column")
    if "bpi_name" not in alias.columns:
        alias["bpi_name"] = ""
        alias.to_csv(ALIAS_PATH, index=False)

    src_team_to_rank = fetch_bpi()

    if len(src_team_to_rank) == 0:
        if OUT_PATH.exists():
            try:
                df = pd.read_csv(OUT_PATH)
                if len(df) > 0:
                    print("BPI pull returned 0 teams in this run. Keeping existing data_raw/BPI_Rank.csv and continuing.")
                    return
            except:
                pass
        raise SystemExit("BPI pull returned 0 teams and no prior BPI_Rank.csv exists to fall back on.")

    snapshot_date = date.today().isoformat()

    used = set()
    matched_rows = []
    unmatched_rows = []

    src_names = list(src_team_to_rank.keys())

    for _, ar in alias.iterrows():
        std = str(ar.get("standard_name", "")).strip()
        if not std:
            continue

        desired = str(ar.get("bpi_name", "")).strip() or std

        exact = None
        for nm in src_names:
            if norm(nm) == norm(desired):
                exact = nm
                break

        chosen = None
        score = -1.0
        if exact is not None and exact not in used:
            chosen = exact
            score = 1.0
        else:
            best_nm = None
            best_sc = -1.0
            for nm in src_names:
                if nm in used:
                    continue
                sc = ratio(desired, nm)
                if sc > best_sc:
                    best_sc = sc
                    best_nm = nm
            if best_nm is not None and best_sc >= 0.86:
                chosen = best_nm
                score = best_sc

        if chosen is None:
            best_nm = None
            best_sc = -1.0
            for nm in src_names:
                sc = ratio(desired, nm)
                if sc > best_sc:
                    best_sc = sc
                    best_nm = nm
            unmatched_rows.append(
                {
                    "source_team": desired,
                    "suggested_standard": std,
                    "match_score": round(best_sc if best_sc >= 0 else 0.0, 6),
                }
            )
            continue

        used.add(chosen)
        matched_rows.append(
            {"snapshot_date": snapshot_date, "Team": std, "BPI_Rank": int(src_team_to_rank[chosen])}
        )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "BPI_Rank"])
    out_df.to_csv(OUT_PATH, index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_df.to_csv(UNMATCHED_PATH, index=False)

    print("BPI_Rank.csv")
    print("unmatched_bpi_teams.csv")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")
    print(f"BPI rows {len(out_df)}")

if __name__ == "__main__":
    main()
