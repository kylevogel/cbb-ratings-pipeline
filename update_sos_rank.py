import os
import re
import datetime as dt
from io import StringIO
from pathlib import Path
import difflib
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent
DATA_RAW = ROOT / "data_raw"
ALIAS_PATH = ROOT / "team_alias.csv"

def _session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.warrennolan.com/",
    })
    return s

def _canon(s):
    s = str(s or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _best_guess(src, candidates):
    src_c = _canon(src)
    best = ("", 0.0)
    for c in candidates:
        c_c = _canon(c)
        if not c_c:
            continue
        score = difflib.SequenceMatcher(None, src_c, c_c).ratio()
        if score > best[1]:
            best = (c, score)
    return best

def _load_alias():
    if not ALIAS_PATH.exists():
        raise SystemExit("team_alias.csv not found at repo root")
    df = pd.read_csv(ALIAS_PATH, dtype=str).fillna("")
    if "standard_name" not in df.columns:
        raise SystemExit("team_alias.csv missing standard_name column")
    for c in ["kenpom_name", "bpi_name", "net_name", "game_log_name"]:
        if c not in df.columns:
            df[c] = ""
    return df

def _alias_maps(alias_df):
    stds = alias_df["standard_name"].astype(str).str.strip().tolist()
    std_lower = {s.lower(): s for s in stds if s}

    cand_cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
    cand_lower = {}
    canon_to_stds = {}

    all_candidates = []
    for _, r in alias_df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        for c in cand_cols:
            v = str(r.get(c, "")).strip()
            if not v:
                continue
            all_candidates.append(v)
            cand_lower[v.lower()] = std
            k = _canon(v)
            if k:
                canon_to_stds.setdefault(k, set()).add(std)

    for k in list(canon_to_stds.keys()):
        canon_to_stds[k] = sorted(list(canon_to_stds[k]))

    return std_lower, cand_lower, canon_to_stds, sorted(set(all_candidates), key=lambda x: x.lower())

def _pick_table(html):
    tables = pd.read_html(StringIO(html))
    best = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("rank" == c or c.startswith("rank") or c == "rk" for c in cols) and any("team" in c for c in cols):
            best = t
            break
    if best is None:
        raise SystemExit("Could not find SoS table (rank/team) on WarrenNolan page")
    return best

def _rank_col(cols):
    for c in cols:
        cl = str(c).strip().lower()
        if cl == "rank" or cl.startswith("rank") or cl == "rk":
            return c
    return cols[0]

def _team_col(cols):
    for c in cols:
        cl = str(c).strip().lower()
        if "team" in cl:
            return c
    return cols[1] if len(cols) > 1 else cols[0]

def _fetch_sos(season, s):
    url = f"https://www.warrennolan.com/basketball/{season}/sos-rpi-predict"
    r = s.get(url, timeout=30)
    if r.status_code != 200 or not r.text:
        raise SystemExit(f"Failed to fetch WarrenNolan SoS page ({r.status_code})")
    t = _pick_table(r.text)
    cols = list(t.columns)
    rc = _rank_col(cols)
    tc = _team_col(cols)

    df = t[[rc, tc]].copy()
    df.columns = ["Rank", "Team"]
    df["Team"] = df["Team"].astype(str).str.strip()
    df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")
    df = df.dropna(subset=["Rank", "Team"])
    df["Rank"] = df["Rank"].astype(int)
    df = df[df["Team"].astype(str).str.len() > 0]
    df = df.sort_values("Rank").reset_index(drop=True)
    return df

def _match_team(src_team, std_lower, cand_lower, canon_to_stds, all_candidates):
    src = str(src_team or "").strip()
    if not src:
        return None, None, 0.0

    overrides = {
        "boston college": "Boston College",
        "boston university": "Boston University",
        "detroit": "Detroit Mercy",
        "cal state northridge": "CSUN",
        "csun": "CSUN",
        "uncg": "UNC Greensboro",
        "uta": "UT Arlington",
        "umass": "Massachusetts",
        "umkc": "Kansas City",
        "loyola md": "Loyola Maryland",
        "loyola marymount": "Loyola Marymount",
        "saint marys": "Saint Mary's",
        "saint johns": "St. John's",
    }
    o = overrides.get(src.lower())
    if o:
        return o, src, 1.0

    exact_std = std_lower.get(src.lower())
    if exact_std:
        return exact_std, src, 1.0

    exact_cand = cand_lower.get(src.lower())
    if exact_cand:
        return exact_cand, src, 1.0

    k = _canon(src)
    if k in canon_to_stds:
        choices = canon_to_stds[k]
        if len(choices) == 1:
            return choices[0], src, 0.99
        best = ("", 0.0)
        for ch in choices:
            score = difflib.SequenceMatcher(None, _canon(src), _canon(ch)).ratio()
            if score > best[1]:
                best = (ch, score)
        if best[0]:
            return best[0], src, best[1]

    guess, score = _best_guess(src, all_candidates)
    if guess and score >= 0.82:
        return cand_lower.get(guess.lower(), None), guess, score

    return None, guess, score

def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    season = os.environ.get("SEASON", "").strip()
    if not season:
        season = "2026"

    alias_df = _load_alias()
    std_lower, cand_lower, canon_to_stds, all_candidates = _alias_maps(alias_df)

    s = _session()
    src_df = _fetch_sos(season, s)

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()

    out_rows = []
    unmatched = []

    for _, r in src_df.iterrows():
        src_team = str(r["Team"]).strip()
        rank = int(r["Rank"])
        std, used, score = _match_team(src_team, std_lower, cand_lower, canon_to_stds, all_candidates)
        if std:
            out_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": rank})
        else:
            unmatched.append({"source_team": src_team, "suggested_standard": (used or ""), "match_score": float(score)})

    out = pd.DataFrame(out_rows)
    out = out.sort_values("SoS").drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)

    out_path = DATA_RAW / "SOS_Rank.csv"
    out.to_csv(out_path, index=False)

    um_path = DATA_RAW / "unmatched_sos_teams.csv"
    pd.DataFrame(unmatched).to_csv(um_path, index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("Pulled source teams:", len(src_df))
    print("Matched:", len(out))
    print("Unmatched:", len(unmatched))

if __name__ == "__main__":
    main()
