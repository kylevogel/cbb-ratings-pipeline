import os
import re
import unicodedata
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
import requests


BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def _norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = s.replace("&", "and")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _load_alias_map(data_dir: str) -> dict:
    candidates = [
        os.path.join(data_dir, "team_alias_map.csv"),
        os.path.join(data_dir, "team_aliases.csv"),
        os.path.join(data_dir, "aliases.csv"),
    ]
    path = next((p for p in candidates if os.path.exists(p)), None)
    if not path:
        return {}
    df = pd.read_csv(path)
    cols = [c.lower().strip() for c in df.columns]

    def col(name_options):
        for opt in name_options:
            if opt in cols:
                return df.columns[cols.index(opt)]
        return None

    alias_col = col(["alias", "from", "alt", "name", "key"])
    canon_col = col(["canonical", "to", "team", "standard"])
    if not alias_col or not canon_col:
        return {}

    out = {}
    for a, c in zip(df[alias_col].astype(str), df[canon_col].astype(str)):
        ak = _norm(a)
        ck = _norm(c)
        if ak and ck:
            out[ak] = ck
    return out


def _canon_key(name: str, alias_map: dict) -> str:
    k = _norm(name)
    return alias_map.get(k, k)


def _find_net_file(data_dir: str) -> str:
    candidates = [
        os.path.join(data_dir, "NET_Rank.csv"),
        os.path.join(data_dir, "net_rank.csv"),
        os.path.join(data_dir, "net_rankings.csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError("Could not find NET file (expected NET_Rank.csv in data_raw/).")


def _load_net_teams(data_dir: str, alias_map: dict) -> tuple[set, dict]:
    net_path = _find_net_file(data_dir)
    df = pd.read_csv(net_path)
    team_col = None
    for c in df.columns:
        if c.lower().strip() in {"team", "school", "name"}:
            team_col = c
            break
    if team_col is None:
        team_col = df.columns[0]
    keys = set()
    display = {}
    for t in df[team_col].astype(str).tolist():
        k = _canon_key(t, alias_map)
        if k:
            keys.add(k)
            display[k] = t
    return keys, display


def _season_bounds_eastern(today_et: date) -> tuple[date, date, int]:
    end_year = today_et.year + 1 if today_et.month >= 7 else today_et.year
    start = date(end_year - 1, 11, 1)
    end = date(end_year, 4, 15)
    return start, end, end_year


def _iter_dates(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def _fetch_scoreboard_day(day: date, session: requests.Session, limit: int = 500) -> list[dict]:
    all_events = []
    offset = 0
    dates = day.strftime("%Y%m%d")

    while True:
        params = {"dates": dates, "limit": limit, "offset": offset}
        r = session.get(BASE, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        events = j.get("events", []) or []
        if not events:
            break
        all_events.extend(events)
        if len(events) < limit:
            break
        offset += limit

    return all_events


def _parse_events(events: list[dict], alias_map: dict, net_keys: set) -> pd.DataFrame:
    rows = []
    for ev in events:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            continue

        neutral = bool(comp.get("neutralSite", False))
        status = (((ev.get("status") or {}).get("type") or {}).get("state")) or ""
        ev_dt = ev.get("date") or ""
        try:
            dt = pd.to_datetime(ev_dt, utc=True).date()
        except Exception:
            continue

        def side(x):
            team = (x.get("team") or {})
            name = team.get("displayName") or team.get("shortDisplayName") or team.get("name") or ""
            homeaway = x.get("homeAway") or ""
            score = x.get("score")
            try:
                score_val = int(score) if score is not None and str(score).strip() != "" else None
            except Exception:
                score_val = None
            return name, homeaway, score_val

        n1, ha1, s1 = side(competitors[0])
        n2, ha2, s2 = side(competitors[1])

        if not n1 or not n2:
            continue

        def loc(homeaway: str) -> str:
            if neutral:
                return "N"
            if homeaway == "home":
                return "H"
            if homeaway == "away":
                return "A"
            return ""

        def add_row(team_name, team_ha, team_score, opp_name, opp_score):
            tk = _canon_key(team_name, alias_map)
            ok = _canon_key(opp_name, alias_map)
            if tk not in net_keys:
                return
            win = None
            if status == "post" and team_score is not None and opp_score is not None:
                win = "W" if team_score > opp_score else ("L" if team_score < opp_score else None)
            rows.append(
                {
                    "Date": dt.isoformat(),
                    "Team": team_name,
                    "TeamKey": tk,
                    "Location": loc(team_ha),
                    "Opponent": opp_name,
                    "OpponentKey": ok,
                    "Team_Score": team_score if team_score is not None else "",
                    "Opponent_Score": opp_score if opp_score is not None else "",
                    "Win?": win if win is not None else "",
                    "Status": status,
                }
            )

        add_row(n1, ha1, s1, n2, s2)
        add_row(n2, ha2, s2, n1, s1)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
    return df


def _pick_games_output_path(data_dir: str) -> str:
    candidates = [
        os.path.join(data_dir, "games_2024_clean_no_ids.csv"),
        os.path.join(data_dir, "games_2024.csv"),
        os.path.join(data_dir, "games.csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return candidates[0]


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(root, "data_raw")
    os.makedirs(data_dir, exist_ok=True)
    cache_dir = os.path.join(root, ".cache")
    os.makedirs(cache_dir, exist_ok=True)

    alias_map = _load_alias_map(data_dir)
    net_keys, _ = _load_net_teams(data_dir, alias_map)

    now_et = datetime.now(ZoneInfo("America/New_York")).date()
    season_start, season_end, season_end_year = _season_bounds_eastern(now_et)

    marker = os.path.join(cache_dir, f"espn_full_backfill_{season_end_year}.txt")
    full_backfill = not os.path.exists(marker)

    if full_backfill:
        d0 = season_start
    else:
        d0 = max(season_start, now_et - timedelta(days=10))

    d1 = season_end

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    all_days = []
    for d in _iter_dates(d0, d1):
        try:
            events = _fetch_scoreboard_day(d, session=session, limit=500)
            df_day = _parse_events(events, alias_map, net_keys)
            if not df_day.empty:
                all_days.append(df_day)
        except Exception:
            continue

    new_df = pd.concat(all_days, ignore_index=True) if all_days else pd.DataFrame()

    out_path = _pick_games_output_path(data_dir)

    if os.path.exists(out_path):
        try:
            old = pd.read_csv(out_path)
        except Exception:
            old = pd.DataFrame()
    else:
        old = pd.DataFrame()

    if not new_df.empty:
        keep_cols = ["Date", "Team", "Location", "Opponent", "Team_Score", "Opponent_Score", "Win?"]
        merged = pd.concat([old, new_df[keep_cols]], ignore_index=True) if not old.empty else new_df[keep_cols].copy()

        for c in ["Team_Score", "Opponent_Score"]:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")
        merged["has_score"] = (~merged["Team_Score"].isna()) & (~merged["Opponent_Score"].isna())
        merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce").dt.date.astype(str)

        merged = merged.sort_values(["Date", "Team", "Opponent", "Location", "has_score"])
        merged = merged.drop_duplicates(subset=["Date", "Team", "Opponent", "Location"], keep="last")
        merged = merged.drop(columns=["has_score"])

        merged["Team_Score"] = merged["Team_Score"].fillna("").astype(str).replace({".0": ""}, regex=True)
        merged["Opponent_Score"] = merged["Opponent_Score"].fillna("").astype(str).replace({".0": ""}, regex=True)

        merged.to_csv(out_path, index=False)

    if full_backfill:
        with open(marker, "w", encoding="utf-8") as f:
            f.write(datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()
