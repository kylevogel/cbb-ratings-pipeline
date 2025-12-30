import os
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
import requests


BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
OUT_PATH = os.path.join("data_raw", "games_2024_clean_no_ids.csv")

COLS = ["Date", "Team", "Location", "Opponent", "Team_Score", "Opponent_Score", "Win?"]


def season_bounds(today_et: date):
    if today_et.month >= 7:
        start = date(today_et.year, 11, 1)
        end = date(today_et.year + 1, 4, 15)
    else:
        start = date(today_et.year - 1, 11, 1)
        end = date(today_et.year, 4, 15)
    return start, end


def fetch_events_for_day(session: requests.Session, d: date, limit: int = 500):
    datestr = d.strftime("%Y%m%d")
    all_events = []
    offset = 0
    while True:
        params = {"dates": datestr, "limit": limit, "offset": offset}
        r = session.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        events = j.get("events") or []
        if not events:
            break
        all_events.extend(events)
        if len(events) < limit:
            break
        offset += limit
    return all_events


def event_date_iso(ev):
    d = ev.get("date")
    if not d:
        return None
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return d[:10] if isinstance(d, str) and len(d) >= 10 else None


def team_school_name(comp):
    t = (comp.get("team") or {})
    return t.get("location") or t.get("shortDisplayName") or t.get("displayName") or t.get("name") or ""


def parse_event_rows(ev):
    comps = ev.get("competitions") or []
    if not comps:
        return []

    comp = comps[0]
    competitors = comp.get("competitors") or []
    if len(competitors) < 2:
        return []

    home = None
    away = None
    for c in competitors:
        if c.get("homeAway") == "home":
            home = c
        elif c.get("homeAway") == "away":
            away = c
    if home is None or away is None:
        home = competitors[0]
        away = competitors[1]

    neutral = bool(comp.get("neutralSite", False))
    status = (comp.get("status") or {}).get("type") or {}
    completed = bool(status.get("completed", False))

    game_date = event_date_iso(ev)
    if not game_date:
        return []

    home_team = team_school_name(home)
    away_team = team_school_name(away)
    if not home_team or not away_team:
        return []

    def loc(tag):
        if neutral:
            return "N"
        return tag

    def score_int(x):
        s = x.get("score")
        if s is None or str(s).strip() == "":
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    hs = score_int(home)
    as_ = score_int(away)

    if completed and hs is not None and as_ is not None:
        home_win = 1 if hs > as_ else 0
        away_win = 1 if as_ > hs else 0
        return [
            {
                "Date": game_date,
                "Team": home_team,
                "Location": loc("H"),
                "Opponent": away_team,
                "Team_Score": hs,
                "Opponent_Score": as_,
                "Win?": home_win,
            },
            {
                "Date": game_date,
                "Team": away_team,
                "Location": loc("A"),
                "Opponent": home_team,
                "Team_Score": as_,
                "Opponent_Score": hs,
                "Win?": away_win,
            },
        ]

    return [
        {
            "Date": game_date,
            "Team": home_team,
            "Location": loc("H"),
            "Opponent": away_team,
            "Team_Score": "",
            "Opponent_Score": "",
            "Win?": "",
        },
        {
            "Date": game_date,
            "Team": away_team,
            "Location": loc("A"),
            "Opponent": home_team,
            "Team_Score": "",
            "Opponent_Score": "",
            "Win?": "",
        },
    ]


def main():
    os.makedirs("data_raw", exist_ok=True)
    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    season_start, season_end = season_bounds(today_et)

    old = None
    if os.path.exists(OUT_PATH):
        try:
            old = pd.read_csv(OUT_PATH)
        except Exception:
            old = None

    do_full = True
    if old is not None and len(old) > 0 and "Date" in old.columns:
        dt = pd.to_datetime(old["Date"], errors="coerce").dt.date
        min_dt = dt.min() if dt.notna().any() else None
        if min_dt is not None and min_dt <= season_start and len(old) >= 30000:
            do_full = False

    if do_full:
        past_start = season_start
    else:
        past_start = max(season_start, today_et - timedelta(days=7))

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    rows = []

    d = past_start
    while d <= today_et:
        try:
            events = fetch_events_for_day(session, d)
            for ev in events:
                rows.extend(parse_event_rows(ev))
        except Exception:
            pass
        time.sleep(0.12)
        d += timedelta(days=1)

    d = today_et
    while d <= season_end:
        try:
            events = fetch_events_for_day(session, d)
            for ev in events:
                rows.extend(parse_event_rows(ev))
        except Exception:
            pass
        time.sleep(0.12)
        d += timedelta(days=1)

    new_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=COLS)
    for c in COLS:
        if c not in new_df.columns:
            new_df[c] = ""

    if old is not None and len(old) > 0:
        for c in COLS:
            if c not in old.columns:
                old[c] = ""
        combined = pd.concat([old[COLS], new_df[COLS]], ignore_index=True)
    else:
        combined = new_df[COLS].copy()

    combined = combined.drop_duplicates(subset=["Date", "Team", "Location", "Opponent"], keep="last")
    combined["Date_dt"] = pd.to_datetime(combined["Date"], errors="coerce")
    combined = combined.sort_values(["Date_dt", "Team", "Opponent", "Location"]).drop(columns=["Date_dt"]).reset_index(drop=True)

    combined.to_csv(OUT_PATH, index=False)


if __name__ == "__main__":
    main()
