import os
import time
from datetime import datetime, timedelta, date

import requests
import pandas as pd

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
OUT_PATH = os.path.join("data_raw", "games_2024_clean_no_ids.csv")

COLS = ["Date", "Team", "Opponent", "Location", "Team_Score", "Opponent_Score", "Win?"]


def fetch_day(d: date):
    datestr = d.strftime("%Y%m%d")
    url = f"{BASE_URL}?dates={datestr}&limit=500"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_game_date(ev):
    d = ev.get("date")
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return d[:10] if isinstance(d, str) and len(d) >= 10 else None


def event_rows(ev, include_incomplete: bool):
    comps = ev.get("competitions", [])
    if not comps:
        return []

    comp = comps[0]
    status = comp.get("status", {}).get("type", {})
    completed = bool(status.get("completed", False))

    competitors = comp.get("competitors", [])
    home = None
    away = None
    for c in competitors:
        if c.get("homeAway") == "home":
            home = c
        elif c.get("homeAway") == "away":
            away = c

    if not home or not away:
        return []

    home_team = home.get("team", {}).get("displayName")
    away_team = away.get("team", {}).get("displayName")
    if not home_team or not away_team:
        return []

    game_date = _parse_game_date(ev)
    if not game_date:
        return []

    neutral = bool(comp.get("neutralSite", False))
    if neutral:
        home_loc = "N"
        away_loc = "N"
    else:
        home_loc = "H"
        away_loc = "A"

    if completed:
        try:
            home_score = int(home.get("score"))
            away_score = int(away.get("score"))
        except Exception:
            return []

        home_win = 1 if home_score > away_score else 0
        away_win = 1 if away_score > home_score else 0

        return [
            {
                "Date": game_date,
                "Team": home_team,
                "Opponent": away_team,
                "Location": home_loc,
                "Team_Score": home_score,
                "Opponent_Score": away_score,
                "Win?": home_win,
            },
            {
                "Date": game_date,
                "Team": away_team,
                "Opponent": home_team,
                "Location": away_loc,
                "Team_Score": away_score,
                "Opponent_Score": home_score,
                "Win?": away_win,
            },
        ]

    if not include_incomplete:
        return []

    return [
        {
            "Date": game_date,
            "Team": home_team,
            "Opponent": away_team,
            "Location": home_loc,
            "Team_Score": "",
            "Opponent_Score": "",
            "Win?": "",
        },
        {
            "Date": game_date,
            "Team": away_team,
            "Opponent": home_team,
            "Location": away_loc,
            "Team_Score": "",
            "Opponent_Score": "",
            "Win?": "",
        },
    ]


def season_end_for_today(today: date) -> date:
    year = today.year + 1 if today.month >= 10 else today.year
    return date(year, 4, 15)


def main():
    os.makedirs("data_raw", exist_ok=True)
    today = date.today()
    season_end = season_end_for_today(today)

    existing = None
    if os.path.exists(OUT_PATH):
        try:
            existing = pd.read_csv(OUT_PATH)
        except Exception:
            existing = None

    if existing is not None and len(existing) > 0 and "Date" in existing.columns:
        dt = pd.to_datetime(existing["Date"], format="%Y-%m-%d", errors="coerce").dt.date
        past_mask = dt.notna() & (dt <= today)
        last_past = dt[past_mask].max() if past_mask.any() else None
        if last_past is None or pd.isna(last_past):
            past_start = today - timedelta(days=14)
        else:
            past_start = last_past - timedelta(days=3)
    else:
        past_start = today - timedelta(days=14)

    if past_start > today:
        past_start = today

    print(f"Past refresh: {past_start.isoformat()} to {today.isoformat()} (completed only)")
    print(f"Future refresh: {today.isoformat()} to {season_end.isoformat()} (include scheduled)")

    new_rows = []

    d = past_start
    while d <= today:
        try:
            data = fetch_day(d)
            events = data.get("events", []) or []
            for ev in events:
                new_rows.extend(event_rows(ev, include_incomplete=False))
        except Exception as e:
            print(f"WARNING: failed on {d.isoformat()} (past): {e}")
        time.sleep(0.15)
        d += timedelta(days=1)

    d = today
    while d <= season_end:
        try:
            data = fetch_day(d)
            events = data.get("events", []) or []
            for ev in events:
                new_rows.extend(event_rows(ev, include_incomplete=True))
        except Exception as e:
            print(f"WARNING: failed on {d.isoformat()} (future): {e}")
        time.sleep(0.15)
        d += timedelta(days=1)

    new_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=COLS)

    if os.path.exists(OUT_PATH):
        old_df = pd.read_csv(OUT_PATH)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    for c in COLS:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined[COLS]

    combined = combined.drop_duplicates(subset=["Date", "Team", "Opponent"], keep="last")

    combined["Date_dt"] = pd.to_datetime(combined["Date"], format="%Y-%m-%d", errors="coerce")
    combined = combined.sort_values(["Date_dt", "Team", "Opponent"]).drop(columns=["Date_dt"]).reset_index(drop=True)

    combined.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(combined)} rows to {os.path.abspath(OUT_PATH)}")


if __name__ == "__main__":
    main()
