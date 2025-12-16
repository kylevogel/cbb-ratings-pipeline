import os
import time
from datetime import datetime, timedelta, date
import requests
import pandas as pd

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
OUT_PATH = os.path.join("data_raw", "games_2024_clean_no_ids.csv")

def fetch_day(d: date):
    datestr = d.strftime("%Y%m%d")
    url = f"{BASE_URL}?dates={datestr}&limit=500"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def event_rows(ev):
    comps = ev.get("competitions", [])
    if not comps:
        return []
    comp = comps[0]
    status = comp.get("status", {}).get("type", {})
    if not bool(status.get("completed", False)):
        return []

    neutral = bool(comp.get("neutralSite", False))

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

    try:
        home_score = int(home.get("score"))
        away_score = int(away.get("score"))
    except Exception:
        return []

    d = ev.get("date")
    try:
        game_date = datetime.fromisoformat(d.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        game_date = d[:10] if isinstance(d, str) and len(d) >= 10 else None
    if not game_date:
        return []

    if neutral:
        home_loc = "N"
        away_loc = "N"
    else:
        home_loc = "H"
        away_loc = "A"

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

def main():
    os.makedirs("data_raw", exist_ok=True)

    end = date.today()

    if os.path.exists(OUT_PATH):
        existing = pd.read_csv(OUT_PATH)
        if len(existing) > 0 and "Date" in existing.columns:
            existing["Date"] = pd.to_datetime(existing["Date"], format="%Y-%m-%d", errors="coerce")
            last_date = existing["Date"].max()
            if pd.isna(last_date):
                start = end - timedelta(days=7)
            else:
                start = last_date.date() - timedelta(days=3)
        else:
            start = end - timedelta(days=7)
    else:
        start = end - timedelta(days=14)

    if start > end:
        start = end

    print(f"Will pull from: {start.isoformat()} to {end.isoformat()}")

    new_rows = []
    d = start
    while d <= end:
        try:
            data = fetch_day(d)
            events = data.get("events", []) or []
            for ev in events:
                new_rows.extend(event_rows(ev))
        except Exception as e:
            print(f"WARNING: failed on {d.isoformat()}: {e}")
        time.sleep(0.2)
        d += timedelta(days=1)

    if not new_rows and os.path.exists(OUT_PATH):
        print("No new completed games found.")
        return

    new_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=["Date","Team","Opponent","Location","Team_Score","Opponent_Score","Win?"])

    if os.path.exists(OUT_PATH):
        old_df = pd.read_csv(OUT_PATH)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    else:
        combined = new_df

    cols = ["Date","Team","Opponent","Location","Team_Score","Opponent_Score","Win?"]
    for c in cols:
        if c not in combined.columns:
            combined[c] = pd.NA
    combined = combined[cols]

    combined = combined.drop_duplicates(subset=["Date","Team","Opponent"], keep="last")
    combined["Date_dt"] = pd.to_datetime(combined["Date"], format="%Y-%m-%d", errors="coerce")
    combined = combined.sort_values(["Date_dt","Team","Opponent"]).drop(columns=["Date_dt"]).reset_index(drop=True)

    combined.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(combined)} rows to {os.path.abspath(OUT_PATH)}")

if __name__ == "__main__":
    main()
