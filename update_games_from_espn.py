import argparse
import json
from datetime import datetime, timedelta, date
import sys
import time

import pandas as pd
import requests


# -----------------------------
# Helpers
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=str, default=None, help="YYYY-MM-DD start date (inclusive)")
    p.add_argument("--end", type=str, default=None, help="YYYY-MM-DD end date (inclusive). Default = today")
    p.add_argument("--games", type=str, default="games_2024.csv", help="Games CSV filename")
    p.add_argument("--alias", type=str, default="team_alias.csv", help="Team alias CSV filename")
    return p.parse_args()


def try_parse_date_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fmt_m_d(d: date) -> str:
    # matches your historical "M/D" format (no leading zeros)
    return f"{d.month}/{d.day}"


def normalize_name(s: str) -> str:
    if s is None:
        return ""
    return str(s).strip()


def load_alias_map(alias_csv: str) -> dict:
    """
    Expects columns like:
      standard_name, kenpom_name, bpi_name, net_name, game_log_name

    We'll create a mapping from any non-empty alias -> standard_name.
    """
    try:
        alias_df = pd.read_csv(alias_csv)
    except FileNotFoundError:
        print(f"ERROR: alias file not found: {alias_csv}")
        sys.exit(1)

    alias_df = alias_df.fillna("")

    needed = ["standard_name"]
    for col in needed:
        if col not in alias_df.columns:
            print(f"ERROR: alias file must contain column: {col}")
            sys.exit(1)

    alias_cols = [c for c in alias_df.columns if c != "standard_name"]

    mapping = {}
    for _, row in alias_df.iterrows():
        std = normalize_name(row["standard_name"])
        if not std:
            continue

        # map standard->standard too
        mapping[normalize_name(std).lower()] = std

        for c in alias_cols:
            alias = normalize_name(row.get(c, ""))
            if alias:
                mapping[alias.lower()] = std

    return mapping


def to_standard(name: str, alias_map: dict) -> str:
    n = normalize_name(name)
    if not n:
        return n
    return alias_map.get(n.lower(), n)


def latest_date_in_games_csv(games_csv: str) -> date | None:
    try:
        df = pd.read_csv(games_csv)
    except FileNotFoundError:
        return None

    if "Date" not in df.columns or df.empty:
        return None

    # Dates stored as M/D. We interpret them as current season year by inference is messy,
    # so instead we find the latest *month/day* observed and assume it’s in the same season year.
    # For updater logic, we primarily use explicit --start if user wants backfill.
    # Here, we just return "today - 1" fallback if parse fails.
    try:
        # parse M/D into a dummy year (2000) so we can compare
        md = pd.to_datetime(df["Date"].astype(str), format="%m/%d", errors="coerce")
        md = md.dropna()
        if md.empty:
            return None
        max_md = md.max()
        # convert back to real date using current year (best-effort)
        yr = datetime.today().year
        return date(yr, int(max_md.month), int(max_md.day))
    except Exception:
        return None


def load_existing_games(games_csv: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(games_csv)
    except FileNotFoundError:
        df = pd.DataFrame(columns=["Date", "Team", "Location", "Opponent", "Team_Score", "Opponent_Score", "Win?", "ESPN_Event_ID", "Source"])
    return df


# -----------------------------
# ESPN Fetch
# -----------------------------
def espn_scoreboard_url(d: date) -> str:
    # ESPN college basketball men's scoreboard endpoint
    # date param expects YYYYMMDD
    ymd = d.strftime("%Y%m%d")
    return f"https://site.web.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/scoreboard?dates={ymd}&limit=500"


def fetch_events_for_day(d: date, sleep_s: float = 0.2) -> list:
    url = espn_scoreboard_url(d)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    events = data.get("events", [])
    time.sleep(sleep_s)
    return events


def extract_completed_rows(events: list, cur: date, alias_map: dict) -> list[dict]:
    rows = []

    for ev in events:
        comps = ev.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]

        state = comp.get("status", {}).get("type", {}).get("state", "")
        if state != "post":
            # only completed games
            continue

        competitors = comp.get("competitors", [])
        if len(competitors) != 2:
            continue

        # ESPN always labels one "home" and one "away", even at neutral sites.
        c_home = next((c for c in competitors if c.get("homeAway") == "home"), None)
        c_away = next((c for c in competitors if c.get("homeAway") == "away"), None)
        if not c_home or not c_away:
            continue

        home_name = c_home.get("team", {}).get("shortDisplayName") or c_home.get("team", {}).get("displayName")
        away_name = c_away.get("team", {}).get("shortDisplayName") or c_away.get("team", {}).get("displayName")

        home_score = float(c_home.get("score", 0) or 0)
        away_score = float(c_away.get("score", 0) or 0)

        neutral_site = comp.get("neutralSite", False)

        home_location = "Neutral" if neutral_site else "Home"
        away_location = "Neutral" if neutral_site else "Away"

        event_id = ev.get("id")

        rows.append({
            "Date": fmt_m_d(cur),
            "Team": to_standard(home_name, alias_map),
            "Location": home_location,
            "Opponent": to_standard(away_name, alias_map),
            "Team_Score": home_score,
            "Opponent_Score": away_score,
            "Win?": "Yes" if home_score > away_score else "No",
            "ESPN_Event_ID": event_id,
            "Source": "ESPN",
        })

        rows.append({
            "Date": fmt_m_d(cur),
            "Team": to_standard(away_name, alias_map),
            "Location": away_location,
            "Opponent": to_standard(home_name, alias_map),
            "Team_Score": away_score,
            "Opponent_Score": home_score,
            "Win?": "Yes" if away_score > home_score else "No",
            "ESPN_Event_ID": event_id,
            "Source": "ESPN",
        })

    return rows


# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()

    alias_map = load_alias_map(args.alias)

    today = datetime.today().date()
    end = try_parse_date_yyyy_mm_dd(args.end) if args.end else today

    if args.start:
        start = try_parse_date_yyyy_mm_dd(args.start)
    else:
        # default: continue from latest date in file if possible, else start at today-7
        latest = latest_date_in_games_csv(args.games)
        if latest is None:
            start = today - timedelta(days=7)
        else:
            # pull from next day
            start = latest + timedelta(days=1)

    if start > end:
        print(f"Start date {start} is after end date {end}. Nothing to do.")
        return

    # Load existing
    existing = load_existing_games(args.games)

    # If ESPN_Event_ID missing in existing, create it (as blank) so concat works
    if "ESPN_Event_ID" not in existing.columns:
        existing["ESPN_Event_ID"] = ""
    if "Source" not in existing.columns:
        existing["Source"] = ""

    # Fetch + build rows
    rows = []
    cur = start
    print(f"Will pull from: {start} to {end}")
    while cur <= end:
        try:
            events = fetch_events_for_day(cur)
            print(f"{cur}: events={len(events)}")
            rows.extend(extract_completed_rows(events, cur, alias_map))
        except Exception as e:
            print(f"WARNING: failed on {cur}: {e}")
        cur += timedelta(days=1)

    if not rows:
        print("No new completed games found.")
        return

    new_games = pd.DataFrame(rows)

    # Append + dedupe (by ESPN event + team; because each event has 2 rows)
    combined = pd.concat([existing, new_games], ignore_index=True)

    # Ensure consistent column set
    keep_cols = ["Date", "Team", "Location", "Opponent", "Team_Score", "Opponent_Score", "Win?", "ESPN_Event_ID", "Source"]
    for c in keep_cols:
        if c not in combined.columns:
            combined[c] = ""

    combined = combined[keep_cols]

    # Deduping strategy:
    # - Primary: (ESPN_Event_ID, Team)
    # - Fallback if ESPN_Event_ID blank: (Date, Team, Opponent, Team_Score, Opponent_Score)
    combined["ESPN_Event_ID"] = combined["ESPN_Event_ID"].fillna("").astype(str)

    with_id = combined[combined["ESPN_Event_ID"].str.len() > 0].copy()
    without_id = combined[combined["ESPN_Event_ID"].str.len() == 0].copy()

    with_id = with_id.drop_duplicates(subset=["ESPN_Event_ID", "Team"], keep="last")
    without_id = without_id.drop_duplicates(subset=["Date", "Team", "Opponent", "Team_Score", "Opponent_Score"], keep="last")

    combined = pd.concat([with_id, without_id], ignore_index=True)

    # Save
    combined.to_csv(args.games, index=False)

    print(f"Added {len(new_games)} rows.")
    print(f"{args.games} now has {len(combined)} rows.")
    print("Done.")


if __name__ == "__main__":
    main()

