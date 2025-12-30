import os
import time
import re
import unicodedata
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
import requests


BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
OUT_PATH = os.path.join("data_raw", "games_2024_clean_no_ids.csv")
CACHE_DIR = ".cache"

COLS = ["Date", "Team", "Location", "Opponent", "Team_Score", "Opponent_Score", "Win?"]


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


def _season_bounds(today_et: date):
    if today_et.month >= 7:
        start = date(today_et.year, 11, 1)
        end = date(today_et.year + 1, 4, 15)
        season_tag = today_et.year + 1
    else:
        start = date(today_et.year - 1, 11, 1)
        end = date(today_et.year, 4, 15)
        season_tag = today_et.year
    return start, end, season_tag


def _fetch_events_for_day(session: requests.Session, d: date, limit: int = 500):
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


def _event_date_iso(ev):
    d = ev.get("date")
    if not d:
        return None
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return d[:10] if isinstance(d, str) and len(d) >= 10 else None


def _team_school_name(comp):
    t = (comp.get("team") or {})
    return t.get("location") or t.get("shortDisplayName") or t.get("displayName") or t.get("name") or ""


def _parse_event_rows(ev):
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

    game_date = _event_date_iso(ev)
    if not game_date:
        return []

    home_team = _team_school_name(home)
    away_team = _team_school_name(away)
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


def _read_existing():
    if not os.path.exists(OUT_PATH):
        return pd.DataFrame(columns=COLS)
    try:
        df = pd.read_csv(OUT_PATH)
    except Exception:
        return pd.DataFrame(columns=COLS)
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    return df[COLS].copy()


def _should_full_backfill(season_tag: int):
    os.makedirs(CACHE_DIR, exist_ok=True)
    marker = os.path.join(CACHE_DIR, f"espn_full_backfill_{season_tag}.txt")
    if os.path.exists(marker):
        return False, marker
    return True, marker


def _mark_done(marker: str):
    with open(marker, "w", encoding="utf-8") as f:
        f.write(datetime.utcnow().isoformat())


def main():
    os.makedirs("data_raw", exist_ok=True)

    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    season_start, season_end, season_tag = _season_bounds(today_et)

    existing = _read_existing()

    # SAFE min date check (no mixed types)
    dt_series = pd.to_datetime(existing["Date"], errors="coerce")
    dt_series = dt_series[dt_series.notna()]
    existing_min = dt_series.min() if len(dt_series) else None

    full_backfill, marker = _should_full_backfill(season_tag)

    if full_backfill:
        start_past = season_start
    else:
        start_past = max(season_start, today_et - timedelta(days=7))

        # if file is obviously incomplete, force a backfill anyway
        if existing_min is None or existing_min.date() > season_start:
            start_past = season_start

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    rows = []

    # past -> today
    d = start_past
    while d <= today_et:
        try:
            events = _fetch_events_for_day(session, d)
            for ev in events:
                rows.extend(_parse_event_rows(ev))
        except Exception:
            pass
        time.sleep(0.12)
        d += timedelta(days=1)

    # today -> season end (future schedule)
    d = today_et
    while d <= season_end:
        try:
            events = _fetch_events_for_day(session, d)
            for ev in events:
                rows.extend(_parse_event_rows(ev))
        except Exception:
            pass
        time.sleep(0.12)
        d += timedelta(days=1)

    new_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=COLS)
    for c in COLS:
        if c not in new_df.columns:
            new_df[c] = ""

    combined = pd.concat([existing, new_df[COLS]], ignore_index=True)

    # keep latest row if duplicated (scheduled row can later become scored row)
    combined = combined.drop_duplicates(subset=["Date", "Team", "Location", "Opponent"], keep="last")
    combined["Date_dt"] = pd.to_datetime(combined["Date"], errors="coerce")
    combined = combined.sort_values(["Date_dt", "Team", "Opponent", "Location"]).drop(columns=["Date_dt"]).reset_index(drop=True)

    combined.to_csv(OUT_PATH, index=False)

    if full_backfill:
        _mark_done(marker)


if __name__ == "__main__":
    main()
