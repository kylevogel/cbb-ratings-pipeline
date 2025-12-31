import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


def _season_bounds(today: date):
    season_year = today.year if today.month >= 7 else today.year - 1
    start = date(season_year, 11, 1)
    end = date(season_year + 1, 4, 15)
    return start, end


def _iter_dates(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def _fetch_scoreboard(d: date):
    ds = d.strftime("%Y%m%d")
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
    params = {"dates": ds, "groups": "50", "limit": "500"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _parse_rows(payload):
    out = []
    events = payload.get("events") or []
    for ev in events:
        ev_date = ev.get("date") or ""
        try:
            dt = datetime.fromisoformat(ev_date.replace("Z", "+00:00"))
            dstr = dt.date().isoformat()
        except Exception:
            dstr = ev_date[:10] if len(ev_date) >= 10 else ""

        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            continue

        neutral = bool(comp.get("neutralSite"))
        status = ((comp.get("status") or {}).get("type") or {})
        state = status.get("state") or ""
        completed = bool(status.get("completed"))

        def team_name(c):
            t = c.get("team") or {}
            return t.get("displayName") or t.get("name") or ""

        def score_val(c):
            s = c.get("score")
            try:
                return int(s)
            except Exception:
                return ""

        c1, c2 = competitors[0], competitors[1]
        for a, b in [(c1, c2), (c2, c1)]:
            ha = a.get("homeAway") or ""
            if neutral:
                loc = "Neutral"
            else:
                loc = "Home" if ha == "home" else ("Away" if ha == "away" else "")

            out.append(
                {
                    "Date": dstr,
                    "Team": team_name(a),
                    "Location": loc,
                    "Opponent": team_name(b),
                    "Status": "Final" if completed else state,
                    "Team_Score": score_val(a) if completed else "",
                    "Opponent_Score": score_val(b) if completed else "",
                }
            )

    return out


def main():
    out_path = Path("data_raw") / "schedule_full.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    start, end = _season_bounds(date.today())
    rows = []

    for d in _iter_dates(start, end):
        try:
            payload = _fetch_scoreboard(d)
            rows.extend(_parse_rows(payload))
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["Date", "Team", "Location", "Opponent", "Status", "Team_Score", "Opponent_Score"])

    df = df.dropna(subset=["Team", "Opponent"])
    df = df[df["Team"].astype(str).str.len() > 0]
    df = df[df["Opponent"].astype(str).str.len() > 0]

    df.to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
