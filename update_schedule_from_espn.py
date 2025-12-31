from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import argparse
import time

import pandas as pd
import requests


SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def _season_window(today: date) -> tuple[date, date]:
    start_year = today.year if today.month >= 7 else today.year - 1
    start = date(start_year, 11, 1)
    end = date(start_year + 1, 4, 15)
    return start, end


def _dateranges(start: date, end: date, chunk_days: int):
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def _get_json_with_retry(session: requests.Session, params: dict, tries: int = 5):
    last_err = None
    for i in range(tries):
        try:
            r = session.get(SCOREBOARD_URL, params=params, timeout=(10, 20))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8 * (2**i))
    raise last_err


def _extract_rows(payload: dict):
    events = payload.get("events") or []
    rows = []
    for ev in events:
        dt_raw = ev.get("date")
        if not dt_raw:
            continue
        try:
            dt = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
        except Exception:
            continue
        dstr = dt.date().isoformat()

        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]

        neutral = bool(comp.get("neutralSite"))
        competitors = comp.get("competitors") or []
        if not competitors:
            continue

        home = None
        away = None
        for c in competitors:
            ha = (c.get("homeAway") or "").lower()
            if ha == "home":
                home = c
            elif ha == "away":
                away = c

        if not home or not away:
            continue

        home_team = ((home.get("team") or {}).get("displayName")) or ""
        away_team = ((away.get("team") or {}).get("displayName")) or ""
        if not home_team or not away_team:
            continue

        rows.append(
            {
                "Date": dstr,
                "Team": home_team,
                "Location": "Neutral" if neutral else "Home",
                "Opponent": away_team,
            }
        )
        rows.append(
            {
                "Date": dstr,
                "Team": away_team,
                "Location": "Neutral" if neutral else "Away",
                "Opponent": home_team,
            }
        )
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--chunk-days", type=int, default=7)
    args = ap.parse_args()

    today = date.today()
    default_start, default_end = _season_window(today)

    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else default_start
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else default_end
    chunk_days = max(1, int(args.chunk_days))

    out_dir = Path("data_raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "schedule_full.csv"

    session = _make_session()

    seen = set()
    all_rows = []

    for a, b in _dateranges(start, end, chunk_days):
        dates_val = a.strftime("%Y%m%d") if a == b else f"{a.strftime('%Y%m%d')}-{b.strftime('%Y%m%d')}"
        params = {"dates": dates_val, "groups": 50, "limit": 5000}
        payload = _get_json_with_retry(session, params=params, tries=5)
        rows = _extract_rows(payload)
        for r in rows:
            k = (r["Date"], r["Team"], r["Opponent"])
            if k in seen:
                continue
            seen.add(k)
            all_rows.append(r)

    df = pd.DataFrame(all_rows)
    if df.empty:
        df = pd.DataFrame(columns=["Date", "Team", "Location", "Opponent"])
    else:
        df = df.sort_values(["Date", "Team", "Opponent"], kind="mergesort").reset_index(drop=True)

    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
