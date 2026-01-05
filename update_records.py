#!/usr/bin/env python3
"""
Fetch team records from ESPN.
Outputs: data_raw/team_records.csv
"""

import os
from datetime import datetime
import requests
import pandas as pd


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def _season_end_year() -> int:
    now = datetime.now()
    return now.year + 1 if now.month >= 7 else now.year


def _get_json(url: str, params: dict | None = None) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed: {url} params={params} err={e}")
        return None


def _pick_team_name(team: dict) -> str:
    for k in ("shortDisplayName", "location", "displayName", "name"):
        v = team.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_record_from_stats(stats: list) -> str:
    if not isinstance(stats, list):
        return ""
    preferred_names = {"overall", "overallrecord", "total", "record"}
    for s in stats:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name", "")).lower().strip()
        if name in preferred_names:
            v = s.get("displayValue") or s.get("value")
            if isinstance(v, str) and v.strip():
                return v.strip()
    for s in stats:
        if not isinstance(s, dict):
            continue
        desc = str(s.get("description", "")).lower()
        abbr = str(s.get("abbreviation", "")).lower()
        if "overall" in desc or abbr in {"ovr", "overall"}:
            v = s.get("displayValue") or s.get("value")
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _iter_entries(obj):
    if isinstance(obj, dict):
        if isinstance(obj.get("entries"), list):
            for e in obj["entries"]:
                yield e
        for v in obj.values():
            yield from _iter_entries(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_entries(v)


def fetch_records_from_standings(season: int) -> pd.DataFrame | None:
    urls = [
        "https://site.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/standings",
        "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/standings",
    ]
    params_list = [
        {"season": season},
        {"season": season, "group": 50},
        {"season": season, "groups": 50},
        {"group": 50},
        {"groups": 50},
        None,
    ]

    for url in urls:
        for params in params_list:
            data = _get_json(url, params=params)
            if not isinstance(data, dict):
                continue

            rows = []
            for entry in _iter_entries(data):
                if not isinstance(entry, dict):
                    continue
                team = entry.get("team", {})
                if not isinstance(team, dict):
                    continue

                team_id = str(team.get("id", "")).strip()
                team_name = _pick_team_name(team)
                display_name = str(team.get("displayName", team_name)).strip()

                record = _extract_record_from_stats(entry.get("stats", []))
                if not record:
                    record = str(entry.get("record", "")).strip()

                if team_id and team_name:
                    rows.append(
                        {
                            "team_id": team_id,
                            "team_espn": team_name,
                            "display_name": display_name,
                            "record": record,
                        }
                    )

            if rows:
                df = pd.DataFrame(rows)
                df = df.drop_duplicates(subset=["team_id"])
                return df

    return None


def fetch_team_list(season: int) -> pd.DataFrame | None:
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    params = {"groups": 50, "limit": 400, "season": season}
    data = _get_json(url, params=params)
    if not isinstance(data, dict):
        return None

    teams = []
    for sport in data.get("sports", []) if isinstance(data.get("sports"), list) else []:
        for league in sport.get("leagues", []) if isinstance(sport.get("leagues"), list) else []:
            for t in league.get("teams", []) if isinstance(league.get("teams"), list) else []:
                team = t.get("team", {})
                if not isinstance(team, dict):
                    continue
                team_id = str(team.get("id", "")).strip()
                team_name = _pick_team_name(team)
                display_name = str(team.get("displayName", team_name)).strip()
                if team_id and team_name:
                    teams.append(
                        {
                            "team_id": team_id,
                            "team_espn": team_name,
                            "display_name": display_name,
                        }
                    )

    if not teams:
        return None

    df = pd.DataFrame(teams).drop_duplicates(subset=["team_id"])
    return df


def fetch_records_from_rankings(season: int) -> pd.DataFrame | None:
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/rankings"
    params = {"season": season}
    data = _get_json(url, params=params)
    if not isinstance(data, dict):
        return None

    rows = []
    for ranking in data.get("rankings", []) if isinstance(data.get("rankings"), list) else []:
        for rank_entry in ranking.get("ranks", []) if isinstance(ranking.get("ranks"), list) else []:
            team = rank_entry.get("team", {})
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id", "")).strip()
            team_name = _pick_team_name(team)
            display_name = str(team.get("displayName", team_name)).strip()
            record = str(rank_entry.get("recordSummary", "")).strip()
            if team_id and team_name:
                rows.append(
                    {
                        "team_id": team_id,
                        "team_espn": team_name,
                        "display_name": display_name,
                        "record": record,
                    }
                )

        for other in ranking.get("others", []) if isinstance(ranking.get("others"), list) else []:
            team = other.get("team", {})
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id", "")).strip()
            team_name = _pick_team_name(team)
            display_name = str(team.get("displayName", team_name)).strip()
            record = str(other.get("recordSummary", "")).strip()
            if team_id and team_name:
                rows.append(
                    {
                        "team_id": team_id,
                        "team_espn": team_name,
                        "display_name": display_name,
                        "record": record,
                    }
                )

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df["record"] = df["record"].fillna("")
    df = df.drop_duplicates(subset=["team_id"])
    return df


def main():
    season = _season_end_year()
    print(f"Fetching team records from ESPN (season={season})...")

    df = fetch_records_from_standings(season)
    if df is not None:
        print(f"Standings returned {len(df)} teams")
        nonempty = df["record"].fillna("").str.strip().ne("")
        print(f"Teams with non-empty record: {int(nonempty.sum())}")
        if len(df) >= 300 and int(nonempty.sum()) >= 250:
            os.makedirs("data_raw", exist_ok=True)
            df = df.drop_duplicates(subset=["team_id"])
            df.to_csv("data_raw/team_records.csv", index=False)
            print("Saved data_raw/team_records.csv")
            return
        print("Standings data looked incomplete, falling back...")

    df_teams = fetch_team_list(season)
    df_rank = fetch_records_from_rankings(season)

    if df_teams is None and df_rank is None:
        print("Failed to fetch team list and rankings records.")
        return

    if df_teams is None:
        df_out = df_rank.copy()
    elif df_rank is None:
        df_out = df_teams.copy()
        df_out["record"] = ""
    else:
        df_out = df_teams.merge(
            df_rank[["team_id", "record"]],
            on="team_id",
            how="left",
        )
        df_out["record"] = df_out["record"].fillna("")

    df_out = df_out.drop_duplicates(subset=["team_id"])
    os.makedirs("data_raw", exist_ok=True)
    df_out.to_csv("data_raw/team_records.csv", index=False)

    nonempty = df_out["record"].fillna("").str.strip().ne("")
    print(f"Saved {len(df_out)} rows to data_raw/team_records.csv")
    print(f"Teams with non-empty record: {int(nonempty.sum())}")
    print(df_out[nonempty].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
