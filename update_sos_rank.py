import os
from pathlib import Path
from datetime import datetime, timezone
from difflib import SequenceMatcher

import pandas as pd
import requests
from bs4 import BeautifulSoup


SEASON = int(os.getenv("SEASON", "2026"))
URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"

DATA_RAW = Path("data_raw")
TEAM_ALIAS = Path("team_alias.csv")


def _clean(s: str) -> str:
    return str(s).replace("\xa0", " ").strip()


def _ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _pick_table_rows(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    best = None
    best_score = -1

    for t in tables:
        thead = t.find("thead")
        if not thead:
            continue
        hdr_cells = thead.find_all(["th", "td"])
        hdr = [_clean(c.get_text(" ", strip=True)) for c in hdr_cells]
        hset = {h.lower() for h in hdr if h}
        score = 0
        if "rank" in hset:
            score += 3
        if "team" in hset:
            score += 3
        if "sos" in hset or "strength of schedule" in hset:
            score += 1
        if score > best_score:
            best_score = score
            best = t

    if best is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    tbody = best.find("tbody")
    if not tbody:
        return pd.DataFrame(columns=["Rank", "Team"])

    rows = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue
        r = _clean(tds[0].get_text(" ", strip=True))
        team = _clean(tds[1].get_text(" ", strip=True))
        if not r or not team:
            continue
        try:
            r_int = int(str(r).replace("#", "").strip())
        except Exception:
            continue
        rows.append((r_int, team))

    df = pd.DataFrame(rows, columns=["Rank", "Team"]).dropna()
    if not df.empty:
        df = df.sort_values("Rank").drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)
    return df


def _fallback_read_html(html: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(html)
    except Exception:
        return pd.DataFrame(columns=["Rank", "Team"])

    best = None
    best_score = -1

    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        lcols = {c.lower() for c in cols}
        score = 0
        if "rank" in lcols:
            score += 3
        if "team" in lcols:
            score += 3
        if "sos" in lcols:
            score += 1
        if score > best_score:
            best_score = score
            best = t

    if best is None:
        return pd.DataFrame(columns=["Rank", "Team"])

    cols_map = {str(c).strip().lower(): c for c in best.columns}
    if "rank" not in cols_map or "team" not in cols_map:
        return pd.DataFrame(columns=["Rank", "Team"])

    out = best[[cols_map["rank"], cols_map["team"]]].copy()
    out.columns = ["Rank", "Team"]
    out["Team"] = out["Team"].astype(str).map(_clean)
    out["Rank"] = out["Rank"].astype(str).str.replace("#", "", regex=False).str.strip()
    out = out[out["Rank"].str.match(r"^\d+$", na=False)].copy()
    out["Rank"] = out["Rank"].astype(int)
    out = out.sort_values("Rank").drop_duplicates(subset=["Team"], keep="first").reset_index(drop=True)
    return out


def fetch_source() -> pd.DataFrame:
    r = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    html = r.text

    df = _pick_table_rows(html)
    if len(df) >= 360:
        return df

    df2 = _fallback_read_html(html)
    if len(df2) > len(df):
        return df2

    return df


def build_alias() -> pd.DataFrame:
    if not TEAM_ALIAS.exists():
        raise SystemExit("team_alias.csv not found")
    a = pd.read_csv(TEAM_ALIAS, dtype=str).fillna("")
    if "standard_name" not in a.columns:
        raise SystemExit("team_alias.csv missing standard_name column")
    if "net_name" not in a.columns:
        a["net_name"] = ""
    return a


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    alias = build_alias()
    standard = alias["standard_name"].astype(str).map(_clean).tolist()

    src = fetch_source()
    src["Team"] = src["Team"].astype(str).map(_clean)
    src_team_to_rank = dict(zip(src["Team"], src["Rank"]))

    snapshot_date = datetime.now(timezone.utc).date().isoformat()

    exact_ci = {t.lower(): t for t in src_team_to_rank.keys()}

    used_source = {}
    matched_rows = []
    unmatched_rows = []
    collisions = []

    for std in standard:
        row = alias.loc[alias["standard_name"].astype(str).map(_clean) == std]
        desired = ""
        if not row.empty:
            desired = _clean(row.iloc[0].get("net_name", ""))
        if not desired:
            desired = std

        chosen = None
        score = 0.0

        if desired in src_team_to_rank:
            chosen = desired
            score = 1.0
        else:
            key = desired.lower()
            if key in exact_ci:
                chosen = exact_ci[key]
                score = 0.999

        if chosen is None:
            best_team = None
            best_score = -1.0
            for t in src_team_to_rank.keys():
                s = _ratio(desired, t)
                if s > best_score:
                    best_score = s
                    best_team = t
            if best_team is not None and best_score >= 0.6:
                if best_team in used_source:
                    collisions.append(
                        {
                            "standard_name": std,
                            "desired_source": desired,
                            "matched_source": best_team,
                            "matched_rank": src_team_to_rank.get(best_team, ""),
                            "match_score": round(best_score, 6),
                            "note": f"already used by {used_source[best_team]}",
                        }
                    )
                    chosen = None
                else:
                    chosen = best_team
                    score = best_score

        if chosen is None:
            unmatched_rows.append(
                {"source_team": std, "suggested_standard": "", "match_score": 0.0}
            )
            matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": ""})
        else:
            used_source[chosen] = std
            matched_rows.append(
                {"snapshot_date": snapshot_date, "Team": std, "SoS": int(src_team_to_rank[chosen])}
            )

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(DATA_RAW / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um_df.to_csv(DATA_RAW / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(
        collisions,
        columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"],
    )
    col_df.to_csv(DATA_RAW / "sos_collisions.csv", index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_team_to_rank)}")
    print(f"Matched: {out_df['SoS'].astype(str).str.strip().replace('nan','').ne('').sum()}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")


if __name__ == "__main__":
    main()
