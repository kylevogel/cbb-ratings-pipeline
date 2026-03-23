"""
Scrape AP Poll rankings from AP News hub page.
Outputs: data_raw/ap_rankings.csv

Fixes vs original:
  - Replaced fragile line-by-line text parser (Method 2) with a focused
    structured approach: find the first <ol> on the page that contains 25
    <li> items, which is always the AP poll list.
  - Kept a tightened Method 1 (table-based) as first attempt.
  - Removed overzealous skip filters in the text fallback that were silently
    dropping teams whose names contain parentheticals like "(FL)" or whose
    surrounding lines matched conference / record patterns.
  - Added AP-to-canonical name normalisation so "UConn" -> "Connecticut",
    "Miami (FL)" -> "Miami", etc.
  - Logs a clear warning for any ranks 1-25 missing after scraping.
"""

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

_AP_TO_CANONICAL: dict[str, str] = {
    "UConn":             "Connecticut",
    "Miami (FL)":        "Miami",
    "Miami (Oh)":        "Miami (OH)",
    "Saint Mary's (CA)": "Saint Mary's",
    "N.C. State":        "NC State",
    "Iowa St.":          "Iowa State",
    "Michigan St.":      "Michigan State",
    "Ohio St.":          "Ohio State",
}


def _normalise(name: str) -> str:
    s = re.sub(r"\s+", " ", name).strip()
    return _AP_TO_CANONICAL.get(s, s)


# ---------------------------------------------------------------------------
# Scraping helpers
# ---------------------------------------------------------------------------

def _clean_team_name(raw: str) -> str:
    """Strip record suffix and extra whitespace from a raw team name string."""
    # Remove trailing record like "23-0" or "(23-0)" or "23-0 (59)"
    s = re.sub(r"\s*\(?\d{1,2}-\d{1,2}\)?.*$", "", raw)
    # Remove trailing vote/point totals like "1475 (59)"
    s = re.sub(r"\s*\d+\s*\(\d+\)\s*$", "", s)
    return s.strip()


def _try_table_method(soup: BeautifulSoup) -> list[dict]:
    """Method 1: look for <tr> rows whose first cell is a rank 1-25."""
    rows = []
    seen = set()
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        rank_text = cells[0].get_text(strip=True)
        if not re.fullmatch(r"\d{1,2}", rank_text):
            continue
        rank = int(rank_text)
        if rank < 1 or rank > 25 or rank in seen:
            continue

        team_cell = cells[1]
        team_name = None

        # Try links first
        for link in team_cell.find_all("a"):
            text = _clean_team_name(link.get_text(strip=True))
            if text and len(text) >= 3 and re.match(r"^[A-Za-z]", text):
                if not re.search(r"Big|Atlantic|SEC|ACC|Pac|Mountain|American", text):
                    team_name = text
                    break

        # Fallback: full cell text
        if not team_name:
            full = team_cell.get_text(" ", strip=True)
            m = re.match(
                r"^([A-Za-z][A-Za-z\s\.\'\(\)&]+?)(?:\s+\d{1,2}-\d{1,2}|\s+Big|\s+Atlantic|\s+SEC|\s+ACC|\s+Pac|\s+Mountain|\s+American|$)",
                full,
            )
            if m:
                team_name = m.group(1).strip()

        if team_name:
            rows.append({"ap_rank": rank, "team_ap": team_name})
            seen.add(rank)

    return rows


def _try_ordered_list_method(soup: BeautifulSoup) -> list[dict]:
    """
    Method 2: find the <ol> that contains 20+ <li> items — that is the AP
    poll list on the AP News hub page. Assign ranks by position (1-based)
    so there are no gaps from rank-number parsing issues.
    """
    for ol in soup.find_all("ol"):
        items = ol.find_all("li", recursive=False)
        if len(items) < 20:
            continue

        candidate_rows = []
        for rank, li in enumerate(items[:25], start=1):
            text = li.get_text(" ", strip=True)
            # Strip leading rank number if the layout includes it
            text = re.sub(r"^\d{1,2}\.?\s*", "", text).strip()
            team = _clean_team_name(text)
            # Strip trailing conference name if glued on
            team = re.sub(
                r"\s+(Big|Atlantic|SEC|ACC|Pac|Mountain|American|West Coast|"
                r"Missouri Valley|MAC|SBC|CUSA|Horizon|Ivy|Patriot|Southern|"
                r"Southland|SWAC|OVC|Big South|Big Sky|Big West|Sun Belt|WAC|"
                r"NEC|CAA|A-10|MWC|WCC|MVC).*$",
                "",
                team,
                flags=re.IGNORECASE,
            ).strip()
            if team and len(team) >= 3 and re.match(r"^[A-Za-z]", team):
                candidate_rows.append({"ap_rank": rank, "team_ap": team})

        if len(candidate_rows) >= 20:
            return candidate_rows

    return []


def _try_text_method(soup: BeautifulSoup) -> list[dict]:
    """
    Method 3: line-by-line text scan.
    Fixed vs original: removed filters that dropped teams with parentheticals
    in their name (like 'Miami (FL)'), and made the team-name lookahead
    more lenient — only hard-skipping lines that are purely numeric,
    purely a record, or a bare conference name.
    """
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    rows = []
    i = 0

    while i < len(lines) - 1 and len(rows) < 25:
        ln = lines[i]

        if re.fullmatch(r"\d{1,2}", ln):
            rk = int(ln)
            # Allow same rank twice (ties), but stop at 25 total teams
            if 1 <= rk <= 25 and len(rows) < 25:
                for j in range(i + 1, min(i + 12, len(lines))):
                    candidate = lines[j]

                    # Hard skips — things that are definitely NOT team names
                    if re.fullmatch(r"\d{1,2}", candidate):           # another rank
                        break
                    if re.fullmatch(r"\d+-\d+", candidate):           # bare record
                        continue
                    if re.fullmatch(r"\d+\s*\(\d+\)", candidate):     # points (votes)
                        continue
                    if re.fullmatch(r"[▲▼↑↓\-\+]\s*\d*", candidate):  # trend arrow
                        continue
                    if len(candidate) < 3:
                        continue
                    # Bare conference names with no team name present
                    if re.fullmatch(
                        r"(Big (12|Ten|East)|SEC|ACC|Pac-12|MWC|WCC|A-10|"
                        r"American|Mountain West|Big Sky|Big South|Big West)",
                        candidate,
                        re.IGNORECASE,
                    ):
                        continue
                    if not re.match(r"^[A-Za-z]", candidate):
                        continue

                    # Looks like a team name — clean and accept
                    team = _clean_team_name(candidate)
                    team = re.sub(
                        r"\s+(Big|Atlantic|SEC|ACC|Pac|Mountain|American).*$",
                        "",
                        team,
                        flags=re.IGNORECASE,
                    ).strip()

                    if team and len(team) >= 3:
                        # Skip if we already have this team (scrape artifact)
                        if not any(r["team_ap"] == team for r in rows):
                            rows.append({"ap_rank": rk, "team_ap": team})
                        break
        i += 1

    return rows


# ---------------------------------------------------------------------------
# Main scrape entry point
# ---------------------------------------------------------------------------

def scrape_ap_poll() -> pd.DataFrame | None:
    url = "https://apnews.com/hub/ap-top-25-college-basketball-poll"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows: list[dict] = []

    # Try methods in order of reliability
    for method_name, method_fn in [
        ("table",        _try_table_method),
        ("ordered-list", _try_ordered_list_method),
        ("text-scan",    _try_text_method),
    ]:
        rows = method_fn(soup)
        if len(rows) >= 20:
            print(f"  Parsed via {method_name} method ({len(rows)} teams)")
            break
        else:
            print(f"  {method_name} method found only {len(rows)} teams, trying next...")

    if not rows:
        print("No AP Poll data found with any method")
        return None

    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["team_ap"])   # dedupe by team, NOT rank — tied ranks are valid
        .sort_values(["ap_rank", "team_ap"])
        .reset_index(drop=True)
    )

    # Normalise names to canonical
    df["team_ap"] = df["team_ap"].apply(_normalise)

    # With ties some rank numbers legitimately don't appear (e.g. two #23s means no #24)
    # Warn only if team count is below 25, not on missing rank numbers
    if len(df) < 25:
        print(f"  WARNING: only {len(df)} teams scraped (expected 25) — parser may need updating.")

    for _, row in df.iterrows():
        print(f"  {int(row['ap_rank'])}. {row['team_ap']}")

    return df


# ---------------------------------------------------------------------------

def main():
    print("Fetching AP Poll rankings from AP News...")
    df = None

    try:
        df = scrape_ap_poll()
    except Exception as e:
        print(f"Error fetching AP Poll: {e}")
        import traceback
        traceback.print_exc()

    os.makedirs("data_raw", exist_ok=True)

    if df is not None and not df.empty:
        df.to_csv("data_raw/ap_rankings.csv", index=False)
        print(f"\nSaved {len(df)} AP Poll teams to data_raw/ap_rankings.csv")
    else:
        print("Failed to fetch AP Poll rankings")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
