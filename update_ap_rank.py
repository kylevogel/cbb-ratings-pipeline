"""
Scrape AP Poll rankings from AP News hub page.
Outputs: data_raw/ap_rankings.csv
"""
import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


def scrape_ap_poll():
    url = "https://apnews.com/hub/ap-top-25-college-basketball-poll"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    rows = []
    
    # Method 1: Try to find the rankings table directly
    # Look for table rows with rank numbers
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2:
            # First cell should be rank
            rank_text = cells[0].get_text(strip=True)
            if re.fullmatch(r"\d{1,2}", rank_text):
                rank = int(rank_text)
                if 1 <= rank <= 25:
                    # Second cell should contain team name
                    team_cell = cells[1]
                    
                    # Try to find team name - it's usually in a link or specific element
                    # Look for the first substantial text that looks like a team name
                    team_name = None
                    
                    # First try: look for a link with the team name
                    links = team_cell.find_all("a")
                    for link in links:
                        text = link.get_text(strip=True)
                        # Team names are typically just the name, maybe with record
                        # Filter out things like "Big 12", "Big Ten", dates, etc.
                        if text and len(text) > 2:
                            # Remove record suffix like "23-0" or "(23-0)"
                            clean = re.sub(r"\s*\(?\d{1,2}-\d{1,2}\)?$", "", text).strip()
                            if clean and not re.search(r"Big|Atlantic|SEC|ACC|Pac|Mountain|American|at \d|p\.m\.|a\.m\.", clean):
                                team_name = clean
                                break
                    
                    # Second try: get direct text content
                    if not team_name:
                        # Get all text, split and find team name
                        full_text = team_cell.get_text(" ", strip=True)
                        # Pattern: "TeamName Record Conference" like "Arizona 23-0 Big 12"
                        match = re.match(r"^([A-Za-z][A-Za-z\s\.\'\(\)&]+?)(?:\s+\d{1,2}-\d{1,2}|\s+Big|\s+Atlantic|\s+SEC|\s+ACC|\s+Pac|\s+Mountain|\s+American|$)", full_text)
                        if match:
                            team_name = match.group(1).strip()
                    
                    if team_name:
                        # Clean up team name
                        team_name = re.sub(r"\s+", " ", team_name).strip()
                        rows.append({"ap_rank": rank, "team_ap": team_name})
                        print(f"  {rank}. {team_name}")
    
    # Method 2: If table parsing didn't work, try text-based with better filtering
    if len(rows) < 20:
        print("Table parsing incomplete, trying text-based method...")
        rows = []
        
        text = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        
        # Find rankings section
        start_idx = None
        for i, ln in enumerate(lines):
            if "released" in ln.lower() and ("february" in ln.lower() or "january" in ln.lower()):
                start_idx = i
                break
            if re.search(r"week\s*\d+", ln.lower()) and i < 50:
                start_idx = i
                break
        
        if start_idx is None:
            start_idx = 0
        
        i = start_idx
        found_ranks = set()
        
        while i < len(lines) - 1 and len(rows) < 25:
            ln = lines[i]
            
            # Check if this line is a rank number (1-25) we haven't found yet
            if re.fullmatch(r"\d{1,2}", ln):
                rk = int(ln)
                if 1 <= rk <= 25 and rk not in found_ranks:
                    # Look ahead for team name
                    for j in range(i + 1, min(i + 10, len(lines))):
                        candidate = lines[j]
                        
                        # Skip invalid entries
                        if re.search(r"^\d{1,2}$", candidate):  # Another rank number
                            break
                        if re.search(r"(p\.m\.|a\.m\.|EDT|EST|vs\.|at \d|Feb\.|Jan\.|Mar\.)", candidate, re.IGNORECASE):
                            continue
                        if re.search(r"^\d+-\d+$", candidate):  # Just a record
                            continue
                        if re.search(r"^(Big|Atlantic|SEC|ACC|Pac|Mountain|American|West|East)", candidate):
                            continue
                        if re.search(r"^[▲▼↑↓\-\+]?\s*\d+$", candidate):  # Trend indicator
                            continue
                        if re.search(r"^\(\d+\)$", candidate):  # Vote count like (0), (59)
                            continue
                        if re.search(r"^\d+\s*\(\d+\)$", candidate):  # Points with votes like "1475 (59)"
                            continue
                        if len(candidate) < 3:
                            continue
                        # Must start with a letter (team names start with letters)
                        if not re.match(r"^[A-Za-z]", candidate):
                            continue
                        
                        # This looks like a team name
                        # Clean it up - remove record if attached
                        team = re.sub(r"\s*\d{1,2}-\d{1,2}.*$", "", candidate).strip()
                        
                        if team and len(team) >= 3:
                            rows.append({"ap_rank": rk, "team_ap": team})
                            found_ranks.add(rk)
                            print(f"  {rk}. {team}")
                            break
            i += 1
    
    if not rows:
        print("No AP Poll data found")
        return None
    
    df = pd.DataFrame(rows).drop_duplicates(subset=["ap_rank"]).sort_values("ap_rank").reset_index(drop=True)
    
    if len(df) < 20:
        print(f"Warning: Only found {len(df)} teams, expected 25")
    
    return df


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
