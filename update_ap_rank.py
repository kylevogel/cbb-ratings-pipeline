"""
Scrape AP Poll rankings from AP News hub page.
Outputs: data_raw/ap_rankings.csv
"""
import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


def is_valid_team_name(text):
    """Check if text looks like a valid team name, not a game result."""
    if not text:
        return False
    
    # Skip if it looks like a game result
    # Patterns like: "@ BYU W 77-66", "vs Duke L 65-70", "W 80-75", "L 60-70"
    game_patterns = [
        r'^@\s',           # Starts with @ (away game)
        r'^vs\.?\s',       # Starts with vs (home game)
        r'\s[WL]\s',       # Contains W or L surrounded by spaces (win/loss)
        r'\s\d+-\d+',      # Contains score like "77-66"
        r'^[WL]\s+\d+',    # Starts with W or L followed by numbers
    ]
    
    for pattern in game_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False
    
    # Skip if it's just numbers or very short
    if len(text) < 3:
        return False
    
    # Skip common non-team text
    skip_words = ['record', 'points', 'votes', 'previous', 'trend', 'poll', 'week']
    if text.lower() in skip_words:
        return False
    
    return True


def scrape_ap_poll():
    url = "https://apnews.com/hub/ap-top-25-college-basketball-poll"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    
    # Find start of AP Top 25 Men's Poll
    start_idx = None
    for i, ln in enumerate(lines):
        if "ap top 25" in ln.lower() and "men" in ln.lower() and "poll" in ln.lower():
            start_idx = i
            break
    
    if start_idx is None:
        print("Could not find AP Top 25 Men's Poll header")
        return None
    
    rows = []
    i = start_idx
    
    stop_markers = {
        "others receiving votes",
        "dropout",
        "dropped out",
        "trend",
        "points",
    }
    
    while i < len(lines) - 1:
        ln = lines[i]
        
        # Stop if we hit end markers and already have some data
        if any(m in ln.lower() for m in stop_markers) and rows:
            break
        
        # Check if this line is a rank number (1-25)
        if re.fullmatch(r"\d{1,2}", ln):
            rk = int(ln)
            if 1 <= rk <= 25:
                # Look ahead for a valid team name (might need to skip game results)
                j = i + 1
                team = None
                
                # Search the next few lines for a valid team name
                while j < min(i + 5, len(lines)):
                    candidate = lines[j]
                    
                    # Remove record suffix like "(21-2)"
                    candidate = re.sub(r"\s*\(\d+-\d+\)\s*$", "", candidate).strip()
                    
                    if is_valid_team_name(candidate):
                        team = candidate
                        break
                    
                    j += 1
                
                if team:
                    rows.append({"ap_rank": rk, "team_ap": team})
                    i = j + 1
                    continue
        
        i += 1
    
    if not rows:
        print("No AP Poll data found")
        return None
    
    df = pd.DataFrame(rows).drop_duplicates(subset=["ap_rank"]).sort_values("ap_rank").reset_index(drop=True)
    
    if len(df) < 20:
        print(f"Only found {len(df)} teams, expected at least 20")
        return None
    
    print(f"Successfully parsed {len(df)} AP Poll teams")
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
        print(f"Saved {len(df)} AP Poll teams to data_raw/ap_rankings.csv")
        print("\nAP Top 10:")
        print(df.head(10).to_string(index=False))
    else:
        print("Failed to fetch AP Poll rankings")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
