"""
Scrape Strength of Schedule rankings from Warren Nolan.
Outputs: data_raw/sos_rankings.csv
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re
from io import StringIO


def scrape_sos_rankings():
    """Scrape SOS rankings from Warren Nolan."""
    url = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        print(f"Successfully fetched page ({len(response.text)} bytes)")
        
        # Parse with BeautifulSoup first - more reliable for this site's structure
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = []
        
        # Find the stats table specifically
        table = soup.find('table', class_='stats-table')
        if not table:
            # Fallback to any table
            table = soup.find('table')
        
        if table:
            print("Found table, parsing rows...")
            tbody = table.find('tbody')
            if tbody:
                all_rows = tbody.find_all('tr')
            else:
                all_rows = table.find_all('tr')[1:]  # Skip header
            
            print(f"Found {len(all_rows)} data rows")
            
            for tr in all_rows:
                cells = tr.find_all('td')
                
                # Table structure: Team | SOS | Rank | Opp Record | Opp Win Percent | SOS Delta
                if len(cells) >= 3:
                    # Get team name - it's nested in divs and an anchor tag
                    team_cell = cells[0]
                    
                    # Try to find the team name in the anchor tag
                    team_link = team_cell.find('a', class_='blue-black')
                    if team_link:
                        team = team_link.get_text(strip=True)
                    else:
                        # Fallback: try any anchor
                        team_link = team_cell.find('a')
                        if team_link:
                            team = team_link.get_text(strip=True)
                        else:
                            # Last resort: get all text from cell
                            team = team_cell.get_text(strip=True)
                    
                    # Get rank from third cell (index 2)
                    rank_text = cells[2].get_text(strip=True)
                    
                    # Clean rank - extract just the number
                    rank_clean = re.sub(r'[^\d]', '', rank_text)
                    
                    if team and rank_clean and rank_clean.isdigit():
                        rank = int(rank_clean)
                        if 1 <= rank <= 400:
                            rows.append({
                                'sos_rank': rank,
                                'team_sos': team
                            })
        
        if rows:
            df = pd.DataFrame(rows)
            df = df.drop_duplicates(subset=['sos_rank']).sort_values('sos_rank')
            print(f"Successfully parsed {len(df)} teams")
            return df
        
        # If BeautifulSoup parsing failed, try pd.read_html as fallback
        print("BeautifulSoup parsing found no rows, trying pd.read_html...")
        try:
            tables = pd.read_html(StringIO(response.text))
            for df in tables:
                cols = [str(c).lower().strip() for c in df.columns]
                
                if any('team' in c for c in cols) and any('rank' in c for c in cols):
                    team_col = None
                    rank_col = None
                    
                    for c in df.columns:
                        c_lower = str(c).lower().strip()
                        if 'team' in c_lower and team_col is None:
                            team_col = c
                        elif 'rank' in c_lower and rank_col is None:
                            rank_col = c
                    
                    if team_col and rank_col:
                        result = df[[team_col, rank_col]].copy()
                        result.columns = ['team_sos', 'sos_rank']
                        result['team_sos'] = result['team_sos'].astype(str).str.strip()
                        result = result.dropna()
                        result['sos_rank'] = pd.to_numeric(result['sos_rank'], errors='coerce')
                        result = result.dropna()
                        result['sos_rank'] = result['sos_rank'].astype(int)
                        result = result[result['sos_rank'] > 0]
                        result = result.drop_duplicates(subset=['sos_rank'])
                        result = result.sort_values('sos_rank')
                        
                        if len(result) > 100:
                            print(f"Successfully parsed {len(result)} teams using pd.read_html")
                            return result
        except Exception as e:
            print(f"pd.read_html also failed: {e}")
        
        print("No SOS data found")
        return None
            
    except Exception as e:
        print(f"Error scraping SOS: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    print("Fetching SOS rankings from Warren Nolan...")
    df = scrape_sos_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/sos_rankings.csv', index=False)
        print(f"Saved {len(df)} SOS rankings to data_raw/sos_rankings.csv")
        print("\nFirst 10 teams:")
        print(df.head(10).to_string(index=False))
    else:
        print("Failed to fetch SOS rankings")


if __name__ == "__main__":
    main()
