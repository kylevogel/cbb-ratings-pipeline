"""
Scrape KenPom rankings.
Note: KenPom requires subscription for full data. This scrapes publicly available rankings.
Outputs: data_raw/kenpom_rankings.csv
"""
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re


def scrape_kenpom_rankings():
    """Scrape KenPom rankings from the public page."""
    url = "https://kenpom.com/"
    
    # Use a full set of browser headers to avoid 403
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    try:
        # Use a session to handle cookies
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        print(f"Successfully fetched KenPom page ({len(response.text)} bytes)")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try to find the ratings table
        table = soup.find('table', {'id': 'ratings-table'})
        if not table:
            tables = soup.find_all('table')
            for t in tables:
                if t.find('th') and 'Team' in t.get_text():
                    table = t
                    break
        
        if not table:
            print("Could not find KenPom table")
            return None
        
        print("Found KenPom table, parsing rows...")
        
        rows = []
        tbody = table.find('tbody')
        if tbody:
            trs = tbody.find_all('tr')
        else:
            trs = table.find_all('tr')[1:]
        
        print(f"Found {len(trs)} table rows")
        
        for tr in trs:
            cells = tr.find_all(['td', 'th'])
            if len(cells) >= 4:
                rank_text = cells[0].get_text(strip=True)
                team_cell = cells[1]
                
                # W-L is in the cell with class="wl" or at index 3
                record = ""
                wl_cell = tr.find('td', class_='wl')
                if wl_cell:
                    record_text = wl_cell.get_text(strip=True)
                    record_match = re.search(r'(\d+)-(\d+)', record_text)
                    if record_match:
                        record = f"{record_match.group(1)}-{record_match.group(2)}"
                
                team_link = team_cell.find('a')
                if team_link:
                    team = team_link.get_text(strip=True)
                else:
                    team = team_cell.get_text(strip=True)
                
                rank = re.sub(r'[^\d]', '', rank_text)
                
                if rank and team:
                    rows.append({
                        'kenpom_rank': int(rank),
                        'team_kenpom': team,
                        'record': record
                    })
        
        if rows:
            df = pd.DataFrame(rows)
            print(f"Successfully parsed {len(df)} teams")
            return df
        else:
            print("No KenPom data found in table")
            return None
            
    except requests.exceptions.HTTPError as e:
        print(f"Error scraping KenPom: {e}")
        if e.response.status_code == 403:
            print("KenPom is blocking automated requests. The site may require authentication or have bot protection.")
        return None
    except Exception as e:
        print(f"Error scraping KenPom: {e}")
        return None


def main():
    print("Fetching KenPom rankings...")
    df = scrape_kenpom_rankings()
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv('data_raw/kenpom_rankings.csv', index=False)
        print(f"Saved {len(df)} KenPom rankings to data_raw/kenpom_rankings.csv")
        print("\nFirst 10 teams:")
        print(df.head(10).to_string(index=False))
    else:
        print("Failed to fetch KenPom rankings")


if __name__ == "__main__":
    main()
