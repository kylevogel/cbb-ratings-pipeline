import pandas as pd
import requests
import os
import time

# Base URL with placeholder for page number
BPI_URL = "https://www.espn.com/mens-college-basketball/bpi/_/view/bpi/page/{}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    all_teams = []
    
    # Loop through pages 1 to 8 to get all ~362 teams
    for page in range(1, 9):
        url = BPI_URL.format(page)
        print(f"Fetching BPI Page {page}...")
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            
            # Read tables from HTML
            tables = pd.read_html(r.text)
            if not tables:
                print(f"No tables found on page {page}.")
                continue

            # ESPN splits BPI into two side-by-side tables (Names + Stats)
            # We merge them if needed
            if len(tables) >= 2:
                names = tables[0]
                stats = tables[1]
                page_df = pd.concat([names, stats], axis=1)
            else:
                page_df = tables[0]
            
            # Find the column containing the Team name
            team_col = next((c for c in page_df.columns if "Team" in str(c)), page_df.columns[0])
            
            # Clean up the dataframe
            page_df = page_df.rename(columns={team_col: "Team"})
            page_df = page_df[["Team"]].copy()
            page_df["Team"] = page_df["Team"].astype(str)
            
            all_teams.append(page_df)
            
            # Sleep briefly to be respectful to the server
            time.sleep(1)
            
        except Exception as e:
            print(f"Error reading page {page}: {e}")
            break

    if not all_teams:
        print("No BPI data found!")
        return

    # Combine all pages into one dataframe
    full_df = pd.concat(all_teams, ignore_index=True)
    
    # Create the BPI Rank column (1 to N)
    full_df["BPI_Rank"] = range(1, len(full_df) + 1)
    
    # Save to CSV
    out_df = full_df[["Team", "BPI_Rank"]].copy()
    out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
    
    os.makedirs("data_raw", exist_ok=True)
    out_path = os.path.join("data_raw", "BPI_Rank.csv")
    out_df.to_csv(out_path, index=False)
    
    print(f"Successfully wrote {len(out_df)} rows to {out_path}")
    
    # Validate count to ensure we fixed the assertion error
    if len(out_df) < 300:
        raise AssertionError(f"BPI validation warning: Only found {len(out_df)} teams (expected >300)")

if __name__ == "__main__":
    main()
