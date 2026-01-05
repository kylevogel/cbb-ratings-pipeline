import pandas as pd
import requests
import os
from datetime import datetime

# Standardize headers to look like a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    # WarrenNolan is a reliable source for NET
    # We dynamically get the current season year (e.g. 2026 for the 2025-26 season)
    year = datetime.now().year
    if datetime.now().month > 10:
        year += 1
    
    url = f"https://www.warrennolan.com/basketball/{year}/net"
    print(f"Fetching NET rankings from {url}...")

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        tables = pd.read_html(r.text)
        if not tables:
            print("No tables found for NET.")
            return

        # WarrenNolan usually has one main table
        df = tables[0]
        
        # Standardize Columns
        # Look for 'Team' and 'Rank' or 'NET'
        # Usually headers are: Rank, Team, Record, ...
        
        # Find team column
        team_col = next((c for c in df.columns if "Team" in str(c)), None)
        # Find rank column (usually first column or named 'NET')
        rank_col = next((c for c in df.columns if "Rank" in str(c) or "NET" in str(c)), df.columns[0])

        if not team_col:
            print("Could not find Team column in NET table.")
            return

        df = df.rename(columns={team_col: "Team", rank_col: "NET_Rank"})
        
        # Clean Team Names (WarrenNolan includes record sometimes, e.g. "Duke (15-2)")
        # We strip the record parenthesis if present
        df["Team"] = df["Team"].astype(str).str.replace(r"\s+\(\d+-\d+\).*", "", regex=True).str.strip()
        
        # Keep only relevant columns
        out_df = df[["Team", "NET_Rank"]].copy()
        
        # Add snapshot
        out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        os.makedirs("data_raw", exist_ok=True)
        out_path = os.path.join("data_raw", "NET_Rank.csv")
        out_df.to_csv(out_path, index=False)
        
        print(f"Successfully wrote {len(out_df)} rows to {out_path}")

    except Exception as e:
        print(f"Error updating NET: {e}")

if __name__ == "__main__":
    main()
