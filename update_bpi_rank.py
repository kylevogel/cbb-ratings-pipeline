import pandas as pd
import requests
import os

BPI_URL = "https://www.espn.com/mens-college-basketball/bpi"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    print(f"Fetching BPI from {BPI_URL}...")
    try:
        r = requests.get(BPI_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        # Parse tables
        tables = pd.read_html(r.text)
        if not tables:
            print("No tables found on BPI page.")
            return

        # ESPN BPI page usually splits names and stats into two tables
        if len(tables) >= 2:
            names = tables[0]
            stats = tables[1]
            df = pd.concat([names, stats], axis=1)
        else:
            df = tables[0]

        # Find the team column
        team_col = next((c for c in df.columns if "Team" in str(c)), df.columns[0])
        
        df[team_col] = df[team_col].astype(str)
        
        # BPI table is already sorted by rank, so we can use index
        df["BPI_Rank"] = range(1, len(df) + 1)
        
        out_df = df[[team_col, "BPI_Rank"]].copy()
        out_df.columns = ["Team", "BPI_Rank"]
        
        out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        os.makedirs("data_raw", exist_ok=True)
        out_path = os.path.join("data_raw", "BPI_Rank.csv")
        out_df.to_csv(out_path, index=False)
        print(f"Successfully wrote {len(out_df)} rows to {out_path}")

    except Exception as e:
        print(f"Error updating BPI: {e}")

if __name__ == "__main__":
    main()
