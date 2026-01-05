import pandas as pd
import requests
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    url = "https://www.espn.com/mens-college-basketball/rankings"
    print(f"Fetching AP rankings from {url}...")

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        tables = pd.read_html(r.text)
        if not tables:
            print("No tables found for AP Poll.")
            return

        # Find the correct table by looking for specific headers
        df = None
        for t in tables:
            # Check if columns indicate a ranking table (Record, Pts, Trend)
            # Convert columns to string to be safe
            cols = [str(c).lower() for c in t.columns]
            if any("rec" in c for c in cols) and any("pts" in c for c in cols):
                df = t
                break
        
        if df is None:
            # Fallback to first table if search fails
            print("Could not identify AP table by headers. Falling back to table[0].")
            df = tables[0]

        # Standardize Columns
        # ESPN tables often have unnamed columns for Rank. 
        # Typically: RK, Team, REC, PTS, TREND
        # We rename the first column to 'AP_Rank' and second to 'Team'
        
        # Ensure we have enough columns
        if len(df.columns) < 2:
            print("Table found but has fewer than 2 columns.")
            return

        df = df.rename(columns={df.columns[0]: "AP_Rank", df.columns[1]: "Team"})
        
        # Clean Data
        df["Team"] = df["Team"].astype(str)
        # Remove record like "(10-2)"
        df["Team"] = df["Team"].str.replace(r"\s*\(\d+-\d+\).*", "", regex=True)
        # Remove leading rank number like "1 Kansas"
        df["Team"] = df["Team"].str.replace(r"^\d+\s+", "", regex=True)
        # Strip whitespace
        df["Team"] = df["Team"].str.strip()

        # Ensure Rank is valid (sometimes it's "RV" or empty)
        # We only want the top 25 integers
        df["AP_Rank"] = pd.to_numeric(df["AP_Rank"], errors="coerce")
        df = df.dropna(subset=["AP_Rank"])
        df["AP_Rank"] = df["AP_Rank"].astype(int)

        out_df = df[["Team", "AP_Rank"]].copy()
        out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        os.makedirs("data_raw", exist_ok=True)
        out_path = os.path.join("data_raw", "AP_Rank.csv")
        out_df.to_csv(out_path, index=False)
        
        print(f"Successfully wrote {len(out_df)} rows to {out_path}")

    except Exception as e:
        print(f"Error updating AP: {e}")

if __name__ == "__main__":
    main()
