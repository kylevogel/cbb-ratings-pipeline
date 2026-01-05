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

        # ESPN Rankings page usually has AP as table 0
        df = tables[0]
        
        # Columns are usually: RK, Team, Rec, Pts...
        # We rename the first two explicitly to be safe
        df = df.rename(columns={df.columns[0]: "AP_Rank", df.columns[1]: "Team"})
        
        # Clean Team Name (ESPN often puts "1 Kansas" or "Kansas (10-2)")
        df["Team"] = df["Team"].astype(str)
        
        # FIX: Use .str accessor for string methods
        # Remove record if present (e.g. " (10-2)")
        df["Team"] = df["Team"].str.replace(r"\s*\(\d+-\d+\).*", "", regex=True)
        # Remove leading rank number if merged (e.g. "1 Kansas")
        df["Team"] = df["Team"].str.replace(r"^\d+\s+", "", regex=True)
        # Strip whitespace
        df["Team"] = df["Team"].str.strip()

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
