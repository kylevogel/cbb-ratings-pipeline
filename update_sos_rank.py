import pandas as pd
import requests
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    url = "https://www.teamrankings.com/ncaa-basketball/ranking/schedule-strength-by-other"
    print(f"Fetching SoS rankings from {url}...")

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        tables = pd.read_html(r.text)
        if not tables:
            print("No tables found for SoS.")
            return

        df = tables[0]
        
        # Headers: Rank, Team, Rating, Hi, Lo, Last
        df = df.rename(columns={"Rank": "SOS_Rank", "Team": "Team"})
        
        # TeamRankings includes record in name: "Duke (15-2)"
        df["Team"] = df["Team"].astype(str).str.replace(r"\s+\(\d+-\d+\).*", "", regex=True).str.strip()
        
        out_df = df[["Team", "SOS_Rank"]].copy()
        out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        os.makedirs("data_raw", exist_ok=True)
        out_path = os.path.join("data_raw", "SOS_Rank.csv")
        out_df.to_csv(out_path, index=False)
        
        print(f"Successfully wrote {len(out_df)} rows to {out_path}")

    except Exception as e:
        print(f"Error updating SoS: {e}")

if __name__ == "__main__":
    main()
