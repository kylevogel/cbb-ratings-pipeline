import pandas as pd
import requests
import os

# Header to look like a real browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def main():
    url = "https://kenpom.com/"
    print(f"Fetching KenPom data from {url}...")
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        # Parse tables
        tables = pd.read_html(r.text, attrs={"id": "ratings-table"})
        if not tables:
            print("Could not find ratings-table in HTML.")
            return

        df = tables[0]
        
        # Clean messy headers (KenPom repeats headers every ~40 rows)
        df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
        df = df[df["Team"] != "Team"].copy()
        
        # Clean Team names (remove seed numbers like '1 Kansas')
        df["Team"] = df["Team"].astype(str).str.replace(r"^\d+\s+", "", regex=True)
        df["Team"] = df["Team"].str.replace(r"\s+\d+$", "", regex=True)
        
        # Rename columns (KenPom uses 'Rank', 'Team', 'AdjEM', etc.)
        # The rank column is often unnamed or the first column
        rename_map = {df.columns[0]: "Rank", "Team": "Team"}
        df = df.rename(columns=rename_map)
        
        # Ensure Rank is numeric
        df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")
        df = df.dropna(subset=["Rank"])
        df["Rank"] = df["Rank"].astype(int)
        
        out_df = df[["Team", "Rank"]].copy()
        out_df.columns = ["Team", "KenPom_Rank"]
        
        # Add snapshot date
        out_df["snapshot_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        os.makedirs("data_raw", exist_ok=True)
        out_path = os.path.join("data_raw", "KenPom_Rank.csv")
        out_df.to_csv(out_path, index=False)
        print(f"Successfully wrote {len(out_df)} rows to {out_path}")
        
    except Exception as e:
        print(f"Error updating KenPom: {e}")

if __name__ == "__main__":
    main()
