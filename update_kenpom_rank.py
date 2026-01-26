"""
Scrape KenPom rankings using Selenium to bypass Cloudflare protection.
Outputs: data_raw/kenpom_rankings.csv
"""
import pandas as pd
import os
import re
import time

def scrape_kenpom_rankings():
    """Scrape KenPom rankings using Selenium."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        print("Selenium not installed. Install with: pip install selenium")
        return None
    
    url = "https://kenpom.com/"
    
    # Set up Chrome options for headless browsing
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = None
    try:
        print("Starting headless Chrome browser...")
        driver = webdriver.Chrome(options=chrome_options)
        
        print(f"Navigating to {url}")
        driver.get(url)
        
        # Wait for the table to load (Cloudflare check should complete)
        print("Waiting for page to load...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "ratings-table"))
        )
        
        # Give it a moment for all data to render
        time.sleep(2)
        
        print("Page loaded, parsing table...")
        
        # Find the table
        table = driver.find_element(By.ID, "ratings-table")
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        print(f"Found {len(rows)} rows in table")
        
        data = []
        for row in rows[1:]:  # Skip header
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 4:
                rank_text = cells[0].text.strip()
                team_text = cells[1].text.strip()
                
                # Get W-L record (usually in cell with class 'wl')
                record = ""
                try:
                    wl_cell = row.find_element(By.CLASS_NAME, "wl")
                    record_text = wl_cell.text.strip()
                    record_match = re.search(r'(\d+)-(\d+)', record_text)
                    if record_match:
                        record = f"{record_match.group(1)}-{record_match.group(2)}"
                except:
                    pass
                
                rank = re.sub(r'[^\d]', '', rank_text)
                
                if rank and team_text:
                    data.append({
                        'kenpom_rank': int(rank),
                        'team_kenpom': team_text,
                        'record': record
                    })
        
        if data:
            df = pd.DataFrame(data)
            print(f"Successfully parsed {len(df)} teams")
            return df
        else:
            print("No data found in table")
            return None
            
    except Exception as e:
        print(f"Error scraping KenPom: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if driver:
            driver.quit()


def main():
    print("Fetching KenPom rankings...")
    df = scrape_kenpom_rankings()
    
    output_path = 'data_raw/kenpom_rankings.csv'
    
    if df is not None and not df.empty:
        os.makedirs('data_raw', exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} KenPom rankings to {output_path}")
        print("\nFirst 10 teams:")
        print(df.head(10).to_string(index=False))
    else:
        # Check if we have existing data to preserve
        if os.path.exists(output_path):
            print(f"Failed to fetch new KenPom data. Keeping existing {output_path}")
        else:
            print("Failed to fetch KenPom rankings.")


if __name__ == "__main__":
    main()
