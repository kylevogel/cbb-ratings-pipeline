"""
update_kenpom_rank.py
Scrapes KenPom rankings (Selenium, to get past Cloudflare) and writes
data_raw/kenpom_rankings.csv with canonical team names and W-L records.

Fixes applied:
  1. Strips trailing conference-seed suffixes from KenPom names (e.g. "Michigan 1" -> "Michigan")
  2. Normalises KenPom abbreviations to the canonical names used by every other source
     (e.g. "Iowa St." -> "Iowa State", "Miami FL" -> "Miami", "SIUE" -> "SIU Edwardsville")
  3. AP source uses "UConn" - this file outputs "Connecticut" to match canonical; the AP
     normaliser handles its own alias instead (see update_ap_rank.py).
"""

import os
import re
import time

import pandas as pd

# ---------------------------------------------------------------------------
# KenPom-specific name normalisation
# ---------------------------------------------------------------------------

# Step 1 – strip trailing " <digit(s)>" conference-rank suffixes that KenPom
# appends to disambiguate teams sharing a city/nickname within a conference.
# e.g. "Michigan 1", "Iowa St. 2", "Prairie View A&M 16"
_SUFFIX_RE = re.compile(r'\s+\d+$')


def _strip_suffix(name: str) -> str:
    return _SUFFIX_RE.sub('', name.strip())


# Step 2 – map every KenPom abbreviation/shorthand to the canonical team name
# used across NET, BPI, AP, and SOS in this pipeline.
_KENPOM_TO_CANONICAL: dict[str, str] = {
    # "St." -> "State" schools
    "Iowa St.":             "Iowa State",
    "Michigan St.":         "Michigan State",
    "Ohio St.":             "Ohio State",
    "Utah St.":             "Utah State",
    "Arizona St.":          "Arizona State",
    "Florida St.":          "Florida State",
    "Boise St.":            "Boise State",
    "San Diego St.":        "San Diego State",
    "Illinois St.":         "Illinois State",
    "Kansas St.":           "Kansas State",
    "Colorado St.":         "Colorado State",
    "Montana St.":          "Montana State",
    "Morehead St.":         "Morehead State",
    "South Dakota St.":     "South Dakota State",
    "North Dakota St.":     "North Dakota State",
    "Weber St.":            "Weber State",
    "Arkansas St.":         "Arkansas State",
    "Kennesaw St.":         "Kennesaw State",
    "Jacksonville St.":     "Jacksonville State",
    "Murray St.":           "Murray State",
    "Fresno St.":           "Fresno State",
    "Penn St.":             "Penn State",
    "Washington St.":       "Washington State",
    "Portland St.":         "Portland State",
    "Oklahoma St.":         "Oklahoma State",
    "Appalachian St.":      "Appalachian State",
    "East Tennessee St.":   "East Tennessee State",
    "New Mexico St.":       "New Mexico State",
    "Wright St.":           "Wright State",
    "Youngstown St.":       "Youngstown State",
    "Idaho St.":            "Idaho State",
    "Ball St.":             "Ball State",
    "Kent St.":             "Kent State",
    "Morgan St.":           "Morgan State",
    "Alcorn St.":           "Alcorn State",
    "Mississippi Valley St.": "Mississippi Valley State",
    "Alabama St.":          "Alabama State",
    "Long Beach St.":       "Long Beach State",
    "Norfolk St.":          "Norfolk State",
    "Georgia St.":          "Georgia State",
    "Texas St.":            "Texas State",
    "Jackson St.":          "Jackson State",
    "Tennessee St.":        "Tennessee State",
    "Sacramento St.":       "Sacramento State",
    "Northwestern St.":     "Northwestern State",
    "Cal St. Fullerton":    "Cal State Fullerton",
    "Cal St. Bakersfield":  "Cal State Bakersfield",
    "Wichita St.":          "Wichita State",

    # Conference/style differences
    "N.C. State":           "NC State",
    "Miami FL":             "Miami",
    "Miami OH":             "Miami (OH)",
    "Mississippi":          "Ole Miss",
    "Sam Houston St.":      "Sam Houston",
    "UT Rio Grande Valley": "UTRGV",
    "Illinois Chicago":     "UIC",
    "Texas A&M Corpus Chris": "Texas A&M-CC",
    "CSUN":                 "Cal State Northridge",
    "SIUE":                 "SIU Edwardsville",
    "Nebraska Omaha":       "Omaha",
    "Bethune Cookman":      "Bethune-Cookman",
    "Tennessee Martin":     "UT Martin",
    "Southeast Missouri":   "Southeast Missouri State",
    "Grambling St.":        "Grambling",
    "Loyola MD":            "Loyola Maryland",
    "Louisiana Monroe":     "ULM",
    "Saint Francis":        "Saint Francis (PA)",
    "Gardner Webb":         "Gardner-Webb",
    "IU Indy":              "IUPUI",
    "St. Bonaventure":      "Saint Bonaventure",
    "Furman":               "Furman",       # already fine, listed for completeness
    "Prairie View A&M":     "Prairie View A&M",  # fine after suffix strip
}


def normalise_kenpom_name(raw: str) -> str:
    """Strip conference suffix then apply abbreviation map."""
    name = _strip_suffix(raw)
    return _KENPOM_TO_CANONICAL.get(name, name)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

OUTPUT_PATH = "data_raw/kenpom_rankings.csv"


def scrape_kenpom_rankings():
    """Scrape the KenPom ratings table with headless Chrome. Returns a DataFrame or None."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        print("Selenium not installed. Install with: pip install selenium")
        return None

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = None
    try:
        print("Starting headless Chrome browser...")
        driver = webdriver.Chrome(options=opts)
        driver.get("https://kenpom.com/")

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "ratings-table"))
        )
        time.sleep(2)

        rows = driver.find_element(By.ID, "ratings-table").find_elements(By.TAG_NAME, "tr")
        print(f"Found {len(rows)} rows in table")

        data = []
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 4:
                continue
            rank = re.sub(r"[^\d]", "", cells[0].text.strip())
            team = cells[1].text.strip()

            record = ""
            try:
                wl = row.find_element(By.CLASS_NAME, "wl").text.strip()
                m = re.search(r"(\d+)-(\d+)", wl)
                if m:
                    record = f"{m.group(1)}-{m.group(2)}"
            except Exception:
                pass

            if rank and team:
                data.append({"kenpom_rank": int(rank), "team_kenpom": team, "record": record})

        if not data:
            print("No data found in table")
            return None

        df = pd.DataFrame(data)
        df["team_kenpom"] = df["team_kenpom"].apply(normalise_kenpom_name)
        print(f"Successfully parsed {len(df)} teams")
        return df

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

    if df is None or df.empty:
        if os.path.exists(OUTPUT_PATH):
            print(f"Failed to fetch new KenPom data. Keeping existing {OUTPUT_PATH}")
        else:
            print("Failed to fetch KenPom rankings.")
        return

    os.makedirs("data_raw", exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} KenPom rankings to {OUTPUT_PATH}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
