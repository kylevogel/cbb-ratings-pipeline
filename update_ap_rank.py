import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

AP_URL = "https://www.ncaa.com/rankings/basketball-men/d1/associated-press"

def clean_team(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)  # remove first-place votes like "(35)"
    s = re.sub(r"\s+", " ", s)
    return s

def main():
    r = requests.get(AP_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    table = None
    for t in soup.find_all("table"):
        head = t.find("thead")
        if not head:
            continue
        hdr = head.get_text(" ", strip=True).lower()
        if "rank" in hdr and ("school" in hdr or "team" in hdr):
            table = t
            break

    if table is None:
        raise RuntimeError("Could not find AP rankings table")

    body = table.find("tbody")
    if body is None:
        raise RuntimeError("AP rankings table missing tbody")

    rows = []
    for tr in body.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue

        rank_txt = tds[0].get_text(" ", strip=True)
        team_txt = tds[1].get_text(" ", strip=True)

        m = re.search(r"\d+", rank_txt)
        if not m:
            continue

        rk = int(m.group(0))
        team = clean_team(team_txt)
        if team:
            rows.append({"AP_Rank": rk, "Team": team})

    out = pd.DataFrame(rows).drop_duplicates(subset=["AP_Rank"], keep="first").sort_values("AP_Rank")

    out_dir = Path("data_raw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "AP_Rank.csv"
    out.to_csv(out_path, index=False)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Wrote {out_path} ({len(out)} rows) at {stamp}")

if __name__ == "__main__":
    main()
