import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

AP_URL = "https://www.ncaa.com/rankings/basketball-men/d1/associated-press"

def clean_team(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)
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
