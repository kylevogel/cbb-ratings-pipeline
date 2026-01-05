import re
import sys
from typing import Optional, List

import pandas as pd
import requests

OUT_PATH = "data_raw/ap.csv"


def _clean_team(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s)).strip()
    s = re.sub(r"\s*\(\d+\)\s*$", "", s).strip()
    s = re.sub(r"^[A-Z]{2,6}\s+", "", s).strip()
    return s


def _pick_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any(c in {"rk", "rank"} for c in cols) and any("team" in c for c in cols):
            return t
    return None


def main() -> int:
    url = "https://www.espn.com/mens-college-basketball/rankings"
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        tables = pd.read_html(r.text)
        t = _pick_table(tables)
        if t is None:
            out = pd.DataFrame(columns=["ap_name", "ap_rank"])
        else:
            cols = list(t.columns)
            rk_col = None
            team_col = None
            for c in cols:
                if str(c).strip().lower() in {"rk", "rank"}:
                    rk_col = c
                if "team" in str(c).strip().lower():
                    team_col = c
            if rk_col is None or team_col is None:
                out = pd.DataFrame(columns=["ap_name", "ap_rank"])
            else:
                out = pd.DataFrame()
                out["ap_rank"] = pd.to_numeric(t[rk_col], errors="coerce").astype("Int64")
                out["ap_name"] = t[team_col].map(_clean_team)
                out = out.dropna(subset=["ap_rank", "ap_name"]).drop_duplicates(subset=["ap_name"])
                out = out.sort_values("ap_rank", kind="stable")
                out = out[["ap_name", "ap_rank"]]
    except Exception as e:
        print(f"update_ap_rank warning: {e}", file=sys.stderr)
        out = pd.DataFrame(columns=["ap_name", "ap_rank"])

    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {len(out)} rows -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
