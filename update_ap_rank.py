import os
import sys
import json
import tempfile
import requests
import pandas as pd


URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/rankings"


def _atomic_write_csv(df: pd.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    d = os.path.dirname(out_path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".csv", newline="") as f:
        tmp = f.name
        df.to_csv(tmp, index=False)
    os.replace(tmp, out_path)


def _fetch_json(url: str) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.espn.com/",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def _find_ap_entries(obj):
    if isinstance(obj, dict):
        t = str(obj.get("type", "")).lower()
        name = str(obj.get("name", "")).lower()
        short = str(obj.get("shortName", "")).lower()

        if (t == "ap") or ("ap" in name) or ("ap" in short):
            for k in ("ranks", "entries", "rankings", "items"):
                v = obj.get(k)
                if isinstance(v, list) and v:
                    return v

        for v in obj.values():
            found = _find_ap_entries(v)
            if found:
                return found

    if isinstance(obj, list):
        for v in obj:
            found = _find_ap_entries(v)
            if found:
                return found

    return None


def _team_name(entry: dict) -> str | None:
    team = entry.get("team")
    if isinstance(team, dict):
        for k in ("displayName", "shortDisplayName", "name", "location"):
            v = team.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    for k in ("displayName", "shortDisplayName", "name"):
        v = entry.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _rank(entry: dict) -> int | None:
    for k in ("current", "rank", "rnk"):
        v = entry.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return None


def main() -> int:
    out_path = os.path.join("data_raw", "ap.csv")

    try:
        data = _fetch_json(URL)
        entries = _find_ap_entries(data)
        if not entries:
            raise RuntimeError("Could not locate AP entries in ESPN rankings JSON")

        rows = []
        for e in entries:
            if not isinstance(e, dict):
                continue
            rk = _rank(e)
            nm = _team_name(e)
            if rk is None or nm is None:
                continue
            rows.append((nm, rk))

        df = pd.DataFrame(rows, columns=["ap_name", "ap_rank"]).drop_duplicates("ap_rank").sort_values("ap_rank")
        df = df[df["ap_rank"].between(1, 25)].reset_index(drop=True)

        _atomic_write_csv(df, out_path)
        print(f"Wrote {len(df)} rows -> {out_path}")
        return 0

    except Exception as e:
        print(f"update_ap_rank failed: {e}", file=sys.stderr)
        if os.path.exists(out_path):
            print(f"Keeping existing file -> {out_path}")
            return 0
        empty = pd.DataFrame(columns=["ap_name", "ap_rank"])
        _atomic_write_csv(empty, out_path)
        print(f"Wrote 0 rows -> {out_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
