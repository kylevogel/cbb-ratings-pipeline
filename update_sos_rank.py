from pathlib import Path
import re
import pandas as pd
import requests

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace("’", "'")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _resolve_standard(raw: str, alias_map: dict) -> str:
    raw = "" if raw is None else str(raw).strip()
    if not raw:
        return ""

    candidates = [raw]

    # Common: State <-> St.
    candidates.append(re.sub(r"\bState\b", "St.", raw))
    candidates.append(re.sub(r"\bSt\.?\b", "State", raw))

    # California wording variants
    candidates.append(re.sub(r"\bCal State\b", "Cal St.", raw))
    candidates.append(re.sub(r"\bCal St\.?\b", "Cal State", raw))

    # NC State variants
    if _norm(raw) in (_norm("North Carolina State"), _norm("NC State"), _norm("N.C. State")):
        candidates.extend(["N.C. State", "NC State", "North Carolina State"])

    # Special one-offs (these show up a lot)
    if "northridge" in raw.lower():
        candidates.append("CSUN")
    if raw.strip().lower() == "detroit":
        candidates.append("Detroit Mercy")

    # Try all candidates
    for c in candidates:
        std = _resolve_standard(raw, alias_map)
        if std:
            return std

    return ""

def _variants(cell: str) -> list[str]:
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p]
    return parts if parts else []

def _load_alias_maps(root: Path):
    alias_path = root / "team_alias.csv"
    alias_df = pd.read_csv(alias_path, dtype=str, keep_default_na=False).fillna("")
    exact = {}
    norm = {}

    for _, row in alias_df.iterrows():
        std = (row.get("standard_name") or "").strip()
        if not std:
            continue

        cols = ["standard_name", "kenpom_name", "bpi_name", "net_name", "game_log_name"]
        for c in cols:
            val = row.get(c, "")
            for v in _variants(val):
                if v not in exact:
                    exact[v] = std
                nv = _norm(v)
                if nv and nv not in norm:
                    norm[nv] = std

    return exact, norm

def _fetch_sos_table() -> pd.DataFrame:
    url = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("No tables found on WarrenNolan SOS page")

    best = None
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if any("team" in c for c in cols) and any("rank" in c for c in cols):
            best = t
            break
    return best if best is not None else tables[0]

def main():
    root = Path(__file__).resolve().parent
    exact_map, norm_map = _load_alias_maps(root)

    df = _fetch_sos_table()
    cols = [str(c) for c in df.columns]

    team_col = None
    rank_col = None

    for c in cols:
        if "team" in c.lower():
            team_col = c
        if c.lower() == "rank" or "rank" in c.lower():
            rank_col = c

    if team_col is None:
        team_col = cols[0]
    if rank_col is None:
        rank_col = cols[1] if len(cols) > 1 else cols[0]

    snapshot_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    out_rows = []
    unmatched = []

    for _, row in df.iterrows():
        team_raw = str(row.get(team_col, "")).strip()
        rank_raw = str(row.get(rank_col, "")).strip()

        if not team_raw or team_raw.lower() in {"nan", "none"}:
            continue

        rank_raw = re.sub(r"[^\d]", "", rank_raw)
        if not rank_raw:
            continue
        sos_rank = int(rank_raw)

        std = exact_map.get(team_raw)
        if std is None:
            std = norm_map.get(_norm(team_raw))

        if std is None:
            unmatched.append({"snapshot_date": snapshot_date, "Team": team_raw, "SoS": sos_rank})
            continue

        out_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": sos_rank})

    out_df = pd.DataFrame(out_rows)
    out_path = root / "data_raw" / "SOS_Rank.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)

    unmatched_df = pd.DataFrame(unmatched)
    unmatched_path = root / "unmatched_sos_teams.csv"
    unmatched_df.to_csv(unmatched_path, index=False)

    print("SOS_Rank.csv")
    print(unmatched_path.name)
    if not unmatched_df.empty:
        print(unmatched_df.head(25).to_csv(index=False).strip())

if __name__ == "__main__":
    main()
