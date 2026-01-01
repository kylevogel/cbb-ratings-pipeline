from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
import re
import difflib
import pandas as pd
import requests

URL = "https://www.warrennolan.com/basketball/2026/sos-rpi-predict"

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[\.\(\)\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in tup if x and "Unnamed" not in str(x)]).strip() for tup in df.columns]
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
    return df

def _load_alias_map(root: Path) -> tuple[dict[str, str], list[str]]:
    alias_path = root / "data_raw" / "team_alias.csv"
    if not alias_path.exists():
        alias_path = root / "team_alias.csv"
    df = pd.read_csv(alias_path, dtype=str).fillna("")
    cols = [c for c in df.columns if c != "standard_name"]

    m: dict[str, str] = {}
    standards: list[str] = []

    for _, r in df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        standards.append(std)
        m[_norm(std)] = std

        for c in cols:
            val = str(r.get(c, "")).strip()
            if not val:
                continue
            for piece in [p.strip() for p in val.split("|") if p.strip()]:
                m[_norm(piece)] = std

    return m, standards

def _pick_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        t = _flatten_cols(t)
        low = [str(c).strip().lower() for c in t.columns]
        if "rank" in low and "team" in low:
            return t
    raise RuntimeError("Could not find SOS table with columns including 'Rank' and 'Team'.")

def _map_team(name: str, alias_map: dict[str, str], standards: list[str]) -> str:
    k = _norm(name)
    if k in alias_map:
        return alias_map[k]

    guess = difflib.get_close_matches(name, standards, n=1, cutoff=0.92)
    if guess:
        return guess[0]

    guess2 = difflib.get_close_matches(_norm(name), [_norm(x) for x in standards], n=1, cutoff=0.92)
    if guess2:
        inv = { _norm(x): x for x in standards }
        return inv.get(guess2[0], name)

    return name

def main() -> None:
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=45)
    r.raise_for_status()

    root = Path(__file__).resolve().parent
    alias_map, standards = _load_alias_map(root)

    tables = pd.read_html(StringIO(r.text))
    t = _pick_table(tables)

    team_col = next((c for c in t.columns if str(c).strip().lower() == "team"), None)
    rank_col = next((c for c in t.columns if str(c).strip().lower() == "rank"), None)
    if team_col is None or rank_col is None:
        raise RuntimeError("Found SOS table but could not locate 'Team' and 'Rank' columns.")

    out = pd.DataFrame({
        "Team": t[team_col].astype(str).str.strip(),
        "SOS": pd.to_numeric(t[rank_col], errors="coerce"),
    }).dropna(subset=["SOS"])

    out["Team"] = out["Team"].map(lambda x: _map_team(x, alias_map, standards))

    out["SOS"] = out["SOS"].astype(int)
    out = out.sort_values("SOS").reset_index(drop=True)

    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    out.insert(0, "snapshot_date", now_et.strftime("%Y-%m-%d"))

    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)
    path = data_raw / "SOS_Rank.csv"
    out.to_csv(path, index=False)

    unmatched = sorted(set(out.loc[~out["Team"].isin(standards), "Team"].tolist()))
    if unmatched:
        (data_raw / "unmatched_sos.txt").write_text("\n".join(unmatched) + "\n", encoding="utf-8")

    print(path.name)
    print(",".join(out.columns.tolist()))
    print(out.head(5).to_csv(index=False).strip())

if __name__ == "__main__":
    main()
