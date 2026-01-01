from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone, timedelta
from io import StringIO
import re
import difflib

import pandas as pd
import requests

URL = "https://www.espn.com/mens-college-basketball/bpi"


def _norm(x: object) -> str:
    s = "" if x is None else str(x)
    s = s.strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"[\u2019\u2018']", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _find_alias_path(root: Path) -> Path:
    p1 = root / "team_alias.csv"
    p2 = root / "data_raw" / "team_alias.csv"
    if p2.exists():
        return p2
    return p1


def _load_alias_df(root: Path) -> pd.DataFrame:
    path = _find_alias_path(root)
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    return df


def _split_aliases(cell: str) -> list[str]:
    s = (cell or "").strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split("|")]
    return [p for p in parts if p]


def _build_variant_map(alias_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    v2std: dict[str, str] = {}
    std_key2std: dict[str, str] = {}

    if "standard_name" not in alias_df.columns:
        raise RuntimeError("team_alias.csv must contain a 'standard_name' column")

    for _, row in alias_df.iterrows():
        std = str(row.get("standard_name", "")).strip()
        if not std:
            continue

        stdk = _norm(std)
        if stdk and stdk not in std_key2std:
            std_key2std[stdk] = std

        for col in alias_df.columns:
            val = str(row.get(col, "")).strip()
            if not val:
                continue
            for piece in _split_aliases(val) + ([val] if "|" not in val else []):
                k = _norm(piece)
                if k and k not in v2std:
                    v2std[k] = std

    return v2std, std_key2std


def _map_team(name: str, v2std: dict[str, str], std_key2std: dict[str, str]) -> tuple[str | None, float]:
    k = _norm(name)
    if not k:
        return None, 0.0
    if k in v2std:
        return v2std[k], 1.0
    if k in std_key2std:
        return std_key2std[k], 1.0

    std_keys = list(std_key2std.keys())
    match = difflib.get_close_matches(k, std_keys, n=1, cutoff=0.88)
    if not match:
        return None, 0.0

    m = match[0]
    score = difflib.SequenceMatcher(None, k, m).ratio()
    return std_key2std[m], score


def _pick_bpi_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        low = [c.lower() for c in cols]
        if ("team" in low or "teams" in low) and ("rk" in low or "rank" in low):
            t = t.copy()
            t.columns = cols
            return t
    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        low = [c.lower() for c in cols]
        if "team" in low or "teams" in low:
            t = t.copy()
            t.columns = cols
            return t
    raise RuntimeError("Could not find a BPI table on ESPN.")


def main() -> None:
    root = Path(__file__).resolve().parent
    alias_df = _load_alias_df(root)
    v2std, std_key2std = _build_variant_map(alias_df)
    std_set = set(alias_df["standard_name"].astype(str).str.strip().tolist())

    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    r = requests.get(URL, headers=headers, timeout=45)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    t = _pick_bpi_table(tables)

    col_low = {str(c).strip().lower(): c for c in t.columns}
    team_col = col_low.get("team") or col_low.get("teams")
    rk_col = col_low.get("rk") or col_low.get("rank")

    if team_col is None:
        raise RuntimeError("Found BPI table but could not locate a 'Team' column.")

    out = t.copy()
    out = out[[c for c in [rk_col, team_col] if c is not None]].copy()

    if rk_col is None:
        out.insert(0, "BPI", range(1, len(out) + 1))
        out.rename(columns={team_col: "Team_raw"}, inplace=True)
    else:
        out.rename(columns={rk_col: "BPI", team_col: "Team_raw"}, inplace=True)

    out["BPI"] = pd.to_numeric(out["BPI"], errors="coerce")
    out = out.dropna(subset=["BPI"])
    out["BPI"] = out["BPI"].astype(int)

    mapped = []
    unmatched_rows = []
    for raw in out["Team_raw"].astype(str).tolist():
        m, score = _map_team(raw, v2std, std_key2std)
        if m is None:
            unmatched_rows.append((raw, "", 0.0))
            mapped.append(raw)
        else:
            if m not in std_set:
                unmatched_rows.append((raw, m, float(score)))
                mapped.append(m)
            else:
                mapped.append(m)

    out["Team"] = mapped
    out = out.drop(columns=["Team_raw"])

    out = out[out["Team"].isin(std_set)].copy()
    out = out.groupby("Team", as_index=False)["BPI"].min()
    out = out.sort_values("BPI").reset_index(drop=True)

    now_et = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5)))
    out.insert(0, "snapshot_date", now_et.strftime("%Y-%m-%d"))

    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)
    out_path = data_raw / "BPI_Rank.csv"
    out.to_csv(out_path, index=False)

    um = pd.DataFrame(unmatched_rows, columns=["source_team", "suggested_standard", "match_score"])
    um = um.drop_duplicates().sort_values(["match_score", "source_team"], ascending=[False, True])
    um_path = data_raw / "unmatched_bpi_teams.csv"
    um.to_csv(um_path, index=False)

    print(out_path.name)
    print(um_path.name)
    print(",".join(out.columns.tolist()))
    print(out.head(5).to_csv(index=False).strip())


if __name__ == "__main__":
    main()
