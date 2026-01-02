import datetime as dt
import io
import re
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests


SEASON = 2026
SOURCE_URL = f"https://www.warrennolan.com/basketball/{SEASON}/sos-rpi-predict"


def _root() -> Path:
    return Path(__file__).resolve().parent


def _data_raw(root: Path) -> Path:
    p = root / "data_raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.warrennolan.com/",
        }
    )
    return s


def _get(url: str, s: requests.Session, timeout: int = 25) -> str:
    r = s.get(url, timeout=timeout)
    if r.status_code != 200:
        return ""
    return r.text or ""


def _discover_candidates(html: str) -> list[str]:
    cands = []

    for m in re.findall(r'["\'](\/_ajax\/[^"\']+)["\']', html or ""):
        if "sos" in m and "predict" in m:
            cands.append("https://www.warrennolan.com" + m)

    guesses = [
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict/",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict?sort=rank",
        f"https://www.warrennolan.com/_ajax/basketball/{SEASON}/sos-rpi-predict?view=all",
    ]
    cands.extend(guesses)

    out = []
    seen = set()
    for u in cands:
        u2 = u.strip()
        if u2 and u2 not in seen:
            seen.add(u2)
            out.append(u2)
    return out


def _pick_table(html: str) -> pd.DataFrame:
    if not html:
        return pd.DataFrame()

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        return pd.DataFrame()

    best = pd.DataFrame()
    best_rows = 0

    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("team" in c for c in cols) and any(c == "rank" or "rank" in c for c in cols):
            if len(t) > best_rows:
                best = t.copy()
                best_rows = len(t)

    return best


def _norm(s: str) -> str:
    x = (s or "").lower().strip()
    x = x.replace("&", " and ")
    x = re.sub(r"[\.\,\(\)\[\]\{\}\-\/']", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    x = re.sub(r"\bstate\b", "st", x)
    x = re.sub(r"\bst\b", "st", x)
    x = re.sub(r"\bsaint\b", "st", x)
    x = re.sub(r"\bmt\b", "mount", x)

    x = re.sub(r"\bnc\b", "north carolina", x)
    x = re.sub(r"\bsc\b", "south carolina", x)
    x = re.sub(r"\bnd\b", "north dakota", x)
    x = re.sub(r"\bsd\b", "south dakota", x)
    x = re.sub(r"\bnm\b", "new mexico", x)

    x = re.sub(r"\s+", " ", x).strip()
    return x


def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _load_alias_df(root: Path) -> pd.DataFrame:
    p = root / "team_alias.csv"
    if not p.exists():
        raise SystemExit("team_alias.csv not found")
    df = pd.read_csv(p, dtype=str).fillna("")
    if "standard_name" not in df.columns:
        raise SystemExit("team_alias.csv missing standard_name column")
    if "sos_name" not in df.columns:
        df["sos_name"] = ""
    return df


def _build_alias_maps(alias_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    alias_to_std = {}
    std_norm_to_std = {}

    for _, r in alias_df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        std_norm = _norm(std)
        std_norm_to_std[std_norm] = std

        alias_to_std[_norm(std)] = std

        sos = str(r.get("sos_name", "")).strip()
        if sos:
            alias_to_std[_norm(sos)] = std

    return alias_to_std, std_norm_to_std


def _canon_team(name: str, alias_to_std: dict[str, str], std_norm_to_std: dict[str, str]) -> tuple[str, str, float]:
    raw = (name or "").strip()
    if not raw:
        return "", "", 0.0

    k = _norm(raw)
    if k in alias_to_std:
        return alias_to_std[k], raw, 1.0

    best_std = ""
    best_score = 0.0
    for kk, std in std_norm_to_std.items():
        sc = _ratio(k, kk)
        if sc > best_score:
            best_score = sc
            best_std = std

    if best_score >= 0.78:
        return best_std, raw, best_score

    return raw, raw, best_score


def _extract_rank_team(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Team", "SoS"])

    cols = {str(c).strip(): str(c).strip().lower() for c in df.columns}

    rank_col = None
    team_col = None

    for orig, low in cols.items():
        if team_col is None and "team" in low:
            team_col = orig
        if rank_col is None and (low == "rank" or "rank" in low):
            rank_col = orig

    if rank_col is None or team_col is None:
        return pd.DataFrame(columns=["Team", "SoS"])

    out = df[[rank_col, team_col]].copy()
    out.columns = ["SoS", "Team"]

    out["Team"] = out["Team"].astype(str).str.strip()

    out["SoS"] = (
        out["SoS"]
        .astype(str)
        .str.replace(r"[^\d]+", "", regex=True)
        .replace("", pd.NA)
    )
    out["SoS"] = pd.to_numeric(out["SoS"], errors="coerce")
    out = out.dropna(subset=["SoS", "Team"])
    out["SoS"] = out["SoS"].astype(int)

    out = out[~out["Team"].eq("")].copy()
    out = out.drop_duplicates(subset=["Team"], keep="first").copy()
    out = out.sort_values("SoS").reset_index(drop=True)

    return out[["Team", "SoS"]]


def _fetch_best_table(s: requests.Session) -> pd.DataFrame:
    html = _get(SOURCE_URL, s)
    cands = [SOURCE_URL] + _discover_candidates(html)

    best = pd.DataFrame()
    best_n = 0

    for u in cands:
        h = html if u == SOURCE_URL else _get(u, s)
        t = _pick_table(h)
        e = _extract_rank_team(t)
        if len(e) > best_n:
            best = e
            best_n = len(e)

        if best_n >= 350:
            break

    return best


def main() -> None:
    root = _root()
    data_raw = _data_raw(root)

    alias_df = _load_alias_df(root)
    alias_to_std, std_norm_to_std = _build_alias_maps(alias_df)

    s = _session()
    base = _fetch_best_table(s)
    if base.empty:
        raise SystemExit("SoS scrape failed: could not find a Rank/Team table.")

    mapped_team = []
    src_team = []
    suggested = []
    score = []

    for t in base["Team"].tolist():
        std, src, sc = _canon_team(t, alias_to_std, std_norm_to_std)
        mapped_team.append(std)
        src_team.append(src)
        score.append(sc)
        suggested.append(std if sc < 1.0 else "")

    df = pd.DataFrame({"Team": mapped_team, "SoS": base["SoS"].astype(int).tolist()})
    df = df.drop_duplicates(subset=["Team"], keep="first").copy()
    df = df[~df["Team"].eq("")].copy()

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    df.insert(0, "snapshot_date", snapshot_date)

    out_path = data_raw / "SOS_Rank.csv"
    df.to_csv(out_path, index=False)

    unmatched_rows = []
    for t, sc, src, sug in zip(mapped_team, score, src_team, suggested):
        if not t or sc < 0.78:
            unmatched_rows.append(
                {
                    "source_team": src,
                    "suggested_standard": t if sc >= 0.60 else "",
                    "match_score": float(sc),
                }
            )

    unmatched_path = data_raw / "unmatched_sos_teams.csv"
    pd.DataFrame(unmatched_rows).to_csv(unmatched_path, index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("rows", len(df))
    if len(df) < 330:
        raise SystemExit(f"SoS scrape returned only {len(df)} teams. The page may be paginated/JS-loaded; we may need the ajax endpoint.")


if __name__ == "__main__":
    main()
