import os
import re
import json
import datetime as dt
from io import StringIO
from pathlib import Path
from difflib import SequenceMatcher

import pandas as pd
import requests


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = s.replace("\xa0", " ")
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    swaps = {
        "uconn": "connecticut",
        "umass": "massachusetts",
        "fau": "florida atlantic",
        "etsu": "east tennessee st",
        "sfa": "stephen f austin",
        "uni": "northern iowa",
        "niu": "northern illinois",
        "uiw": "incarnate word",
        "umkc": "kansas city",
        "uncg": "unc greensboro",
        "nccu": "north carolina central",
        "ncat": "north carolina a t",
        "nc a t": "north carolina a t",
        "ut martin": "tennessee martin",
        "seattle u": "seattle",
        "southern u": "southern",
        "penn state": "penn st",
        "utah state": "utah st",
    }

    toks = s.split()
    out = []
    i = 0
    while i < len(toks):
        if i + 1 < len(toks):
            two = f"{toks[i]} {toks[i+1]}"
            if two in swaps:
                out.extend(swaps[two].split())
                i += 2
                continue
        one = toks[i]
        if one in swaps:
            out.extend(swaps[one].split())
        else:
            out.append(one)
        i += 1

    s = " ".join(out)
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bstate\b", "st", s)
    s = re.sub(r"\bst\.\b", "st", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _score(a: str, b: str) -> float:
    an = _norm(a)
    bn = _norm(b)
    if not an or not bn:
        return 0.0
    if an == bn:
        return 1.0
    seq = SequenceMatcher(None, an, bn).ratio()
    at = set(an.split())
    bt = set(bn.split())
    j = len(at & bt) / max(1, len(at | bt))
    return max(seq, j)


def _extract_next_data(html: str):
    m = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.S)
    if not m:
        return None
    raw = (m.group(1) or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _walk(obj):
    stack = [obj]
    while stack:
        x = stack.pop()
        yield x
        if isinstance(x, dict):
            stack.extend(list(x.values()))
        elif isinstance(x, list):
            stack.extend(x)


def _get_team_str(x):
    if isinstance(x, str):
        return x.strip()
    if isinstance(x, dict):
        for k in ["team", "teamName", "team_name", "name", "school", "displayName", "shortName"]:
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in x.values():
            t = _get_team_str(v)
            if t:
                return t
    if isinstance(x, list):
        for v in x:
            t = _get_team_str(v)
            if t:
                return t
    return ""


def _get_rank_int(d):
    if not isinstance(d, dict):
        return None
    for k in ["rank", "rk", "sosRank", "sos_rank", "sosrank", "Rank", "Rk", "#"]:
        v = d.get(k)
        try:
            if v is None:
                continue
            iv = int(str(v).strip())
            if iv > 0:
                return iv
        except Exception:
            pass
    for k, v in d.items():
        if isinstance(k, str) and "rank" in k.lower():
            try:
                iv = int(str(v).strip())
                if iv > 0:
                    return iv
            except Exception:
                pass
    return None


def _candidate_rows_from_next_data(next_data):
    best = []
    for x in _walk(next_data):
        if not isinstance(x, list) or len(x) < 250:
            continue
        if not all(isinstance(v, dict) for v in x[:25]):
            continue
        rows = []
        for d in x:
            rk = _get_rank_int(d)
            team = _get_team_str(d)
            if rk is None or not team:
                continue
            rows.append((rk, team))
        if len(rows) > len(best):
            best = rows
    return best


def _pick_table(tables):
    best = None
    best_len = -1
    for t in tables:
        cols = [str(c).lower().strip() for c in t.columns]
        has_team = any("team" in c for c in cols)
        has_rank = any(c in ("rank", "rk", "#") or "rank" in c for c in cols)
        if has_team and has_rank and len(t) > best_len:
            best = t
            best_len = len(t)
    if best is None:
        raise SystemExit("Could not find a SoS table with Team + Rank columns")
    return best


def _load_standards(team_alias_path: Path):
    df = pd.read_csv(team_alias_path, dtype=str).fillna("")
    if "standard_name" not in df.columns:
        raise SystemExit("team_alias.csv must contain a 'standard_name' column")

    has_sos = "sos_name" in df.columns

    standards = []
    desired = {}
    for _, r in df.iterrows():
        std = str(r.get("standard_name", "")).strip()
        if not std:
            continue
        standards.append(std)
        if has_sos:
            sn = str(r.get("sos_name", "")).strip()
            desired[std] = sn if sn else std
        else:
            desired[std] = std

    return standards, desired, has_sos


def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_raw.mkdir(parents=True, exist_ok=True)

    season = int(os.getenv("SEASON", "2026"))
    url = f"https://www.warrennolan.com/basketball/{season}/sos-rpi-predict"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.warrennolan.com/",
    }

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"Failed to fetch SoS page: HTTP {r.status_code}")

    source_rows = []
    next_data = _extract_next_data(r.text)
    if next_data is not None:
        cand = _candidate_rows_from_next_data(next_data)
        if cand:
            source_rows = cand

    if not source_rows:
        tables = pd.read_html(StringIO(r.text))
        t = _pick_table(tables)
        cols = {str(c).lower().strip(): c for c in t.columns}

        team_col = None
        for k, v in cols.items():
            if "team" in k:
                team_col = v
                break
        if team_col is None:
            raise SystemExit("Could not identify Team column")

        rank_col = None
        for k, v in cols.items():
            if k in ("rank", "rk", "#") or "rank" in k:
                rank_col = v
                break
        if rank_col is None:
            rank_col = t.columns[0]

        tmp = t[[rank_col, team_col]].copy()
        tmp.columns = ["SoS", "Team"]
        tmp["Team"] = tmp["Team"].astype(str).str.strip()
        tmp["SoS"] = pd.to_numeric(tmp["SoS"], errors="coerce")
        tmp = tmp.dropna(subset=["SoS", "Team"])
        tmp["SoS"] = tmp["SoS"].astype(int)
        source_rows = [(int(a), str(b)) for a, b in zip(tmp["SoS"].tolist(), tmp["Team"].tolist())]

    src_df = pd.DataFrame(source_rows, columns=["SoS", "source_team"]).dropna()
    src_df["source_team"] = src_df["source_team"].astype(str).str.strip()
    src_df["SoS"] = pd.to_numeric(src_df["SoS"], errors="coerce")
    src_df = src_df.dropna(subset=["SoS", "source_team"])
    src_df["SoS"] = src_df["SoS"].astype(int)
    src_df = src_df[src_df["SoS"] > 0].copy()

    if len(src_df) < 330:
        raise SystemExit(f"SoS scrape too small ({len(src_df)} rows)")

    src_out = src_df.sort_values("SoS")[["source_team", "SoS"]].copy()
    (data_raw / "sos_source_teams.csv").write_text(src_out.to_csv(index=False), encoding="utf-8")

    team_alias_path = root / "team_alias.csv"
    standards, desired_map, has_sos = _load_standards(team_alias_path)

    src_names = src_df["source_team"].tolist()
    src_norm = {n: _norm(n) for n in src_names}
    src_by_norm = {}
    for n in src_names:
        k = src_norm[n]
        src_by_norm.setdefault(k, []).append(n)

    used_src = set()
    matched_rows = []
    unmatched_rows = []
    collisions = []

    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()

    for std in standards:
        desired = desired_map.get(std, std)

        dkey = _norm(desired)
        pick = None
        pick_score = 0.0

        if dkey in src_by_norm:
            for n in src_by_norm[dkey]:
                if n not in used_src:
                    pick = n
                    pick_score = 1.0
                    break

        if pick is None:
            scored = []
            for n in src_names:
                if n in used_src:
                    continue
                s = _score(desired, n)
                if s > 0:
                    scored.append((s, n))
            scored.sort(reverse=True, key=lambda x: x[0])

            if scored:
                best_s, best_n = scored[0]
                if best_s >= 0.72:
                    pick = best_n
                    pick_score = float(best_s)
                else:
                    tops = scored[:8]
                    cand_str = " | ".join([f"{x[1]}:{x[0]:.3f}" for x in tops])
                    unmatched_rows.append(
                        {
                            "standard_name": std,
                            "desired_source": desired,
                            "best_candidate": best_n,
                            "best_score": float(best_s),
                            "candidates": cand_str,
                        }
                    )
                    continue
            else:
                unmatched_rows.append(
                    {
                        "standard_name": std,
                        "desired_source": desired,
                        "best_candidate": "",
                        "best_score": 0.0,
                        "candidates": "",
                    }
                )
                continue

        if pick in used_src:
            collisions.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "matched_source": pick,
                    "matched_rank": int(src_df.loc[src_df["source_team"].eq(pick), "SoS"].iloc[0]),
                    "match_score": float(pick_score),
                    "note": "already used",
                }
            )
            unmatched_rows.append(
                {
                    "standard_name": std,
                    "desired_source": desired,
                    "best_candidate": pick,
                    "best_score": float(pick_score),
                    "candidates": "",
                }
            )
            continue

        used_src.add(pick)
        rk = int(src_df.loc[src_df["source_team"].eq(pick), "SoS"].iloc[0])
        matched_rows.append({"snapshot_date": snapshot_date, "Team": std, "SoS": rk})

    out_df = pd.DataFrame(matched_rows, columns=["snapshot_date", "Team", "SoS"])
    out_df.to_csv(data_raw / "SOS_Rank.csv", index=False)

    um_df = pd.DataFrame(unmatched_rows, columns=["standard_name", "desired_source", "best_candidate", "best_score", "candidates"])
    um_df.to_csv(data_raw / "unmatched_sos_teams.csv", index=False)

    col_df = pd.DataFrame(collisions, columns=["standard_name", "desired_source", "matched_source", "matched_rank", "match_score", "note"])
    col_df.to_csv(data_raw / "sos_collisions.csv", index=False)

    print("SOS_Rank.csv")
    print("unmatched_sos_teams.csv")
    print("sos_collisions.csv")
    print(f"Pulled source teams: {len(src_df)}")
    print(f"Matched: {len(out_df)}")
    print(f"Unmatched: {len(um_df)}")
    print(f"Collisions: {len(col_df)}")
    if not has_sos:
        print("team_alias.csv has no sos_name column. You can add one to force exact WarrenNolan names.")


if __name__ == "__main__":
    main()
