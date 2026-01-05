import os
import json
import pandas as pd


def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _ensure_standard_name(alias: pd.DataFrame) -> pd.DataFrame:
    alias = _norm_cols(alias)
    cols_lc = {c.lower(): c for c in alias.columns}

    if "standard_name" in cols_lc:
        alias = alias.rename(columns={cols_lc["standard_name"]: "standard_name"})
        alias["standard_name"] = alias["standard_name"].astype(str).str.strip()
        return alias

    for fallback in ("team", "school", "name"):
        if fallback in cols_lc:
            alias = alias.rename(columns={cols_lc[fallback]: "standard_name"})
            alias["standard_name"] = alias["standard_name"].astype(str).str.strip()
            return alias

    if len(alias.columns) >= 1:
        alias = alias.rename(columns={alias.columns[0]: "standard_name"})
        alias["standard_name"] = alias["standard_name"].astype(str).str.strip()
        return alias

    return pd.DataFrame({"standard_name": []})


def _pick_col(alias: pd.DataFrame, want: str) -> str | None:
    cols_lc = {c.lower(): c for c in alias.columns}
    if want.lower() in cols_lc:
        return cols_lc[want.lower()]
    return None


def _map_rank(alias: pd.DataFrame, ranks: pd.DataFrame, alias_col: str, rank_col: str, out_col: str) -> pd.Series:
    if alias_col not in alias.columns or ranks.empty:
        return pd.Series([pd.NA] * len(alias), index=alias.index)

    ranks = ranks.copy()
    ranks.columns = [c.strip() for c in ranks.columns]
    if alias_col not in alias.columns or rank_col not in ranks.columns:
        return pd.Series([pd.NA] * len(alias), index=alias.index)

    m = {}
    for _, r in ranks.iterrows():
        key = str(r[alias_col]).strip()
        val = str(r[rank_col]).strip()
        if key and val:
            m[key] = val

    return alias[alias_col].astype(str).str.strip().map(m)


def _record_from_games(games_path: str, alias: pd.DataFrame) -> pd.Series:
    if not os.path.exists(games_path):
        return pd.Series([""] * len(alias), index=alias.index)

    g = pd.read_csv(games_path)
    g.columns = [c.strip() for c in g.columns]
    if "Team" not in g.columns:
        return pd.Series([""] * len(alias), index=alias.index)

    if "Win?" in g.columns:
        win_col = "Win?"
    elif "Win" in g.columns:
        win_col = "Win"
    else:
        return pd.Series([""] * len(alias), index=alias.index)

    team_counts = {}
    for team, sub in g.groupby("Team"):
        w = (sub[win_col].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1", "W"])).sum()
        l = (sub[win_col].astype(str).str.upper().isin(["N", "NO", "FALSE", "0", "L"])).sum()
        team_counts[str(team).strip()] = f"{int(w)}-{int(l)}"

    if "game_log_name" in alias.columns:
        key_col = "game_log_name"
    else:
        key_col = "standard_name"

    return alias[key_col].astype(str).str.strip().map(team_counts).fillna("")


def main() -> int:
    alias = _read_csv("team_alias.csv")
    alias = _ensure_standard_name(alias)

    net = _read_csv(os.path.join("data_raw", "net.csv"))
    kenpom = _read_csv(os.path.join("data_raw", "kenpom.csv"))
    bpi = _read_csv(os.path.join("data_raw", "bpi.csv"))
    ap = _read_csv(os.path.join("data_raw", "ap.csv"))
    sos = _read_csv(os.path.join("data_raw", "sos.csv"))

    for df in (net, kenpom, bpi, ap, sos):
        if not df.empty:
            df.columns = [c.strip() for c in df.columns]

    net_name_col = _pick_col(alias, "net_name") or "standard_name"
    kp_name_col = _pick_col(alias, "kenpom_name") or "standard_name"
    bpi_name_col = _pick_col(alias, "bpi_name") or "standard_name"
    sos_name_col = _pick_col(alias, "sos_name") or "standard_name"
    ap_name_col = _pick_col(alias, "ap_name") or "standard_name"

    out = pd.DataFrame()
    out["Team"] = alias["standard_name"].astype(str).str.strip()

    out["Record"] = _record_from_games(os.path.join("data_raw", "games_2024.csv"), alias)

    if not ap.empty and "ap_name" in ap.columns and "ap_rank" in ap.columns:
        ap_map = {str(r["ap_name"]).strip(): str(r["ap_rank"]).strip() for _, r in ap.iterrows() if str(r["ap_name"]).strip()}
        out["AP"] = alias[ap_name_col].astype(str).str.strip().map(ap_map)
    else:
        out["AP"] = pd.NA

    if not net.empty and "net_name" in net.columns and "net_rank" in net.columns:
        net_map = {str(r["net_name"]).strip(): str(r["net_rank"]).strip() for _, r in net.iterrows() if str(r["net_name"]).strip()}
        out["NET"] = alias[net_name_col].astype(str).str.strip().map(net_map)
    else:
        out["NET"] = pd.NA

    if not kenpom.empty and "kenpom_name" in kenpom.columns and "kenpom_rank" in kenpom.columns:
        kp_map = {str(r["kenpom_name"]).strip(): str(r["kenpom_rank"]).strip() for _, r in kenpom.iterrows() if str(r["kenpom_name"]).strip()}
        out["KenPom"] = alias[kp_name_col].astype(str).str.strip().map(kp_map)
    else:
        out["KenPom"] = pd.NA

    if not bpi.empty and "bpi_name" in bpi.columns and "bpi_rank" in bpi.columns:
        bpi_map = {str(r["bpi_name"]).strip(): str(r["bpi_rank"]).strip() for _, r in bpi.iterrows() if str(r["bpi_name"]).strip()}
        out["BPI"] = alias[bpi_name_col].astype(str).str.strip().map(bpi_map)
    else:
        out["BPI"] = pd.NA

    if not sos.empty and "sos_name" in sos.columns and "sos_rank" in sos.columns:
        sos_map = {str(r["sos_name"]).strip(): str(r["sos_rank"]).strip() for _, r in sos.iterrows() if str(r["sos_name"]).strip()}
        out["SoS"] = alias[sos_name_col].astype(str).str.strip().map(sos_map)
    else:
        out["SoS"] = pd.NA

    for c in ["NET", "KenPom", "BPI", "SoS", "AP"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["AvgRankValue"] = out[["NET", "KenPom", "BPI"]].mean(axis=1, skipna=False)
    out["AVG"] = out["AvgRankValue"].rank(method="min", ascending=True)

    out = out.drop(columns=["AvgRankValue"])
    out["AVG"] = pd.to_numeric(out["AVG"], errors="coerce")

    out = out[["Team", "Record", "AVG", "AP", "NET", "KenPom", "BPI", "SoS"]]

    os.makedirs("docs", exist_ok=True)
    out.to_csv(os.path.join("docs", "rankings.csv"), index=False)
    out.to_csv(os.path.join("docs", "site_rankings.csv"), index=False)

    with open(os.path.join("docs", "rankings.json"), "w", encoding="utf-8") as f:
        json.dump(out.fillna("").to_dict(orient="records"), f)

    print(f"Wrote {len(out)} rows -> docs/rankings.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
