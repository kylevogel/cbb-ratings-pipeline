from pathlib import Path
import pandas as pd

def pick_games_file(data_raw: Path) -> Path:
    candidates = [
        data_raw / "games_2024.csv",
        data_raw / "games_2024_clean_no_ids.csv",
        data_raw / "games_2024_clean.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    raise RuntimeError(
        "Could not find a games file in data_raw/. Looked for: "
        + ", ".join([c.name for c in candidates])
        + f"\nFiles present: {[p.name for p in data_raw.glob('*.csv')]}"
    )

def load_alias_map(path: Path) -> dict:
    df = pd.read_csv(path).fillna("")
    cols = [c.lower().strip() for c in df.columns]

    if "standard_name" not in cols:
        raise RuntimeError(f"team_alias.csv must contain standard_name. Found: {df.columns.tolist()}")

    # Find the real column names (case safe)
    col_map = {c.lower().strip(): c for c in df.columns}
    std_col = col_map["standard_name"]

    # These may or may not exist, depending on your file
    possible = ["game_log_name", "net_name", "bpi_name", "kenpom_name", "standard_name"]
    present = [col_map[p] for p in possible if p in col_map]

    alias = {}
    for _, r in df.iterrows():
        std = str(r[std_col]).strip()
        if not std:
            continue
        for c in present:
            val = str(r[c]).strip()
            if val:
                alias[val] = std
    return alias

def apply_alias(series: pd.Series, alias: dict) -> pd.Series:
    def f(x):
        s = str(x).strip()
        return alias.get(s, s)
    return series.astype(str).map(f)

def prep_rank_file(path: Path, alias: dict, team_col: str, rank_col: str, std_name: str) -> pd.DataFrame:
    df = pd.read_csv(path).copy()
    if team_col not in df.columns or rank_col not in df.columns:
        raise RuntimeError(f"{path.name} missing needed cols. Need {team_col} and {rank_col}. Found {df.columns.tolist()}")

    df[std_name] = apply_alias(df[team_col], alias)
    df[rank_col] = pd.to_numeric(df[rank_col], errors="coerce")
    df = df.dropna(subset=[rank_col])
    df = df.drop_duplicates(subset=[std_name])
    return df[[std_name, rank_col] + (["snapshot_date"] if "snapshot_date" in df.columns else [])]

def main():
    root = Path(__file__).resolve().parent
    data_raw = root / "data_raw"
    data_processed = root / "data_processed"
    data_processed.mkdir(exist_ok=True)

    games_path = pick_games_file(data_raw)

    alias_path = None
    for p in [data_raw / "team_alias.csv", root / "team_alias.csv"]:
        if p.exists():
            alias_path = p
            break
    if alias_path is None:
        raise RuntimeError("Could not find team_alias.csv in data_raw/ or project root.")

    alias = load_alias_map(alias_path)

    games = pd.read_csv(games_path).copy()
    if "Team" not in games.columns or "Opponent" not in games.columns:
        raise RuntimeError(f"Games file must have Team and Opponent columns. Found {games.columns.tolist()}")

    games["Team_std"] = apply_alias(games["Team"], alias)
    games["Opponent_std"] = apply_alias(games["Opponent"], alias)

    net_path = data_raw / "NET_Rank.csv"
    bpi_path = data_raw / "BPI_Rank.csv"
    kp_path = data_raw / "KenPom_Rank.csv"

    if not net_path.exists():
        raise RuntimeError(f"Missing {net_path}")
    if not bpi_path.exists():
        raise RuntimeError(f"Missing {bpi_path}")
    if not kp_path.exists():
        raise RuntimeError(f"Missing {kp_path}. Run python update_kenpom_rank.py first")

    net = prep_rank_file(net_path, alias, team_col="Team", rank_col="NET_Rank", std_name="Team_std")
    bpi = prep_rank_file(bpi_path, alias, team_col="Team", rank_col="BPI_Rank", std_name="Team_std")
    kp  = prep_rank_file(kp_path,  alias, team_col="Team", rank_col="KenPom_Rank", std_name="Team_std")

    out = games.merge(net[["Team_std", "NET_Rank"]], on="Team_std", how="left")
    out = out.merge(bpi[["Team_std", "BPI_Rank"]], on="Team_std", how="left")
    out = out.merge(kp[["Team_std", "KenPom_Rank"]], on="Team_std", how="left")

    out = out.rename(columns={
        "NET_Rank": "Team_NET_Rank",
        "BPI_Rank": "Team_BPI_Rank",
        "KenPom_Rank": "Team_KenPom_Rank",
    })

    out = out.merge(
        net.rename(columns={"Team_std": "Opponent_std", "NET_Rank": "Opponent_NET_Rank"})[["Opponent_std", "Opponent_NET_Rank"]],
        on="Opponent_std",
        how="left"
    )
    out = out.merge(
        bpi.rename(columns={"Team_std": "Opponent_std", "BPI_Rank": "Opponent_BPI_Rank"})[["Opponent_std", "Opponent_BPI_Rank"]],
        on="Opponent_std",
        how="left"
    )
    out = out.merge(
        kp.rename(columns={"Team_std": "Opponent_std", "KenPom_Rank": "Opponent_KenPom_Rank"})[["Opponent_std", "Opponent_KenPom_Rank"]],
        on="Opponent_std",
        how="left"
    )

    out_path = data_processed / "games_with_ranks.csv"
    out.to_csv(out_path, index=False)

    print(f"Games input: {games_path}")
    print(f"Wrote: {out_path}")
    print(f"Rows: {len(out)}")

if __name__ == "__main__":
    main()
