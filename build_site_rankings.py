"""
Build the site rankings data by merging all data sources.
Creates the final dashboard data file.
Outputs:
  - data_processed/site_rankings.csv
  - docs/rankings.json

NOTE: docs/index.html is NOT written by this script.
Edit docs/index.html directly — it will not be overwritten on pipeline runs.
"""

import pandas as pd
import json
import os
from datetime import datetime, timezone, timedelta
from clean_team_alias import standardize_team_names, load_team_alias


def load_and_standardize_data():
    data = {}

    net_path = "data_raw/net_rankings.csv"
    if os.path.exists(net_path):
        net_df = pd.read_csv(net_path)
        net_df = standardize_team_names(net_df, "team_net", "net")
        data["net"] = net_df[["team", "net_rank"]].drop_duplicates(subset=["team"])
        print(f"Loaded {len(data['net'])} NET rankings")

    kenpom_path = "data_raw/kenpom_rankings.csv"
    if os.path.exists(kenpom_path):
        kenpom_df = pd.read_csv(kenpom_path)
        kenpom_df = standardize_team_names(kenpom_df, "team_kenpom", "kenpom")
        data["kenpom"] = kenpom_df[["team", "kenpom_rank"]].drop_duplicates(subset=["team"])
        print(f"Loaded {len(data['kenpom'])} KenPom rankings")

        if "record" in kenpom_df.columns:
            data["records"] = kenpom_df[["team", "record"]].drop_duplicates(subset=["team"])
            print(f"Loaded {len(data['records'])} team records (from KenPom)")

    bpi_path = "data_raw/bpi_rankings.csv"
    if os.path.exists(bpi_path):
        bpi_df = pd.read_csv(bpi_path)
        bpi_df = standardize_team_names(bpi_df, "team_bpi", "bpi")
        data["bpi"] = bpi_df[["team", "bpi_rank"]].drop_duplicates(subset=["team"])
        print(f"Loaded {len(data['bpi'])} BPI rankings")

    ap_path = "data_raw/ap_rankings.csv"
    if os.path.exists(ap_path):
        ap_df = pd.read_csv(ap_path)
        if not ap_df.empty:
            ap_df = standardize_team_names(ap_df, "team_ap", "ap")
            data["ap"] = ap_df[["team", "ap_rank"]].drop_duplicates(subset=["team"])
            print(f"Loaded {len(data['ap'])} AP rankings")

    sos_path = "data_raw/sos_rankings.csv"
    if os.path.exists(sos_path):
        sos_df = pd.read_csv(sos_path)
        sos_df = standardize_team_names(sos_df, "team_sos", "sos")
        data["sos"] = sos_df[["team", "sos_rank"]].drop_duplicates(subset=["team"])
        print(f"Loaded {len(data['sos'])} SOS rankings")

    return data


def build_master_rankings(data):
    alias_df = load_team_alias()
    if alias_df is not None:
        master = pd.DataFrame({"team": alias_df["canonical"].unique()})
    else:
        master = data["net"][["team"]].copy() if "net" in data else pd.DataFrame({"team": []})

    if "records" in data:
        master = master.merge(data["records"], on="team", how="left")
    else:
        master["record"] = ""

    if "ap" in data:
        master = master.merge(data["ap"], on="team", how="left")
    else:
        master["ap_rank"] = None

    if "net" in data:
        master = master.merge(data["net"], on="team", how="left")
    else:
        master["net_rank"] = None

    if "kenpom" in data:
        master = master.merge(data["kenpom"], on="team", how="left")
    else:
        master["kenpom_rank"] = None

    if "bpi" in data:
        master = master.merge(data["bpi"], on="team", how="left")
    else:
        master["bpi_rank"] = None

    if "sos" in data:
        master = master.merge(data["sos"], on="team", how="left")
    else:
        master["sos_rank"] = None

    def calc_avg_value(row):
        ranks = []
        for col in ["net_rank", "kenpom_rank", "bpi_rank"]:
            val = row.get(col)
            if pd.notna(val):
                ranks.append(float(val))
        if ranks:
            return round(sum(ranks) / len(ranks), 1)
        return None

    master["avg_value"] = master.apply(calc_avg_value, axis=1)

    has_ranking = master["net_rank"].notna() | master["kenpom_rank"].notna() | master["bpi_rank"].notna()
    master = master[has_ranking].reset_index(drop=True)

    master["avg_rank"] = master["avg_value"].rank(method="min", ascending=True).astype("Int64")

    master = master.sort_values(["avg_rank", "avg_value", "team"], na_position="last").reset_index(drop=True)
    return master


_EST = timezone(timedelta(hours=-5))


def _format_updated_est(dt_utc: datetime) -> str:
    dt_est = dt_utc.astimezone(_EST)
    s = dt_est.strftime("%m/%d/%Y at %I:%M %p")
    s = s[:-2] + s[-2:].lower()
    return f"Updated: {s} EST"


def create_dashboard_json(master_df):
    records = []
    for _, row in master_df.iterrows():
        records.append(
            {
                "team": row["team"],
                "record": row["record"] if pd.notna(row["record"]) else "",
                "ap_rank": int(row["ap_rank"]) if pd.notna(row["ap_rank"]) else None,
                "avg_rank": int(row["avg_rank"]) if pd.notna(row["avg_rank"]) else None,
                "net_rank": int(row["net_rank"]) if pd.notna(row["net_rank"]) else None,
                "kenpom_rank": int(row["kenpom_rank"]) if pd.notna(row["kenpom_rank"]) else None,
                "bpi_rank": int(row["bpi_rank"]) if pd.notna(row["bpi_rank"]) else None,
                "sos_rank": int(row["sos_rank"]) if pd.notna(row["sos_rank"]) else None,
            }
        )

    updated_str = _format_updated_est(datetime.now(timezone.utc))
    return {"updated": updated_str, "teams": records}


def main():
    print("Building site rankings...")

    data = load_and_standardize_data()
    if not data:
        print("No data loaded - cannot build rankings")
        return

    master = build_master_rankings(data)
    print(f"Built master rankings with {len(master)} teams")

    os.makedirs("data_processed", exist_ok=True)
    master.to_csv("data_processed/site_rankings.csv", index=False)
    print("Saved data_processed/site_rankings.csv")

    os.makedirs("docs", exist_ok=True)

    dashboard_json = create_dashboard_json(master)
    with open("docs/rankings.json", "w") as f:
        json.dump(dashboard_json, f, indent=2)
    print("Saved docs/rankings.json")

    # FIX: index.html is no longer written here.
    # Edit docs/index.html directly — it is the source of truth and
    # will not be overwritten by the pipeline.
    if not os.path.exists("docs/index.html"):
        print("WARNING: docs/index.html does not exist. Make sure it is committed to the repo.")
    else:
        print("docs/index.html already exists — skipping (not overwritten).")

    print("Done!")


if __name__ == "__main__":
    main()
