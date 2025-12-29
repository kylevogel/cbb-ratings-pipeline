import pandas as pd
from pathlib import Path
from datetime import datetime

def pick_col(cols, candidates):
  lower = {c.lower(): c for c in cols}
  for cand in candidates:
    if cand.lower() in lower:
      return lower[cand.lower()]
  for c in cols:
    lc = c.lower()
    for cand in candidates:
      if cand.lower() in lc:
        return c
  return None

def load_rank(path, team_candidates, rank_candidates):
  if not Path(path).exists():
    return pd.DataFrame(columns=["Team", "Rank"])
  df = pd.read_csv(path)
  df.columns = [str(c).strip() for c in df.columns]
  team_col = pick_col(df.columns, team_candidates)
  rank_col = pick_col(df.columns, rank_candidates)

  if team_col is None:
    team_col = df.columns[0]
  if rank_col is None:
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    rank_col = num_cols[0] if num_cols else df.columns[-1]

  out = df[[team_col, rank_col]].copy()
  out.columns = ["Team", "Rank"]
  out["Team"] = out["Team"].astype(str).str.strip()
  out["Rank"] = pd.to_numeric(out["Rank"], errors="coerce").astype("Int64")
  out = out.dropna(subset=["Team"])
  out = out.drop_duplicates(subset=["Team"], keep="first")
  return out

def main():
  net = load_rank("data_raw/NET_Rank.csv", ["Team", "School"], ["NET", "Rank"])
  kp = load_rank("data_raw/KenPom_Rank.csv", ["Team", "School"], ["KenPom", "Rank"])
  bpi = load_rank("data_raw/BPI_Rank.csv", ["Team", "School"], ["BPI", "Rank"])
  ap = load_rank("data_raw/AP_Rank.csv", ["Team", "School"], ["AP", "AP_Rank", "Rank"])

  net = net.rename(columns={"Rank": "NET"})
  kp = kp.rename(columns={"Rank": "KenPom"})
  bpi = bpi.rename(columns={"Rank": "BPI"})
  ap = ap.rename(columns={"Rank": "AP"})

  teams = pd.DataFrame({"Team": pd.concat([net["Team"], kp["Team"], bpi["Team"], ap["Team"]], ignore_index=True).dropna().unique()})
  df = teams.merge(net, on="Team", how="left").merge(kp, on="Team", how="left").merge(bpi, on="Team", how="left").merge(ap, on="Team", how="left")

  df["AP"] = df["AP"].astype("Int64")
  df["AP"] = df["AP"].apply(lambda x: "NR" if pd.isna(x) else int(x))

  def int_or_blank(v):
    if pd.isna(v):
      return ""
    return int(v)

  df["NET"] = df["NET"].apply(int_or_blank)
  df["KenPom"] = df["KenPom"].apply(int_or_blank)
  df["BPI"] = df["BPI"].apply(int_or_blank)

  out_dir = Path("docs") / "data"
  out_dir.mkdir(parents=True, exist_ok=True)

  payload = {
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "rows": df.sort_values(by=["Team"]).to_dict(orient="records")
  }

  out_path = out_dir / "rankings_current.json"
  import json
  out_path.write_text(json.dumps(payload, ensure_ascii=False))
  print(f"Wrote {out_path} ({len(df)} teams)")

if __name__ == "__main__":
  main()
