# College Basketball Data Pipeline and Ratings Analyzer

Automated Python pipeline that pulls NCAA men’s basketball results from ESPN, standardizes team naming using an alias map, and merges daily NET, BPI, and KenPom rankings.

## What it does
- Pulls game results from ESPN and builds a clean season table with team, opponent, location, and score.
- Pulls NET, BPI, and KenPom rankings and standardizes team naming using an `alias` map.
- Produces a merged dataset with both team and opponent ranks for each game for downstream analysis.

## Main output
- `data_processed/games_with_ranks.csv`
  Game level table with team and opponent ranks for NET, BPI, and KenPom

## Quick start
From the project root:

```bash
python update_all.py
``` 

## Repo Structure
- update_all.py Runs the full pipeline end to end (games, rankings, merge)
- update_games_from_espn.py Pulls NCAA men’s game results from ESPN and writes a clean season table to data_raw/
- update_net_rank.py Pulls NET rankings and writes data_raw/NET_Rank.csv
- update_bpi_rank.py Pulls ESPN BPI rankings and writes data_raw/BPI_Rank.csv
- update_kenpom_rank.py Pulls KenPom rankings and writes data_raw/KenPom_Rank.csv
- merge_games_with_ranks.py Standardizes team names and merges rankings onto each game (team and opponent). Output is written to data_processed/games_with_ranks.csv
- team_alias.csv Team name mapping used to align ESPN, NET, BPI, and KenPom naming differences
- data_raw/ Raw outputs produced by the scrapers (games and ranking tables)
- data_processed/ Final merged dataset used for analysis and modeling

## Notes
- If a source site changes its layout or endpoints, the corresponding updater may need a small parsing update
- Team name standardization is controlled by team_alias.csv

## License
- MIT
