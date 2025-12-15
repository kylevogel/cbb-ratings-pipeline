# College Basketball Data Pipeline & Ratings Analyzer

- Pulls NCAA men’s game results from ESPN and builds a clean, analysis-ready season dataset (teams, opponent, location, score, win/loss).
- Scrapes and merges major rating systems (NET, ESPN BPI, KenPom) into a single game-level table with team and opponent ranks.
- Includes scripts to update data on demand and output a merged `games_with_ranks.csv` for downstream modeling and visualization.

## Run
python update_all.py
