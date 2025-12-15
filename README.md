# College Basketball Data Pipeline and Ratings Analyzer

Automated Python pipeline that pulls NCAA men’s basketball results from ESPN, standardizes team names, and merges major rating systems (NET, ESPN BPI, KenPom) into an analysis ready game level dataset.

## What it does
* Pulls recent game results from ESPN and builds a clean season table with team, opponent, location, score, and win or loss
* Scrapes NET, BPI, and KenPom rankings and standardizes team names using an alias map
* Produces a merged dataset with both team and opponent ranks for each game for downstream modeling and visualization

## Main output
* `data_processed/games_with_ranks.csv`
  * Includes team and opponent ranks for NET, BPI, and KenPom

## How to run
From the project root:

```bash
python update_all.py
