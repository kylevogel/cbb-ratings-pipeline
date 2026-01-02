# CBB Ratings Pipeline and Dashboard

Automated Python pipeline that pulls NCAA men’s basketball games from ESPN, collects major rating and ranking sources, merges everything into clean datasets, and publishes a public rankings dashboard via GitHub Pages.

## Live dashboard
CBB Rankings Dashboard
https://kylevogel.github.io/cbb-ratings-pipeline/

## What this project does
This pipeline updates data in four stages

1. Pulls games from ESPN
   - Completed results and upcoming games
   - Team and opponent on each row
   - Home, away, neutral location labeling

2. Pulls rankings and ratings sources
   - NET
   - KenPom
   - ESPN BPI
   - AP Poll
   - Strength of Schedule

3. Standardizes team names across sources
   - Uses a dedicated mapping file so ESPN, NET, KenPom, and BPI all align

4. Produces outputs for analysis and for the dashboard
   - A merged game level dataset enriched with team and opponent ranks
   - A sortable dashboard table that includes Record, SoS, AP, and an average metric view

## Why it exists
College basketball data and rankings live across multiple sites with inconsistent team naming. This project makes the data usable by automatically collecting, standardizing, and merging it into consistent tables that can power analysis, modeling, and a simple public dashboard.

## Main outputs
### Analysis ready data
- data_processed
  - games_with_ranks.csv
    A game level dataset where each row represents one team perspective of a game and includes team and opponent ranks for major metrics

### Raw data
- data_raw
  - games and schedule exports from ESPN
  - downloaded ranking tables from each source

### Dashboard
- docs
  - static site assets for GitHub Pages
  - generated rankings table used by the dashboard

## How the dashboard ranks teams
The dashboard displays individual metrics and also includes an Avg of Metrics view that averages a team’s NET, KenPom, and BPI ranks and then ranks teams by that average.

Ties are allowed and teams can share the same average based rank.

## Quick start
From the project root

pip install -r requirements.txt
python update_all.py

## Project structure
Core runner
- update_all.py
  Runs the full pipeline in sequence and is the main entry point

ESPN ingestion
- update_games_from_espn.py
  Pulls results and near term schedule from the ESPN scoreboard API and writes a clean season table
- update_schedule_from_espn.py
  Builds a full season schedule reference from ESPN

Rank and rating updaters
- update_net_rank.py
  Pulls NET rankings and writes a clean CSV
- update_kenpom_rank.py
  Pulls KenPom rankings and writes a clean CSV
- update_bpi_rank.py
  Pulls ESPN BPI rankings and writes a clean CSV
- update_ap_rank.py
  Pulls AP Poll rankings and writes a clean CSV
- update_sos_rank.py
  Pulls strength of schedule rankings and writes a clean CSV

Merge and publish
- merge_games_with_ranks.py
  Standardizes team names and merges the ranks onto each game for both team and opponent
- build_site_rankings.py
  Builds the dataset the dashboard reads and places outputs into docs for GitHub Pages

Team naming
- team_alias.csv
  Source of truth mapping to align team names across ESPN, NET, KenPom, BPI, AP, and SoS formatting differences

Data directories
- data_raw
  Raw exports produced by each updater
- data_processed
  Final merged outputs used for analysis and modeling
- docs
  GitHub Pages site and generated dashboard data

## Notes and limitations
- Source sites can change HTML layout or endpoints, which can require small parsing updates
- Team naming mismatches are the most common cause of missing ranks, and team_alias.csv is designed to fix that quickly
- Neutral site classification is best effort based on ESPN metadata and may not be perfect for every game

## License
MIT
