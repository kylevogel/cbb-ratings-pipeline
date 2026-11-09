# CBB Ratings Pipeline and Dashboard

Automated Python pipeline that collects NCAA men's basketball rankings from major sources, standardizes team names, and publishes a public rankings dashboard via GitHub Pages.

## Live dashboard

CBB Rankings Dashboard  
https://kylevogel.github.io/cbb-ratings-pipeline/

## What this project does

This pipeline updates data in three stages:

1. Pulls rankings, ratings, and records from multiple sources
   - NET rankings
   - KenPom rankings
   - ESPN BPI
   - AP Poll
   - Strength of Schedule
   - Team records

2. Standardizes team names across sources
   - Uses a dedicated mapping file so NET, KenPom, BPI, AP, and SoS all align

3. Produces outputs for the dashboard
   - A master rankings table with all metrics merged by team
   - A sortable dashboard that includes Record, AP, NET, KenPom, BPI, SoS, and the KV Rank

## Why it exists

In college basketball, there are so many different rankings and metrics these days that it's hard to keep track of them all. I built this dashboard so fans can see how their team ranks across all the major metrics in one place, without ads.

## Main outputs

### Processed data
- `data_processed/site_rankings.csv` - Master rankings table with all metrics merged by team

### Raw data
- `data_raw/` - Downloaded ranking tables from each source (NET, KenPom, BPI, AP, SoS, records)

### Dashboard
- `docs/index.html` - Static dashboard page for GitHub Pages
- `docs/rankings.json` - JSON data powering the dashboard

## How the KV Rank works

KV Rank is a custom blend, not a simple average. Each rank is converted to a z-score (`z = -NORM.S.INV((rank - 0.5) / 365)`) so that being #1 vs #10 counts for more than #100 vs #110.

- **Quality** = average of KenPom z and BPI z
- **Record_adj** = z(win%) + 0.5 x SOS z (credit for winning against a harder schedule)

**Phase 1: before NET is released (mid-December)**
`Final = (1 - w) x Quality + w x Record_adj`, where `w = 0.45 x min(1, games played / 15)`.
Early on this is almost all KenPom and BPI; record takes over gradually as games are played.

**Phase 2: once NET is available**
`Resume = 0.6 x NET z + 0.4 x Record_adj`, then `Final = 0.5 x Quality + 0.5 x Resume`.

The pipeline switches to Phase 2 automatically for any team with a NET rank. AP is not used, and SOS is only used inside Record_adj. Teams are ranked by Final (highest first); ties share a rank.

## Quick start

From the project root:
```bash
pip install -r requirements.txt
python update_all.py
```

## Project structure

### Core runner
- `update_all.py` - Runs the full pipeline in sequence

### Rank and rating updaters
- `update_net_rank.py` - Pulls NET rankings
- `update_kenpom_rank.py` - Pulls KenPom rankings
- `update_bpi_rank.py` - Pulls ESPN BPI rankings
- `update_ap_rank.py` - Pulls AP Poll rankings
- `update_sos_rank.py` - Pulls Strength of Schedule rankings
- `update_records.py` - Pulls team records

### Build and publish
- `build_site_rankings.py` - Merges all data sources, standardizes team names, and outputs dashboard files

### Team naming
- `team_alias.csv` - Source of truth mapping to align team names across all sources
- `clean_team_alias.py` - Utility module for team name standardization

### Data directories
- `data_raw/` - Raw CSV exports from each updater
- `data_processed/` - Final merged output
- `docs/` - GitHub Pages dashboard

## Notes and limitations

- Source sites can change HTML layout or endpoints, which may require parsing updates
- Team naming mismatches are the most common cause of missing ranks—`team_alias.csv` is designed to fix these quickly

## License

MIT
