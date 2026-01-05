#!/usr/bin/env python3
"""
Build the site rankings data by merging all data sources.
Creates the final dashboard data file and HTML page.
Outputs:
  - data_processed/site_rankings.csv
  - docs/rankings.json
  - docs/index.html
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

    records_path = "data_raw/team_records.csv"
    if os.path.exists(records_path):
        records_df = pd.read_csv(records_path)

        if "team_espn" in records_df.columns:
            records_df = standardize_team_names(records_df, "team_espn", "espn")
            data["records"] = records_df[["team", "record"]].drop_duplicates(subset=["team"])
            print(f"Loaded {len(data['records'])} team records (ESPN)")
        elif "team_net" in records_df.columns:
            records_df = standardize_team_names(records_df, "team_net", "net")
            data["records"] = records_df[["team", "record"]].drop_duplicates(subset=["team"])
            print(f"Loaded {len(data['records'])} team records (NET)")
        else:
            print("team_records.csv exists but does not contain team_espn or team_net columns")

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


def create_dashboard_html():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CBB Rankings Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #e0e0e0;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        header { text-align: center; padding: 30px 0; margin-bottom: 30px; }
        h1 {
            font-size: 2.5rem;
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 10px;
        }
        .updated { color: #888; font-size: 0.9rem; }
        .search-container { margin-bottom: 20px; display: flex; justify-content: center; }
        #search {
            width: 100%;
            max-width: 400px;
            padding: 12px 20px;
            font-size: 1rem;
            border: 2px solid #333;
            border-radius: 25px;
            background: rgba(255, 255, 255, 0.05);
            color: #fff;
            outline: none;
            transition: border-color 0.3s;
        }
        #search:focus { border-color: #00d9ff; }
        #search::placeholder { color: #666; }
        .table-container {
            overflow-x: auto;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.3);
        }
        table { width: 100%; border-collapse: collapse; min-width: 900px; }
        th, td {
            padding: 15px 12px;
            text-align: left;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }
        th {
            background: rgba(0, 217, 255, 0.1);
            cursor: pointer;
            user-select: none;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.8rem;
            letter-spacing: 0.5px;
            white-space: nowrap;
            position: sticky;
            top: 0;
            transition: background 0.2s;
        }
        th:hover { background: rgba(0, 217, 255, 0.2); }
        th.sort-asc::after { content: ' \\25B2'; font-size: 0.7rem; }
        th.sort-desc::after { content: ' \\25BC'; font-size: 0.7rem; }
        tr:hover { background: rgba(255, 255, 255, 0.05); }
        .team-name { font-weight: 500; color: #fff; }
        .rank-cell { text-align: center; font-family: 'Monaco', 'Consolas', monospace; }
        .ap-rank { background: rgba(255, 215, 0, 0.1); color: #ffd700; font-weight: bold; }
        .avg-rank { color: #00ff88; font-weight: bold; }
        .top-10 { background: rgba(0, 255, 136, 0.1); }
        .top-25 { background: rgba(0, 217, 255, 0.05); }
        .record-cell { color: #aaa; font-family: 'Monaco', 'Consolas', monospace; }
        .empty { color: #444; }
        .stats { text-align: center; margin-top: 20px; color: #666; font-size: 0.9rem; }
        @media (max-width: 768px) {
            h1 { font-size: 1.8rem; }
            th, td { padding: 10px 8px; font-size: 0.85rem; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>CBB Rankings Dashboard</h1>
            <p class="updated"><span id="update-time">Loading...</span></p>
        </header>

        <div class="search-container">
            <input type="text" id="search" placeholder="Search teams..." autocomplete="off">
        </div>

        <div class="table-container">
            <table id="rankings-table">
                <thead>
                    <tr>
                        <th data-sort="team">Team</th>
                        <th data-sort="record">Record</th>
                        <th data-sort="ap_rank">AP</th>
                        <th data-sort="avg_rank">Avg Rank</th>
                        <th data-sort="net_rank">NET</th>
                        <th data-sort="kenpom_rank">KenPom</th>
                        <th data-sort="bpi_rank">BPI</th>
                        <th data-sort="sos_rank">SOS</th>
                    </tr>
                </thead>
                <tbody id="rankings-body">
                    <tr><td colspan="8" style="text-align: center; padding: 40px;">Loading rankings data...</td></tr>
                </tbody>
            </table>
        </div>

        <p class="stats" id="stats"></p>
    </div>

    <script>
        let teamsData = [];
        let currentSort = { column: 'avg_rank', direction: 'asc' };

        function parseRecord(rec) {
            if (rec === null || rec === undefined) return null;
            const s = String(rec).trim();
            if (!s) return null;
            const m = s.match(/(\\d+)\\s*-\\s*(\\d+)/);
            if (!m) return null;
            const w = parseInt(m[1], 10);
            const l = parseInt(m[2], 10);
            const g = w + l;
            const pct = g > 0 ? (w / g) : -1;
            return { w, l, pct };
        }

        async function loadData() {
            try {
                const response = await fetch('rankings.json');
                const data = await response.json();
                teamsData = data.teams;
                document.getElementById('update-time').textContent = data.updated;
                renderTable();
                updateStats();
            } catch (error) {
                console.error('Error loading data:', error);
                document.getElementById('rankings-body').innerHTML =
                    '<tr><td colspan="8" style="text-align: center; padding: 40px; color: #ff6b6b;">Error loading data. Please refresh.</td></tr>';
            }
        }

        function renderTable() {
            const searchTerm = document.getElementById('search').value.toLowerCase();

            let filtered = teamsData.filter(team =>
                team.team.toLowerCase().includes(searchTerm)
            );

            filtered.sort((a, b) => {
                let aVal = a[currentSort.column];
                let bVal = b[currentSort.column];

                if (aVal === null && bVal === null) return 0;
                if (aVal === null) return 1;
                if (bVal === null) return -1;

                if (currentSort.column === 'team') {
                    aVal = String(aVal).toLowerCase();
                    bVal = String(bVal).toLowerCase();
                    if (currentSort.direction === 'asc') return aVal.localeCompare(bVal);
                    return bVal.localeCompare(aVal);
                }

                if (currentSort.column === 'record') {
                    const ar = parseRecord(aVal);
                    const br = parseRecord(bVal);

                    if (ar === null && br === null) return 0;
                    if (ar === null) return 1;
                    if (br === null) return -1;

                    const cmpPct = br.pct - ar.pct;
                    if (cmpPct !== 0) {
                        return currentSort.direction === 'asc' ? cmpPct : -cmpPct;
                    }

                    const cmpW = br.w - ar.w;
                    if (cmpW !== 0) {
                        return currentSort.direction === 'asc' ? cmpW : -cmpW;
                    }

                    const cmpL = ar.l - br.l;
                    if (cmpL !== 0) {
                        return currentSort.direction === 'asc' ? cmpL : -cmpL;
                    }

                    const at = String(a.team || '').toLowerCase();
                    const bt = String(b.team || '').toLowerCase();
                    return at.localeCompare(bt);
                }

                if (currentSort.direction === 'asc') return aVal - bVal;
                return bVal - aVal;
            });

            const tbody = document.getElementById('rankings-body');

            if (filtered.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 40px;">No teams found</td></tr>';
                return;
            }

            tbody.innerHTML = filtered.map(team => {
                const rowClass = team.avg_rank && team.avg_rank <= 10 ? 'top-10' :
                                 team.avg_rank && team.avg_rank <= 25 ? 'top-25' : '';

                return `
                    <tr class="${rowClass}">
                        <td class="team-name">${team.team}</td>
                        <td class="record-cell">${team.record || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell ap-rank">${team.ap_rank || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell avg-rank">${team.avg_rank || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell">${team.net_rank || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell">${team.kenpom_rank || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell">${team.bpi_rank || '<span class="empty">-</span>'}</td>
                        <td class="rank-cell">${team.sos_rank || '<span class="empty">-</span>'}</td>
                    </tr>
                `;
            }).join('');

            updateSortIndicators();
        }

        function updateSortIndicators() {
            document.querySelectorAll('th').forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
                if (th.dataset.sort === currentSort.column) {
                    th.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
                }
            });
        }

        function updateStats() {
            const total = teamsData.length;
            const withAP = teamsData.filter(t => t.ap_rank).length;
            document.getElementById('stats').textContent =
                `Showing ${total} D1 teams | ${withAP} teams in AP Top 25`;
        }

        document.getElementById('search').addEventListener('input', renderTable);

        document.querySelectorAll('th[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                const column = th.dataset.sort;
                if (currentSort.column === column) {
                    currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
                } else {
                    currentSort.column = column;
                    currentSort.direction = 'asc';
                }
                renderTable();
            });
        });

        loadData();
    </script>
</body>
</html>"""


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

    html = create_dashboard_html()
    with open("docs/index.html", "w") as f:
        f.write(html)
    print("Saved docs/index.html")

    print("Done!")


if __name__ == "__main__":
    main()
