from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def main() -> None:
    root = Path(__file__).resolve().parent

    scripts = [
        "update_games_from_espn.py",
        "update_schedule_from_espn.py",
        "update_net_rank.py",
        "update_kenpom_rank.py",
        "update_bpi_rank.py",
        "update_ap_rank.py",
        "update_sos_rank.py",
        "merge_games_with_ranks.py",
    ]

    ran = 0
    failed = 0
    for s in scripts:
        p = root / s
        if not p.exists():
            continue
        ran += 1
        r = subprocess.run([sys.executable, s], cwd=root)
        if r.returncode != 0:
            failed += 1
            print(f"[WARN] {s} failed (exit {r.returncode}). Continuing...")

    if ran > 0 and failed == ran:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
