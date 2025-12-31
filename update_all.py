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
    ]

    failed = []

    for s in scripts:
        p = root / s
        if not p.exists():
            continue
        try:
            subprocess.run([sys.executable, s], check=True, cwd=root)
        except subprocess.CalledProcessError as e:
            failed.append((s, e.returncode))
            print(f"FAILED: {s} (exit {e.returncode})", file=sys.stderr)

    if failed:
        msg = ", ".join([f"{s}:{rc}" for s, rc in failed])
        print(f"Some update scripts failed (continuing anyway): {msg}", file=sys.stderr)


if __name__ == "__main__":
    main()
