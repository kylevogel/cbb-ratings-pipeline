import subprocess
import sys


SCRIPTS = [
    "update_games_from_espn.py",
    "update_net_rank.py",
    "update_kenpom_rank.py",
    "update_bpi_rank.py",
    "update_ap_rank.py",
    "update_sos_rank.py",
    "build_site_rankings.py",
]


def main() -> int:
    for s in SCRIPTS:
        print(f"\n=== running {s} ===")
        r = subprocess.run([sys.executable, s], check=False)
        if r.returncode != 0:
            raise SystemExit(r.returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
