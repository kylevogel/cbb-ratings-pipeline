import subprocess
import sys

def run(script: str):
    print(f"\n--- Running: {script} ---")
    subprocess.run([sys.executable, script], check=True)

def main():
    run("update_games_from_espn.py")
    run("update_net_rank.py")
    run("update_bpi_rank.py")
    run("update_kenpom_rank.py")
    run("merge_games_with_ranks.py")
    print("\nAll updates finished.")

if __name__ == "__main__":
    main()
