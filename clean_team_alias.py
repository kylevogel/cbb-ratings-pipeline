from pathlib import Path
import csv

def main():
    root = Path(__file__).resolve().parent
    src = root / "data_raw" / "team_alias.csv"
    if not src.exists():
        src = root / "team_alias.csv"
    if not src.exists():
        raise FileNotFoundError("Could not find team_alias.csv in data_raw/ or repo root.")

    out = src.with_name("team_alias.cleaned.csv")

    lines = src.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise RuntimeError("team_alias.csv is empty.")

    header = lines[0].split(",")
    if len(header) < 5:
        raise RuntimeError("Expected at least 5 columns in team_alias.csv header.")

    fixed_rows = []
    fixed_rows.append(header[:5])

    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split(",", 4)
        while len(parts) < 5:
            parts.append("")
        parts = parts[:5]
        fixed_rows.append(parts)

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerows(fixed_rows)

    print(str(out))

if __name__ == "__main__":
    main()
