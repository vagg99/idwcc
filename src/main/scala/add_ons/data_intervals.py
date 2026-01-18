import sys
import random
from pathlib import Path

def get_paths():
    """Dynamically resolves input/output paths relative to this script."""
    script_path = Path(__file__).resolve()

    # Navigate 4 levels up to project root: idwcc/
    project_root = script_path.parents[4]

    data_dir = project_root / "data"
    input_file = data_dir / "com-amazon.ungraph.txt"
    output_file = data_dir / "amazon_generated_intervals.txt"

    return input_file, output_file

def get_interval(parts):
    """Parses existing time interval or generates a new random one [2, 4]."""
    # Case 1: Interval exists (Source, Target, Start, End)
    if len(parts) >= 4:
        try:
            val_start = int(parts[-2])
            val_end = int(parts[-1])
            # Clamp to valid range
            new_start = max(2, min(val_start, 4))
            new_end = max(2, min(val_end, 4))
            return new_start, new_end
        except ValueError:
            pass # Fall through to generation if parsing fails

    # Case 2: Generate synthetic interval
    new_start = random.randint(2, 4)
    # Ensure end is always >= start (e.g., 2->4, 3->3 is valid; 4->2 is not)
    new_end = random.randint(new_start, 4)

    return new_start, new_end

def main():
    input_path, output_path = get_paths()

    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    print(f"Reading from: {input_path}")
    print(f"Writing to:   {output_path}")

    try:
        with open(input_path, "r", encoding="utf-8") as in_file, \
                open(output_path, "w", encoding="utf-8") as out_file:

            count = 0
            for line in in_file:
                # Preserve comments
                if line.startswith("#"):
                    out_file.write(line)
                    continue

                # Handles both tabs and spaces cleanly
                parts = line.strip().split()

                if len(parts) < 2:
                    continue

                # Extract nodes (assuming first two cols are always Source, Target)
                src, dst = parts[0], parts[1]

                # Get intervals
                start, end = get_interval(parts)

                # Write efficiently
                out_file.write(f"{src}\t{dst}\t{start}\t{end}\n")

                count += 1
                if count % 500000 == 0:
                    print(f"Processed {count} lines...", end="\r")

        print(f"\nSuccess! Total processed lines: {count}")

    except Exception as e:
        print(f"\nCritical Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()