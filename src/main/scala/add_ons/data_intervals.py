import sys
import random
from pathlib import Path

DATASET_NAME = "youtube"  # Choose your dataset here

# Time window
MIN_T = 1
MAX_T = 10
# =========================

def get_paths():
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[4]
    data_dir = project_root / "data"

    input_file = data_dir / f"com-{DATASET_NAME}.ungraph.txt"
    output_file = data_dir / f"{DATASET_NAME}_generated_intervals.txt"
    return input_file, output_file


def build_valid_suffixes(min_t: int, max_t: int):
    """
    Returns a list of pre-formatted suffix strings: "\tstart\tend\n"
    for all pairs (start,end) with min_t <= start <= end <= max_t.
    """
    if min_t > max_t:
        raise ValueError(f"Invalid time window: MIN_T ({min_t}) > MAX_T ({max_t})")

    suffixes = []
    for s in range(min_t, max_t + 1):
        for e in range(s, max_t + 1):
            suffixes.append(f"\t{s}\t{e}\n")
    return suffixes


def clamp_time(t: int, min_t: int, max_t: int) -> int:
    if t < min_t:
        return min_t
    if t > max_t:
        return max_t
    return t


def main():
    input_path, output_path = get_paths()

    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    print(f"Reading: {input_path}")
    print(f"Writing: {output_path}")
    print(f"Time window: [{MIN_T}, {MAX_T}]")

    # Pre-bake valid suffixes to save CPU
    valid_suffixes = build_valid_suffixes(MIN_T, MAX_T)

    try:
        # 1MB buffer reduces the number of system calls to the hard drive.
        buffer_size = 1024 * 1024

        with open(input_path, "r", encoding="utf-8", buffering=buffer_size) as in_file, \
                open(output_path, "w", encoding="utf-8", buffering=buffer_size) as out_file:

            # Local references for speed
            write_line = out_file.write
            get_random_suffix = random.choice
            suffixes = valid_suffixes

            count = 0

            for line in in_file:
                # Fast skip for comments
                if line.startswith("#"):
                    write_line(line)
                    continue

                clean_line = line.rstrip()
                parts = clean_line.split()

                if not parts:
                    continue

                # Case A: Data is missing intervals (Standard Graph)
                # Write nodes + random (start,end) in the configured window
                if len(parts) < 3:
                    # Expect at least two tokens for an edge
                    if len(parts) >= 2:
                        write_line(f"{parts[0]}\t{parts[1]}{get_random_suffix(suffixes)}")
                    else:
                        # If a malformed line exists, just skip it
                        continue

                # Case B: Data has intervals, clamp them to [MIN_T, MAX_T]
                else:
                    try:
                        start = int(parts[-2])
                        end = int(parts[-1])

                        new_start = clamp_time(start, MIN_T, MAX_T)
                        new_end = clamp_time(end, MIN_T, MAX_T)

                        base_content = "\t".join(parts[:-2])
                        write_line(f"{base_content}\t{new_start}\t{new_end}\n")

                    except ValueError:
                        # Fallback if parsing fails: treat as missing
                        if len(parts) >= 2:
                            write_line(f"{parts[0]}\t{parts[1]}{get_random_suffix(suffixes)}")
                        else:
                            continue

                # Progress report (only every 1 million lines)
                count += 1
                if count % 1_000_000 == 0:
                    print(f"Processed {count:,} lines...", end="\r")

        print(f"\nSuccess! Processed {count:,} lines.")

    except Exception as e:
        print(f"\nCritical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
