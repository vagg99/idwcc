import sys
import random
from pathlib import Path

DATASET_NAME = "dblp"  # Choose your dataset here

# Global time domain used for interval generation
MIN_T = 1
MAX_T = 100000

# Set to an integer (e.g. 42) for reproducible results, or None for fresh randomness
RANDOM_SEED = 42


def get_paths():
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[4]
    data_dir = project_root / "data"

    input_file = data_dir / f"com-{DATASET_NAME}.ungraph.txt"
    output_file = data_dir / f"{DATASET_NAME}_generated_intervals.txt"
    return input_file, output_file


def clamp_time(t: int, min_t: int, max_t: int) -> int:
    if t < min_t:
        return min_t
    if t > max_t:
        return max_t
    return t


def normalize_interval(start: int, end: int) -> tuple[int, int]:
    if start <= end:
        return start, end
    return end, start


def generate_random_interval(randint_func, min_t: int, max_t: int) -> tuple[int, int]:
    """
    Generate a random interval by drawing start and end independently
    and uniformly from [min_t, max_t], then sorting them so that
    start <= end.

    This matches the closest defensible reading of the paper's wording:
    'the start and end times are drawn from a predefined time range'
    using a uniform distribution.
    """
    a = randint_func(min_t, max_t)
    b = randint_func(min_t, max_t)
    return normalize_interval(a, b)


def main():
    if MIN_T > MAX_T:
        print(f"Error: invalid time window [{MIN_T}, {MAX_T}]")
        sys.exit(1)

    input_path, output_path = get_paths()

    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    rng = random.Random(RANDOM_SEED)
    randint = rng.randint

    print(f"Reading: {input_path}")
    print(f"Writing: {output_path}")
    print(f"Time window: [{MIN_T}, {MAX_T}]")
    print(f"Random seed: {RANDOM_SEED}")

    try:
        buffer_size = 1024 * 1024  # 1 MB

        with open(input_path, "r", encoding="utf-8", buffering=buffer_size) as in_file, \
                open(output_path, "w", encoding="utf-8", buffering=buffer_size) as out_file:

            write_line = out_file.write
            count = 0

            for line in in_file:
                # Preserve comments as-is
                if line.startswith("#"):
                    write_line(line)
                    continue

                parts = line.split()

                # Skip empty / malformed lines
                if not parts:
                    continue

                # Case A: Standard graph edge without intervals
                # Expect at least two tokens: u v
                if len(parts) < 3:
                    if len(parts) < 2:
                        continue

                    u, v = parts[0], parts[1]
                    start, end = generate_random_interval(randint, MIN_T, MAX_T)
                    write_line(f"{u}\t{v}\t{start}\t{end}\n")

                # Case B: Existing interval data
                # Treat the last two tokens as start/end and preserve any preceding columns
                else:
                    try:
                        raw_start = int(parts[-2])
                        raw_end = int(parts[-1])

                        start = clamp_time(raw_start, MIN_T, MAX_T)
                        end = clamp_time(raw_end, MIN_T, MAX_T)
                        start, end = normalize_interval(start, end)

                        base_content = "\t".join(parts[:-2])
                        write_line(f"{base_content}\t{start}\t{end}\n")

                    except ValueError:
                        # If the last two tokens are not parseable as ints,
                        # fallback to treating the line as a plain edge.
                        if len(parts) < 2:
                            continue

                        u, v = parts[0], parts[1]
                        start, end = generate_random_interval(randint, MIN_T, MAX_T)
                        write_line(f"{u}\t{v}\t{start}\t{end}\n")

                count += 1
                if count % 1_000_000 == 0:
                    print(f"Processed {count:,} lines...", end="\r")

        print(f"\nSuccess! Processed {count:,} lines.")

    except Exception as e:
        print(f"\nCritical Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
