import sys
import random
from pathlib import Path

def get_paths():

    script_path = Path(__file__).resolve()
    project_root = script_path.parents[4]
    data_dir = project_root / "data"

    input_file = data_dir / "com-dblp.ungraph.txt"
    output_file = data_dir / "dblp_generated_intervals.txt"
    return input_file, output_file

def main():
    input_path, output_path = get_paths()

    if not input_path.exists():
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    print(f"Reading: {input_path}")
    print(f"Writing: {output_path}")

    # These are the only 6 valid outcomes. We pre-format them to save CPU.
    # We include the tab separator at the start of the suffix.
    valid_suffixes = [
        "\t2\t2\n",
        "\t2\t3\n",
        "\t2\t4\n",
        "\t3\t3\n",
        "\t3\t4\n",
        "\t4\t4\n"
    ]

    try:
        # 1MB buffer (1024*1024) reduces the number of system calls to the hard drive.
        buffer_size = 1024 * 1024

        with open(input_path, "r", encoding="utf-8", buffering=buffer_size) as in_file, \
                open(output_path, "w", encoding="utf-8", buffering=buffer_size) as out_file:

            # Looking up 'out_file.write' and 'random.choice' in the global scope
            # millions of times is slow. We create local references.
            write_line = out_file.write
            get_random_suffix = random.choice
            suffixes = valid_suffixes

            count = 0

            for line in in_file:
                # Fast skip for comments
                if line.startswith("#"):
                    write_line(line)
                    continue

                # Strip whitespace from the right (newline) but keep the left
                clean_line = line.rstrip()

                # Instead of full split(), just count tabs/spaces to guess structure.
                # If the file is strictly Tab Separated, using .count('\t') is faster.
                # Here we use split() to be safe, but minimized.
                parts = clean_line.split()

                if not parts:
                    continue

                # Case A: Data is missing intervals (Standard Graph)
                # We just write the clean nodes + a random pre-baked suffix
                if len(parts) < 3:
                    # Logic: "Node1 Node2" + "\t2\t4\n"
                    # We reconstruct the line using the parts to ensure single tab separation
                    write_line(f"{parts[0]}\t{parts[1]}{get_random_suffix(suffixes)}")

                # Case B: Data has intervals, we need to clamp them
                else:
                    try:
                        # We must do the math here, no way around it
                        start = int(parts[-2])
                        end = int(parts[-1])

                        # Fast clamp logic
                        new_start = 2 if start < 2 else (4 if start > 4 else start)
                        new_end = 2 if end < 2 else (4 if end > 4 else end)

                        # Reconstruct
                        # Join all parts except the last two, then add new times
                        base_content = "\t".join(parts[:-2])
                        write_line(f"{base_content}\t{new_start}\t{new_end}\n")

                    except ValueError:
                        # Fallback if parsing fails: treat as missing
                        write_line(f"{parts[0]}\t{parts[1]}{get_random_suffix(suffixes)}")

                # Progress report (only every 1 million lines to reduce print overhead)
                count += 1
                if count % 1_000_000 == 0:
                    print(f"Processed {count:,} lines...", end='\r')

        print(f"\nSuccess! Processed {count:,} lines.")

    except Exception as e:
        print(f"\nCritical Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()