"""
This script is intended as an auxiliary tool for the final visualization and
quantitative comparison of the three algorithms that detect (count) triangles
on historical graphs.

It reads experimental output (logs) directly from user input at runtime and
produces two bar charts:
  1) Runtime comparison
  2) Triangle counts
"""

import os
import re
import matplotlib.pyplot as plt

# --- Constants / algorithm names (as used in the figures) ---
ALGO_ORDER = ["Tr-Intervals", "Tr-Filtering", "Tr-Thresholding"]

# Log line regex patterns
RE_TOTAL_TRI = re.compile(r"Total number of triangles:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
RE_FORMED_TRI = re.compile(r"Formed triangles with edge intervals:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
RE_TIME_MS = re.compile(r"Time taken to calculate triangle scores:\s*([0-9]+)\s*milliseconds", re.IGNORECASE)


def read_multiline_until_end(prompt: str) -> str:
    """
    Read multi-line text from stdin until the user enters a final line 'END'.
    """
    print(prompt)
    print("Paste the text and finish with a line containing only: END")
    lines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line.strip() == "END":
            break
        lines.append(line)
    return "\n".join(lines)


def parse_log(text: str):
    """
    Returns:
      time_s: float
      triangles: float
    """
    time_ms = None
    m = RE_TIME_MS.search(text)
    if m:
        time_ms = int(m.group(1))

    total_tri = None
    m = RE_TOTAL_TRI.search(text)
    if m:
        total_tri = float(m.group(1))

    formed_tri = None
    m = RE_FORMED_TRI.search(text)
    if m:
        formed_tri = float(m.group(1))

    triangles = formed_tri if formed_tri is not None else total_tri

    if time_ms is None:
        raise ValueError(
            "Runtime line not found: 'Time taken to calculate triangle scores: ... milliseconds'"
        )
    if triangles is None:
        raise ValueError(
            "Triangle count line not found: either 'Formed triangles with edge intervals: ...' "
            "or 'Total number of triangles: ...'"
        )

    return time_ms / 1000.0, triangles


def grouped_bar_chart(datasets, values_by_algo, ylabel, title, out_pdf):
    """
    datasets: list[str]
    values_by_algo: dict[algo -> list[float]] with the same length as datasets
    """
    n_d = len(datasets)
    n_a = len(ALGO_ORDER)

    x = list(range(n_d))
    width = 0.25 if n_a >= 3 else 0.4
    offsets = [(j - (n_a - 1) / 2.0) * width for j in range(n_a)]

    fig, ax = plt.subplots()

    for j, algo in enumerate(ALGO_ORDER):
        vals = values_by_algo[algo]
        ax.bar([xi + offsets[j] for xi in x], vals, width, label=algo)

    ax.set_xticks(x)
    ax.set_xticklabels(datasets)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_pdf)
    plt.close(fig)


def main():
    print("=== Generate figures from experimental results (runtime & triangle counts) ===")
    output_directory = input("Output folder (press Enter for current directory): ").strip()
    if output_directory == "":
        output_directory = "."
    os.makedirs(output_directory, exist_ok=True)

    # Number of datasets
    while True:
        try:
            n = int(input("How many datasets will you enter? (e.g., 2): ").strip())
            if n <= 0:
                raise ValueError()
            break
        except ValueError:
            print("Please enter a positive integer (e.g., 2).")

    datasets = []
    runtime = {algo: [] for algo in ALGO_ORDER}
    counts = {algo: [] for algo in ALGO_ORDER}

    for i in range(n):
        ds = input(f"Dataset name #{i + 1} (e.g., Amazon or DBLP): ").strip()
        if not ds:
            ds = f"dataset_{i + 1}"
        datasets.append(ds)

        for algo in ALGO_ORDER:
            text = read_multiline_until_end(
                f"\n--- Dataset: {ds} | Algorithm: {algo} ---"
            )
            try:
                t_s, tri = parse_log(text)
            except Exception as e:
                print(f"\n[Parsing error for {ds} / {algo}] {e}")
                print("Please paste the correct output block again.")
                text = read_multiline_until_end(f"Re-enter output for {ds} / {algo}:")
                t_s, tri = parse_log(text)

            runtime[algo].append(t_s)
            counts[algo].append(tri)

    # Print a concise summary table in the terminal
    print("\n=== Summary ===")
    for idx, ds in enumerate(datasets):
        print(f"\nDataset: {ds}")
        for algo in ALGO_ORDER:
            print(f"  {algo:14s}  time={runtime[algo][idx]:.3f}s  triangles={counts[algo][idx]}")

    # Generate PDFs
    runtime_pdf = os.path.join(output_directory, "runtime_comparison.pdf")
    counts_pdf = os.path.join(output_directory, "triangle_counts.pdf")

    # IMPORTANT: Keep ylabel/title in Greek (as requested)
    grouped_bar_chart(
        datasets=datasets,
        values_by_algo=runtime,
        ylabel="Χρόνος εκτέλεσης (s)",
        title="Σύγκριση χρόνου εκτέλεσης ανά dataset",
        out_pdf=runtime_pdf
    )

    grouped_bar_chart(
        datasets=datasets,
        values_by_algo=counts,
        ylabel="Πλήθος τριγώνων",
        title="Πλήθος τριγώνων ανά dataset",
        out_pdf=counts_pdf
    )

    print("\nFiles created:")
    print(f" - {runtime_pdf}")
    print(f" - {counts_pdf}")


if __name__ == "__main__":
    main()
