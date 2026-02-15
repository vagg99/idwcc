"""
Το συγκεκριμένο αρχείο λειτουργεί ως παραστατική βοήθεια για
την τελική γραφηματική αναπαράσταση και ποσοτικοποίηση των τριών
αλγορίθμων, που εντοπίζουν τρίγωνα πάνω στους ιστορικούς γράφους.
"""

import os
import re
import matplotlib.pyplot as plt

# --- Σταθερές / ονόματα αλγορίθμων (όπως στα σχήματα) ---
ALGO_ORDER = ["Tr-Intervals", "Tr-Filtering", "Tr-Thresholding"]

# --- Regex που “πιάνει” τις γραμμές από τα logs σου ---
RE_TOTAL_TRI = re.compile(r"Total number of triangles:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
RE_FORMED_TRI = re.compile(r"Formed triangles with edge intervals:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
RE_TIME_MS = re.compile(r"Time taken to calculate triangle scores:\s*([0-9]+)\s*milliseconds", re.IGNORECASE)


def read_multiline_until_end(prompt: str) -> str:
    """
    Διαβάζει multi-line κείμενο από stdin μέχρι ο χρήστης να γράψει τελευταία γραμμή "END".
    """
    print(prompt)
    print("Κάνε paste το κείμενο και τελείωσε με μία γραμμή που περιέχει μόνο: END")
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
    Επιστρέφει:
      time_s: float
      triangles: float  (Formed αν υπάρχει, αλλιώς Total)
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
        raise ValueError("Δεν βρέθηκε γραμμή χρόνου: 'Time taken to calculate triangle scores: ... milliseconds'")
    if triangles is None:
        raise ValueError("Δεν βρέθηκε γραμμή τριγώνων: 'Formed ...' ή 'Total number of triangles:'")

    return time_ms / 1000.0, triangles


def grouped_bar_chart(datasets, values_by_algo, ylabel, title, out_pdf):
    """
    datasets: list[str]
    values_by_algo: dict[algo -> list[float]] με ίδιο μήκος όσο datasets
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
    print("=== Δημιουργία γραφημάτων βάσει πειραματικών αποτελεσμάτων (runtime & triangle counts) ===")
    output_directory = input("Output folder (πάτα Enter για τρέχον φάκελο): ").strip()
    if output_directory == "":
        output_directory = "."
    os.makedirs(output_directory, exist_ok=True)

    # Πόσα datasets;
    while True:
        try:
            n = int(input("Πόσα datasets θα εισαγάγεις; (π.χ. 2): ").strip())
            if n <= 0:
                raise ValueError()
            break
        except ValueError:
            print("Δώσε έναν θετικό ακέραιο αριθμό (π.χ. 2).")

    datasets = []
    runtime = {algo: [] for algo in ALGO_ORDER}
    counts = {algo: [] for algo in ALGO_ORDER}

    for i in range(n):
        ds = input(f"Όνομα dataset #{i + 1} (π.χ. Amazon ή DBLP): ").strip()
        if not ds:
            ds = f"dataset_{i + 1}"
        datasets.append(ds)

        for algo in ALGO_ORDER:
            text = read_multiline_until_end(
                f"\n--- Dataset: {ds} | Αλγόριθμος: {algo} ---"
            )
            try:
                t_s, tri = parse_log(text)
            except Exception as e:
                print(f"\n[Σφάλμα parsing για {ds} / {algo}] {e}")
                print("Ξανακάνε paste το σωστό κομμάτι output.")
                text = read_multiline_until_end(f"Ξαναδώσε output για {ds} / {algo}:")
                t_s, tri = parse_log(text)

            runtime[algo].append(t_s)
            counts[algo].append(tri)

    # Εκτύπωση συνοπτικού πίνακα στο terminal
    print("\n=== Σύνοψη ===")
    for idx, ds in enumerate(datasets):
        print(f"\nDataset: {ds}")
        for algo in ALGO_ORDER:
            print(f"  {algo:14s}  time={runtime[algo][idx]:.3f}s  triangles={counts[algo][idx]}")

    # Παραγωγή PDFs (όπως τα σχήματα 6.2 και 6.3)
    runtime_pdf = os.path.join(output_directory, "runtime_comparison.pdf")
    counts_pdf = os.path.join(output_directory, "triangle_counts.pdf")

    grouped_bar_chart(
        datasets=datasets,
        values_by_algo=runtime,
        ylabel="Χρόνος εκτέλεσης (s)",
        title="Σύγκριση χρόνου εκτέλεσης ανά παραλλαγή",
        out_pdf=runtime_pdf
    )

    grouped_bar_chart(
        datasets=datasets,
        values_by_algo=counts,
        ylabel="Πλήθος τριγώνων",
        title="Πλήθος τριγώνων ανά παραλλαγή",
        out_pdf=counts_pdf
    )

    print("\nΈτοιμα τα αρχεία:")
    print(f" - {runtime_pdf}")
    print(f" - {counts_pdf}")


if __name__ == "__main__":
    main()
