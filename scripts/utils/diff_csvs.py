import argparse
import csv

from utils.csv import read_csv


def diff_csv(source_file, comparison_file, output_file):
    """Finds rows in source_file not in comparison_file and writes them to output_file."""
    rows_source = set(read_csv(source_file))
    rows_comparison = set(read_csv(comparison_file))

    unique_rows = rows_source - rows_comparison

    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for row in unique_rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Compare two CSV files and output rows unique to the first file."
    )
    parser.add_argument("--source-file", "-s", help="Path to the source CSV file")
    parser.add_argument(
        "--comparison-file", "-c", help="Path to the comparison CSV file"
    )
    parser.add_argument("--output", "-o", help="Path to the output CSV file")
    args = parser.parse_args()

    diff_csv(args.source_file, args.comparison_file, args.output)


if __name__ == "__main__":
    main()
