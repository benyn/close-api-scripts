import csv
from typing import Any


def read_csv(file_path, exclude_header=False):
    """Reads a CSV file and returns a list of rows."""
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        if exclude_header:
            next(reader)  # Skip the header row
        return [tuple(row) for row in reader]


def write_csv(file_name: str, keys: list[str], rows: dict[str, Any]):
    with open(file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(rows)
