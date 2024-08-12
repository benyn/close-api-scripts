import csv
from typing import Any, Iterable, Mapping


def read_csv(file_path, exclude_header=False):
    """Reads a CSV file and returns a list of rows."""
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        if exclude_header:
            next(reader)  # Skip the header row
        return [tuple(row) for row in reader]


def read_csv_to_dict(file_path: str) -> list[dict[str, Any]]:
    """Reads a CSV file with a header row and returns a list of dicts using the header row as key names."""
    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


def write_csv(file_name: str, keys: list[str], rows: Iterable[Mapping[str, Any]]):
    with open(file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(rows)
