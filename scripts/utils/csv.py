import csv
from typing import Any


def write_csv(file_name: str, keys: list[str], rows: dict[str, Any]):
    with open(file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(rows)
