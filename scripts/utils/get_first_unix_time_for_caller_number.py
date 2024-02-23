#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Script for Easy CSV Editor
Finds and returns the `unix_time` value of the first CallTrackingMetrics activity
whose `caller_number` matches the input.
"""

import json
import os
import sys


def get_json_file_path():
    csv_file_path = sys.argv[1]
    csv_file_dir = os.path.dirname(csv_file_path)
    json_file_path = os.path.join(
        csv_file_dir, "..", "output", "archive", "calltrackingmetrics-calls-prod.json"
    )
    return json_file_path


def find_first_record_with_caller_number(json_file_path, caller_number):
    try:
        with open(json_file_path, "r") as json_file:
            data = json.load(json_file)
            for record in data:
                if record.get("caller_number") == caller_number:
                    return record.get("unix_time")
    except FileNotFoundError:
        return f"File not found: {json_file_path}"
    except json.JSONDecodeError:
        return f"Error decoding JSON from file: {json_file_path}"
    return None


def main():
    caller_number = sys.stdin.read()
    if not caller_number:
        return None

    json_file_path = get_json_file_path()
    first_unix_time = find_first_record_with_caller_number(
        json_file_path, caller_number
    )
    if first_unix_time:
        sys.stdout.write(str(first_unix_time))


if __name__ == "__main__":
    main()
