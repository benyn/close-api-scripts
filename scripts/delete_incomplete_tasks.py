import argparse
import re
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.csv import write_csv
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Remove automatically created incomplete tasks."
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--environment",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = parser.parse_args()

if args.environment:
    api_key = get_api_key("api.close.com", f"admin_{args.environment}")
elif args.api_key:
    api_key = args.api_key

api = CloseApiWrapper(api_key)

if args.verbose:
    print("Fetching tasks...")

task_fields = [
    "id",
    "created_by_name",
    "assigned_to_name",
    "text",
    "date_created",
    "date_updated",
    "is_complete",
    "lead_id",
    "lead_name",
]
fields_param = ",".join(task_fields)

tasks = api.get_all_items(
    "task",
    params={"is_complete": False, "_order_by": "date_created", "_fields": fields_param},
)

# Filter tasks based on creator name and content
pattern = r"^(?:Call (?:#?\d\b|\+ Auto)|F/U via)"
regex = re.compile(pattern)
filtered_tasks = [
    task
    for task in tasks
    if task["created_by_name"] == "Yok Kanya" or regex.match(task["text"])
]

write_csv(f"output/all_tasks.csv-{args.environment}", task_fields, tasks)
write_csv(f"output/deleted_tasks-{args.environment}.csv", task_fields, filtered_tasks)

ans = input(f"Delete {len(filtered_tasks)} of {len(tasks)} incomplete tasks? (y/n): ")
if ans.lower() != "y":
    sys.exit(0)

# Delete filtered tasks
total_cnt = len(filtered_tasks)
for idx, task in enumerate(filtered_tasks):
    api.delete("task/" + task["id"])
    if args.verbose:
        print(f"Deleting {(idx + 1)}/{total_cnt}")

print(f"Deleted {total_cnt} tasks.")