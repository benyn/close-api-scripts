import argparse
import json
import sys

import gevent.monkey

gevent.monkey.patch_all()

from closeio_api import APIError, Client as CloseIO_API
from gevent.pool import Pool
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(description="Restore an array of deleted tasks by ID.")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--tasks", help="List of task IDs in a form of a comma separated list"
)
group.add_argument(
    "--tasks-file",
    help="List of task IDs in a form of a textual file with single column of task IDs",
)
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseIO_API(api_key)

# Array of Task IDs. Add the IDs you want to restore here.
if args.tasks:
    task_ids = args.tasks.split(",")
elif args.tasks_file:
    with open(args.tasks_file) as f:
        lines = f.readlines()
    task_ids = [el.strip() for el in lines]  # Strip new lines
    task_ids = list(filter(None, task_ids))  # Strip empty lines
else:
    print("Either tasks or tasks_file must be provided.")
    sys.exit(1)

# Create a list of active users for the sake of posting opps.
org_id = api.get("me")["organizations"][0]["id"]
memberships = api.get("organization/" + org_id, params={"_fields": "memberships"})[
    "memberships"
]
active_users = [i["user_id"] for i in memberships]

# Array to keep track of number of tasks restored. Because we use gevent, we can't have a standard counter variable.
total_tasks_restored = {}


def restore_task(old_task_id):
    resp = api.get(
        "event",
        params={
            "action": "deleted",
            "object_id": old_task_id,
        },
    )
    if len(resp["data"]) > 0 and resp["data"][0].get("previous_data"):
        prev = resp["data"][0]["previous_data"]
        if "id" in prev:
            del prev["id"]
        # Post New Task.
        try:
            post_task = api.post("task", data=prev)
            if "id" in post_task:
                total_tasks_restored[old_task_id] = post_task
                print(f"{len(total_tasks_restored)}: Restored {old_task_id}")
        except APIError as e:
            print(f"{old_task_id}: Task could not be posted because {str(e)}")
    else:
        print(
            f"{old_task_id} could not be restored because there is no data to restore"
        )


print(f"Total tasks being restored: {len(task_ids)}")
pool = Pool(5)
pool.map(restore_task, task_ids)
print(f"Total tasks restored {len(total_tasks_restored)}")
print(f"Total tasks not restored {(len(task_ids) - len(total_tasks_restored))}")

# Save restored tasks to JSON file
with open("output/restored_tasks.json", "w") as file:
    json.dump(total_tasks_restored, file)
