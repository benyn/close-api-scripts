import argparse
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.csv import write_csv
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Delete all the opportunities in a given status."
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--environment",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument("--status", "-s", type=str, required=True, help="Status label")
parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = parser.parse_args()

if args.environment:
    api_key = get_api_key("api.close.com", f"admin_{args.environment}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseApiWrapper(api_key)

# Get the status_id
statuses = api.get("status/opportunity")["data"]
status_id = [st["id"] for st in statuses if st["label"].lower() == args.status.lower()]
if not status_id:
    print(f"Status not found: {args.status}")
    sys.exit(1)

status_id = status_id[0]

if args.verbose:
    print(f"Fetching opportunities in {args.status}...")

opportunity_fields = [
    "id",
    "lead_id",
    "lead_name",
    "status_id",
    "status_label",
    "value",
    "value_period",
    "value_currency",
    "user_id",
    "user_name",
    "contact_id",
    "date_created",
]
fields_param = ",".join(opportunity_fields)

opportunities = api.get_all(
    "opportunity",
    params={
        "status_id": status_id,
        "_order_by": "date_created",
        "_fields": fields_param,
    },
)

write_csv(
    f"output/deleted_opportunities-{args.environment}-{status_id}.csv",
    opportunity_fields,
    opportunities,
)

ans = input(f"Delete {len(opportunities)} opportunities? (y/n): ")
if ans.lower() != "y":
    sys.exit(0)

# Delete fetched opportunities
total_cnt = len(opportunities)
for idx, opp in enumerate(opportunities):
    api.delete("opportunity/" + opp["id"])
    if args.verbose:
        print(f"Deleting {(idx + 1)}/{total_cnt}")

print(f"Deleted {total_cnt} opportunities.")
