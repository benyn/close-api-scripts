import argparse
import json
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(description="Merge 'Lost' opportunity statuses.")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument(
    "--source-status", "-s", type=str, required=True, help="Source status label"
)
parser.add_argument(
    "--target-status", "-t", type=str, required=True, help="Target status label"
)
parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseApiWrapper(api_key)


# Get the status_id
statuses = api.get("status/opportunity")["data"]
source_status_id = next(
    (st["id"] for st in statuses if st["label"].lower() == args.source_status.lower()),
    None,
)
target_status_id = next(
    (st["id"] for st in statuses if st["label"].lower() == args.target_status.lower()),
    None,
)
if source_status_id is None:
    print(f"Source status not found: {args.source_status}")
    sys.exit(1)
if target_status_id is None:
    print(f"Target status not found: {args.target_status}")
    sys.exit(1)

if args.verbose:
    print(f"Fetching opportunities in {args.source_status}...")


# Loss Reason custom field ID
loss_reason_field_id = api.get_custom_field_id("opportunity", "Loss Reason")
loss_reason_field_id_with_prefix = f"custom.{loss_reason_field_id}"


opportunities = api.get_all_items(
    "opportunity",
    params={
        "status_id": source_status_id,
        "_order_by": "date_created",
        "_fields": "id",
    },
)

ans = input(f"Update {len(opportunities)} opportunities? (y/n): ")
if ans.lower() != "y":
    sys.exit(0)

# Update fetched opportunities
updated_opps = []
total_cnt = len(opportunities)
for idx, opp in enumerate(opportunities):
    updated_opp = api.put(
        "opportunity/" + opp["id"],
        data={
            "status_id": target_status_id,
            loss_reason_field_id_with_prefix: "No Response",
        },
    )
    updated_opps.append(updated_opp)
    if args.verbose:
        print(f"Updating {(idx + 1)}/{total_cnt}")

if updated_opps:
    print(f"Updated {len(updated_opps)} out of {len(opportunities)} opportunities.")
    with open(f"output/opportunities_status_merged-{args.env}.json", "w") as f:
        json.dump(updated_opps, f)
else:
    print("No opportunities were updated.")
