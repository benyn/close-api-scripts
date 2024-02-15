import argparse
import json
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Updates Lead Status based on Loss Reason."
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument("--loss-reason", "-r", required=True, help="Loss Reason")
parser.add_argument("--lead-status", "-s", required=True, help="New Lead Status")
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


# Loss Reason custom field ID
loss_reason_field_id = api.get_custom_field_id("opportunity", "Loss Reason")
loss_reason_field_id_with_prefix = f"custom.{loss_reason_field_id}"


# Get the status_id
lead_statuses = api.get("status/lead")["data"]
lead_status_id = next(
    (
        st["id"]
        for st in lead_statuses
        if st["label"].lower() == args.lead_status.lower()
    ),
    None,
)
if lead_status_id is None:
    print(f"Lead status not found: {args.lead_status}")
    sys.exit(1)


if args.verbose:
    print("Fetching opportunities...")

opportunities = api.get_all_items(
    "opportunity",
    params={
        "query": f"{loss_reason_field_id_with_prefix}:{args.loss_reason}",
        "_order_by": "date_created",
        "_fields": "id,lead_id",
    },
)

ans = input(f"Update {len(opportunities)} opportunities and leads? (y/n): ")
if ans.lower() != "y":
    sys.exit(0)

# Update fetched opportunities and leads
updated_leads = []
updated_opps = []
total_cnt = len(opportunities)
for idx, opp in enumerate(opportunities):
    updated_lead = api.put(
        "lead/" + opp["lead_id"],
        data={
            "status_id": lead_status_id,
        },
    )
    updated_leads.append(updated_lead)
    updated_opp = api.put(
        "opportunity/" + opp["id"],
        data={
            loss_reason_field_id_with_prefix: None,
        },
    )
    updated_opps.append(updated_opp)
    if args.verbose:
        print(f"Updating {(idx + 1)}/{total_cnt}")

if updated_leads:
    print(f"Updated {len(updated_leads)} out of {len(opportunities)} leads.")
    with open(
        f"output/leads_status_updated_from_loss_reason-{args.env}.json", "w"
    ) as f:
        json.dump(updated_leads, f)
else:
    print("No leads were updated.")

if updated_opps:
    print(f"Updated {len(updated_opps)} out of {len(opportunities)} opportunities.")
    with open(f"output/opportunities_loss_reason_removed-{args.env}.json", "w") as f:
        json.dump(updated_opps, f)
else:
    print("No opportunities were updated.")
