import argparse
import json
import sys

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key

parser = argparse.ArgumentParser(
    description="Update custom field value for opportunities"
)
parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument("--custom-field", "-f", required=True, help="Custom field name")
parser.add_argument("--old-value", "-o", required=True, help="Current value to match")
parser.add_argument("--new-value", "-n", required=True, help="New value to set")
parser.add_argument("--status-label", "-s", help="Opportunity status label")
parser.add_argument("--end-date", "-d", help="Opportunity close date end date")
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

if args.old_value == args.new_value:
    print("Error: --old-value and --new-value must be different.")
    sys.exit(1)

api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(api_key)

custom_field_id = close.get_prefixed_custom_field_id(
    "shared", args.custom_field
) or close.get_prefixed_custom_field_id("opportunity", args.custom_field)
if not custom_field_id:
    print(f"{args.custom_field} custom field not found")
    sys.exit(1)

params = {
    "query": f"{custom_field_id}:{args.old_value}",
    "_fields": f"id,{custom_field_id},lead_name",
}
if args.status_label:
    params["status_label"] = args.status_label
if args.end_date:
    params["date_won__lte"] = args.end_date

opportunities = close.get_all("opportunity", params=params)
user_input = input(
    f"Fetched {len(opportunities)} opportunities. Do you want to proceed? (y/n): "
)
if user_input.lower() != "y":
    print("Operation cancelled by user.")
    sys.exit(0)

updated_opportunities = []
for opportunity in opportunities:
    current_value = opportunity.get(custom_field_id)
    # Update only the exact matches, handling both string and list cases
    if isinstance(current_value, list):
        if args.old_value in current_value:
            if args.new_value != "null":
                new_value = [
                    args.new_value if v == args.old_value else v for v in current_value
                ]
            else:
                new_value = [v for v in current_value if v != args.old_value]
        else:
            if args.verbose:
                print(
                    f"Exact match for '{args.old_value}' not found in {current_value}"
                )
            continue
    elif current_value == args.old_value:
        new_value = args.new_value if args.new_value != "null" else None
    else:
        if args.verbose:
            print(f"Value mismatch: {current_value} vs. {args.old_value}")
        continue

    updated_opportunity = close.put(
        f"opportunity/{opportunity['id']}", data={custom_field_id: new_value}
    )
    updated_opportunities.append(updated_opportunity)
    if args.verbose:
        print(
            f"Updated {updated_opportunity['lead_id']} {updated_opportunity['lead_name']}"
        )

print(f"Updated {len(updated_opportunities)} opportunities")

if updated_opportunities:
    with open(
        f"output/updated_opportunities_{args.custom_field}-{args.env}.json", "w"
    ) as f:
        json.dump(updated_opportunities, f)
    print("Updated opportunities saved to disk")
