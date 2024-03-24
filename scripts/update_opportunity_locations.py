import argparse
import sys
from typing import cast

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


parser = argparse.ArgumentParser(
    description="Replace Lost opportunity with Lead Qualification form"
)
parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
parser.add_argument("--end-date", "-d", required=True, help="End date")
parser.add_argument("--location", "-l", required=True, help="Location")
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(api_key)

location_field_id = close.get_prefixed_custom_field_id("shared", "Location")
if not location_field_id:
    print("Location custom field not found")
    sys.exit(1)

won_opportunities = close.get_all(
    "opportunity", params={"status_type": "won", "date_won__lte": args.end_date}
)
updated_opportunities = []
for opportunity in won_opportunities:
    location = opportunity.get(location_field_id)
    if location:
        if location != args.location:
            print(f"Location mismatch: {location} vs. {args.location}")
        continue

    updated_opportunity = close.put(
        f"opportunity/{opportunity['id']}", data={location_field_id: args.location}
    )
    updated_opportunities.append(updated_opportunity)
    if args.verbose:
        print(
            f"Updated {updated_opportunity['lead_id']} {updated_opportunity['lead_name']}"
        )


print(f"Updated {len(updated_opportunities)} opportunities")
