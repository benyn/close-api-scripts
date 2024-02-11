import argparse
import json
import sys

from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Remove Opportunity notes matching Lead names."
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = arg_parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


# Fetch Lead Source values
if args.env == "dev":
    lead_source_field_id = "cf_hHFZuGDsQMlYUVsON5rms6JaPudAeogJCYtXNs34AaS"
elif args.env == "prod":
    lead_source_field_id = "cf_I4o5fFnU8bWIwi4q7b0YpS1RIMWICR5qrhJOrc9MuA7"
else:
    print("Unsupported environment")
    sys.exit(1)

lead_source_field_id_with_prefix = f"custom.{lead_source_field_id}"

lead_sources = close.get(f"custom_field/lead/{lead_source_field_id}")["choices"]


# Fetch and iterate through opportunities
opportunities = close.get_all_items(
    "opportunity",
    params={"_fields": "id,note,lead_name,lead_id"},
)
if args.verbose:
    print(f"Scanning {len(opportunities)} opportunities...")

updated_opps = []
updated_leads = []
for opp in opportunities:
    if opp["note"] == opp["lead_name"]:
        updated_opp = close.put(f"opportunity/{opp['id']}", data={"note": None})
        updated_opps.append(updated_opp)
        if args.verbose:
            print(f"✅ Removed note from {updated_opp['lead_name']}")

    else:
        matched_source = next(
            (
                source
                for source in lead_sources
                if opp["note"] == f"{opp['lead_name']}\n{source}"
            ),
            None,
        )
        if matched_source:
            updated_opp = close.put(f"opportunity/{opp['id']}", data={"note": None})
            updated_opps.append(updated_opp)
            updated_lead = close.put(
                f"lead/{opp['lead_id']}",
                data={lead_source_field_id_with_prefix: matched_source},
            )
            updated_leads.append(updated_lead)
            if args.verbose:
                print(
                    f"✅ Removed note from {updated_opp['lead_name']} and updated lead source with {matched_source}"
                )

        elif opp["note"] and args.verbose:
            print(
                f"ℹ️ https://app.close.com/lead/{opp['lead_id']}/ {opp['lead_name']}\tvs. {opp['note']}"
            )

if updated_opps:
    print(f"Updated {len(updated_opps)} out of {len(opportunities)} opportunities.")
    with open(f"output/opportunities_updated-{args.env}.json", "w") as f:
        json.dump(updated_opps, f)
else:
    print("No opportunities were updated.")

if updated_leads:
    print(f"Updated {len(updated_leads)} leads with Lead Source values.")
    with open(f"output/lead_source_updated-{args.env}.json", "w") as f:
        json.dump(updated_leads, f)
else:
    print("No leads were updated.")
