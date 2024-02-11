import argparse
import json
import sys
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


parser = argparse.ArgumentParser(description="Delete leads")
parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--last-minutes", "-m", type=int, help="Last minutes")
group.add_argument(
    "--leads", help="List of lead IDs in a form of a comma separated list"
)
group.add_argument(
    "--leads-file",
    help="List of lead IDs in a form of a textual file with single column of lead IDs",
)
parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
api = CloseApiWrapper(close_api_key)

# Array of Lead IDs. Add the IDs you want to restore here.
if args.leads:
    lead_ids = args.leads.split(",")
elif args.leads_file:
    with open(args.leads_file) as f:
        lines = f.readlines()
    lead_ids = [el.strip() for el in lines]  # Strip new lines
    lead_ids = list(filter(None, lead_ids))  # Strip empty lines
elif args.last_minutes:
    queried_leads = api.search(
        {
            "type": "and",
            "queries": [
                {"object_type": "lead", "type": "object_type"},
                {
                    "type": "field_condition",
                    "field": {
                        "field_name": "date_created",
                        "object_type": "lead",
                        "type": "regular_field",
                    },
                    "condition": {
                        "before": {"type": "now"},
                        "on_or_after": {
                            "direction": "past",
                            "moment": {"type": "now"},
                            "offset": {
                                "days": 0,
                                "hours": 0,
                                "minutes": args.last_minutes,
                                "months": 0,
                                "seconds": 0,
                                "weeks": 0,
                                "years": 0,
                            },
                            "type": "offset",
                            "which_day_end": "start",
                        },
                        "type": "moment_range",
                    },
                },
            ],
        },
        fields=["id", "name"],
    )
    with open(f"output/deleted_leads-{args.env}.json", "w") as f:
        json.dump(queried_leads, f)
    lead_ids = [lead["id"] for lead in queried_leads]
else:
    print("Invalid coditions")
    sys.exit(0)

# Delete leads
total_cnt = len(lead_ids)
for idx, id in enumerate(lead_ids):
    api.delete(f"lead/{id}")
    if args.verbose:
        print(f"Deleted {(idx+1)}/{total_cnt}\t{id}")

print(f"Deleted {total_cnt} leads.")
