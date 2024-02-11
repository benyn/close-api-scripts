import argparse
import json

# import math
from operator import itemgetter
import sys

import gevent.monkey
from gevent.pool import Pool

from utils.csv import write_csv

gevent.monkey.patch_all()

from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key


parser = argparse.ArgumentParser(
    description="Find duplicate contact details on a contact in your Close org"
)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    "--env",
    "-e",
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
group.add_argument("--api-key", "-k", help="API Key")
parser.add_argument(
    "--detail-type",
    "-t",
    default="all",
    choices=["email", "phone", "url", "all"],
    required=False,
    help="Specity a type of contact details to delete",
)
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

# Initialize Close API Wrapper
api = CloseApiWrapper(api_key)

# # Calculate number of slices necessary to get all leads
# total_contacts = api.count(
#     {
#         "type": "or",
#         "queries": [
#             {
#                 "type": "field_condition",
#                 "field": {
#                     "type": "regular_field",
#                     "object_type": "contact",
#                     "field_name": "emails_count",
#                 },
#                 "condition": {"type": "number_range", "gt": 1},
#             },
#             {
#                 "type": "field_condition",
#                 "field": {
#                     "type": "regular_field",
#                     "object_type": "contact",
#                     "field_name": "phones_count",
#                 },
#                 "condition": {"type": "number_range", "gt": 1},
#             },
#         ],
#     },
#     "contact",
# )
# total_slices = int(math.ceil(float(total_contacts) / 1000))
# slices = range(1, total_slices + 1)
# contacts = []
contacts = api.get_all_items(
    "contact", params={"_fields": "lead_id,id,name,emails,phones,urls,date_created"}
)
contacts.sort(key=itemgetter("date_created"))


def dedupliate_contact_details(
    details, detail_type: str, priority_type1: str, priority_type2: str
):
    seen = {}
    for detail in details:
        detail_key = detail[detail_type]
        type_label = detail["type"]

        if detail_key not in seen:
            # If the email/phone/url hasn't been seen before, add it directly.
            seen[detail_key] = detail
        elif seen[detail_key]["type"] != priority_type1 and (
            type_label == priority_type1
            or (
                seen[detail_key]["type"] != priority_type2
                and type_label == priority_type2
            )
        ):
            # If the current entry has a more preferred type, replace the existing one.
            seen[detail_key] = detail

    return list(seen.values())


def update_contact(contact_id_and_payload):
    contact_id, payload = contact_id_and_payload
    return api.put(f"contact/{contact_id}", data=payload)


# Process duplicates
contact_updates = []
diffs = []
total_deduped_email_count = 0
total_deduped_phone_count = 0
total_deduped_url_count = 0
for contact in contacts:
    update = {}
    diff = {
        "lead_id": contact["lead_id"],
        "contact_id": contact["id"],
        "contact_name": contact["name"],
    }

    if args.detail_type in ["all", "email"]:
        deduped_emails = dedupliate_contact_details(
            contact["emails"], "email", "direct", "home"
        )
        deduped_email_count = len(contact["emails"]) - len(deduped_emails)
        if deduped_email_count > 0:
            update["emails"] = deduped_emails
            diff["emails_before"] = contact["emails"]
            diff["emails_after"] = deduped_emails
            total_deduped_email_count += deduped_email_count

    if args.detail_type in ["all", "phone"]:
        deduped_phones = dedupliate_contact_details(
            contact["phones"], "phone", "mobile", "direct"
        )
        deduped_phone_count = len(contact["phones"]) - len(deduped_phones)
        if deduped_phone_count > 0:
            update["phones"] = deduped_phones
            diff["phones_before"] = contact["phones"]
            diff["phones_after"] = deduped_phones
            total_deduped_phone_count += deduped_phone_count

    if args.detail_type in ["all", "url"]:
        deduped_urls = dedupliate_contact_details(contact["urls"], "url", "url", "url")
        deduped_url_count = len(contact["urls"]) - len(deduped_urls)
        if deduped_url_count > 0:
            update["urls"] = deduped_urls
            diff["urls_before"] = contact["urls"]
            diff["urls_after"] = deduped_urls
            total_deduped_url_count += deduped_url_count

    if update:
        contact_updates.append((contact["id"], update))
        diffs.append(diff)

if diffs:
    with open(f"updated_contacts-{args.env}.json", "w") as f:
        json.dump(diffs, f)


if contact_updates:
    ans = input(
        f"Delete {total_deduped_email_count} emails, {total_deduped_phone_count} phones, and {total_deduped_url_count} URLs across {len(contact_updates)} contacts? (y/n): "
    )
    if ans.lower() != "y":
        sys.exit(0)

    pool = Pool(7)
    pool.map(update_contact, contact_updates)
    print(f"Updated {len(contact_updates)} contacts.")
else:
    print("No duplicate contact details found.")
