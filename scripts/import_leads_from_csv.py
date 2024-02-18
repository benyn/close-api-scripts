import argparse
from datetime import datetime
import json
import logging
import os
import sys
from CloseApiWrapper import CloseApiWrapper
from utils.csv import read_csv

from utils.get_api_key import get_api_key


arg_parser = argparse.ArgumentParser(
    description="Sync CallTrackingMetrics records to Close"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--data-path", "-f", required=True, help="Path to the CSV file")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


if not os.path.exists(args.data_path):
    logging.error(f"The data file {args.data_path} does not exist.")
    sys.exit(1)


# Close API client
close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


user_email_to_id_map = close.get_user_ids_by_email()

patient_navigator_field_id = close.get_custom_field_id("lead", "Patient Navigator")
lead_source_field_id = close.get_custom_field_id("lead", "Lead Source")
preferred_language_field_id = close.get_custom_field_id("contact", "Preferred Language")

patient_navigator_field_id_with_prefix = f"custom.{patient_navigator_field_id}"
lead_source_field_id_with_prefix = f"custom.{lead_source_field_id}"
preferred_language_field_id_id_with_prefix = f"custom.{preferred_language_field_id}"


leads = read_csv(args.data_path, exclude_header=True)

created_leads = []
for lead in leads:
    (
        name,
        email,
        phone,
        status_label,
        patient_navigator_email,
        lead_source,
        preferred_language,
        date_created,
        unix_time,
        note,
        note_user_email,
    ) = lead

    date_created = date_created or datetime.utcfromtimestamp(int(unix_time)).isoformat()

    created_lead = close.post(
        "lead",
        data={
            "name": name,
            "status": status_label,
            "contacts": [
                {
                    "name": name,
                    "emails": [{"email": email, "type": "direct"}] if email else None,
                    "phones": [{"phone": phone, "type": "mobile"}] if phone else None,
                    preferred_language_field_id_id_with_prefix: preferred_language,
                    "date_created": date_created,  # Doesn't seem to work
                }
            ],
            "url": None,
            patient_navigator_field_id_with_prefix: user_email_to_id_map.get(
                patient_navigator_email
            ),
            lead_source_field_id_with_prefix: lead_source,
            "date_created": date_created,
        },
    )
    if args.verbose:
        print(f"Added: {created_lead['id']} {created_lead['name']}")
    created_leads.append(created_lead)

    # Add note
    if note and note_user_email:
        post_note = close.post(
            "activity/note",
            data={
                "user_id": user_email_to_id_map.get(note_user_email),
                "note": note,
                "lead_id": created_lead["id"],
                "activity_at": date_created,
                "date_created": date_created,
            },
        )


if created_leads:
    print(f"Created {len(created_leads)} Leads")
    with open(
        f"output/leads_imported_from_csv-{args.env}.json",
        "w",
    ) as f:
        json.dump(created_leads, f)
else:
    print("No Leads were created.")
