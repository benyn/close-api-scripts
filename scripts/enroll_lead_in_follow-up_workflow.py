import argparse
import sys
from CloseApiWrapper import CloseApiWrapper

from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


arg_parser = argparse.ArgumentParser(
    description="Enroll Lead in one of the Follow Up Workflows"
)
arg_parser.add_argument(
    "--env",
    "-e",
    required=True,
    choices=["dev", "prod"],
    help="Target environment (dev/prod)",
)
arg_parser.add_argument("--lead-id", "-l", required=True, help="Lead ID")
arg_parser.add_argument(
    "--verbose", "-v", action="store_true", help="Increase logging verbosity."
)
args = arg_parser.parse_args()


close_api_key = get_api_key("api.close.com", f"admin_{args.env}")
close = CloseApiWrapper(close_api_key)


def get_workflow_id(contact):
    pref_language_custom_field_id = close.get_prefixed_custom_field_id(
        "contact", "Preferred Language"
    )
    pref_language = contact[pref_language_custom_field_id]
    language_suffix = "🇪🇸" if pref_language == "Spanish" else "🇬🇧"
    workflow_id = close.get_workflow_id("D:", language_suffix)
    return workflow_id


lead_id = get_lead_id(args.lead_id)

# Get Follow-Up Date & Time from the last Lead Qualification instance
lead_qualification = close.get_last_lead_qualification(lead_id, args.verbose)
if not lead_qualification:
    print(f"No Lead Qualification custom activity instances found for {lead_id}")
    sys.exit(1)

lq_custom_field_ids = close.get_custom_field_name_prefixed_id_mapping(
    f"activity/{lead_qualification['custom_activity_type_id']}"
)
followup_datetime_custom_field_id = lq_custom_field_ids["Follow-Up Date & Time"]
followup_datetime = lead_qualification[followup_datetime_custom_field_id]
if not followup_datetime:
    print(f"Follow-Up Date & Time not found for {lead_id}")
    sys.exit(1)

# Choose contacts
lead = close.get(f"lead/{lead_id}", params={"_fields": "contacts"})
contacts = {contact["name"]: contact for contact in lead["contacts"]}
selected_contact_name = prompt_user_for_choice("Contact", list(contacts.keys()))
selected_contact = contacts[selected_contact_name]


# Enroll selected Contact in one of the Workflows
workflow_id = get_workflow_id(selected_contact)
if not workflow_id:
    print("Workflow ID not found")
    sys.exit(0)

post_subscription = close.post(
    "sequence_subscription",
    data={
        "sequence_id": workflow_id,
        "lead_id": selected_contact["lead_id"],
        "contact_id": selected_contact["id"],
        "start_date": followup_datetime,
    },
)
print(
    f"Created workflow subscrption {post_subscription['id']} for {post_subscription['lead_id']}"
)
