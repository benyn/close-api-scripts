import argparse
import sys
from typing import cast

from CloseApiWrapper import CloseApiWrapper
from utils.get_api_key import get_api_key
from utils.get_lead_id import get_lead_and_activity_id, get_lead_id
from utils.prompt_user_for_choice import prompt_user_for_choice


parser = argparse.ArgumentParser(
    description="Replace Lost opportunity with Lead Qualification form"
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
    "--activity-url",
    "-u",
    required=True,
    help="URL of Activity whose timestamp will be used",
)
parser.add_argument(
    "--activity-type",
    "-t",
    choices=[
        "call",
        "created",
        "email",
        "meeting",
        "note",
        "sms",
        "status_change/lead",
        "task_completed",
    ],
    help="Activity type",
)
parser.add_argument("--verbose", "-v", action="store_true", help="verbose logging")
args = parser.parse_args()

if args.env:
    api_key = get_api_key("api.close.com", f"admin_{args.env}")
elif args.api_key:
    api_key = args.api_key
else:
    print("Either environment or API key must be provided.")
    sys.exit(1)

api = CloseApiWrapper(api_key)


def get_lead_qualification_create_payload(activity):
    # Custom Activity Type ID
    cat_id = api.get_custom_activity_type_id("Lead Qualification")
    payload = {"custom_activity_type_id": cat_id}

    # Copy activity field values to payload
    copied_default_field_ids = {
        "created_by",
        "date_created",
        "date_updated",
        "lead_id",
        "updated_by",
        "user_id",
    }

    for key, value in activity.items():
        if key in copied_default_field_ids:
            payload[key] = value

    # Set custom field values
    lq_custom_field_ids = api.get_custom_field_name_prefixed_id_mapping(
        f"activity/{cat_id}"
    )
    next_step_custom_field_id = lq_custom_field_ids["Next Step"]
    notes_custom_field_id = lq_custom_field_ids["Notes"]
    payload[next_step_custom_field_id] = "Proceed to Clinical Consultation"
    payload[notes_custom_field_id] = activity.get("note")

    return payload


def get_opportunity_payload(activity, source_custom_activity_instance_id):
    copied_default_field_ids = {
        "date_created",
        "date_updated",
        "lead_id",
        "note",
        "updated_by",
        "user_id",
    }
    payload = {
        key: value for key, value in activity.items() if key in copied_default_field_ids
    }

    opp_custom_field_ids = api.get_custom_field_name_prefixed_id_mapping("opportunity")
    source_custom_activity_instance_id_field_id = opp_custom_field_ids[
        "source_custom_activity_instance_id"
    ]
    payload[source_custom_activity_instance_id_field_id] = (
        source_custom_activity_instance_id
    )
    return payload


def create_lead_qualification(activity):
    lead_qualification_data = get_lead_qualification_create_payload(activity)
    post_custom_activity_instance = api.post("activity/custom", lead_qualification_data)
    print(f"Created {post_custom_activity_instance['id']}")
    return post_custom_activity_instance


def create_opportunity(activity, source_custom_activity_instance_id):
    opportunity_data = get_opportunity_payload(
        activity, source_custom_activity_instance_id
    )
    post_opportunity = api.post("opportunity", opportunity_data)
    return post_opportunity


lead_id, activity_id = get_lead_and_activity_id(args.activity_url)
activity_type = args.activity_type if args.activity_type else "note"
activity = api.get(f"activity/{activity_type}/{activity_id}")
created_lead_qualification = create_lead_qualification(activity)
created_opportunity = create_opportunity(activity, created_lead_qualification["id"])
print(f"Created {created_opportunity['id']}")
